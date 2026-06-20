# V3.0 调度器设计（Scheduler TCP 控制面 + Worker 控制进程）

> 目标：替换 V2.0 的「DB 租约 + runner DB 注册表 + runner DB 轮询领取」三段式，改为
> **调度器 = TCP server 单一权威派发** + **Worker 控制进程 = TCP client（主进程，非任务进程）**。
> DB 退化为"任务记录账本 + 终态持久化"，不再承担实时派发/心跳。

## 0. 与 V2.0 的关键差异

| 维度 | V2.0 | V3.0 |
|---|---|---|
| 派发 | manager 写 DB lease；runner **轮询** DB 自取（抢占式） | 调度器**顺序 push** `RUN` 命令给 worker（非抢占） |
| Worker 形态 | uvicorn 服务器，任务在其**线程**内跑 | uvicorn 服务器 = **控制主进程**；任务在**独立子进程**里跑 |
| 心跳/状态 | DB `lease_expires_at` + supervisor DB 续租 | 控制进程**TCP** 上报心跳/状态；**TCP 断联 = worker 死** |
| Worker 可见性 | runner DB 注册表（45s 窗口，易漂移） | 调度器内存"已连接 worker 表"（实时，断联即知）|
| 持久化 | 全 DB（rollout 安全） | 任务记录/终态仍写 DB；**实时控制面在内存**，重启靠 DB 重建 + TCP 再对账 |

## 1. 进程拓扑

```
                          ┌──────────────── Manager Pod (role=manager) ────────────────┐
                          │  FastAPI (REST/CRUD/health)                                 │
                          │  SchedulerV3 (本模块)  ◄──┐                                 │
                          │   └ TCP server :PORT      │ DB 读写                          │
                          │   └ 顺序派发/回收/控制     │ (任务记录/终态)                  │
                          └────────────▲──────────────┼─────────────────────────────────┘
                                       │ TCP (集群内)  │
                          ┌────────────┴──────────────┴─────────────────────────────────┐
                          │            (N 个) Runner Pod (role=runner)                    │
                          │  ┌──────────────────────────────────────────────────────┐   │
                          │  │ WorkerControl (主进程=控制进程, 常驻)                   │   │
                          │  │   - TCP client → 连调度器 (持久连接, 断联=死)           │   │
                          │  │   - 收 RUN/CANCEL/RESTART 命令                          │   │
                          │  │   - spawn/kill 任务子进程 (TaskRunner, 单任务)          │   │
                          │  │   - 上报 task 心跳/状态                                 │   │
                          │  │   - 任务前后 + cancel 时清理 pod 进程(白名单:探针+自己)  │   │
                          │  │   - 任务进程被杀时代为归档产物                          │   │
                          │  │  probe sidecar (18080, k8s liveness/readiness)         │   │
                          │  └──────────────────────────────────────────────────────┘   │
                          └────────────────────────────────────────────────────────────┘
```

## 2. 协议（TCP 长连接 + 长度前缀 JSON 帧）

帧格式：`<4 字节大端 length><length 字节 UTF-8 JSON>`。

Worker → Scheduler（worker 上行）:
- `HELLO`        : `{type:"hello", worker_id, ip, max_tasks:1}` 连接建立首帧。
- `HEARTBEAT`    : `{type:"heartbeat", worker_id, task_id|null, task_state}` 周期上报（含"我空闲/我在跑哪个任务"）。
- `TASK_STATE`   : `{type:"task_state", worker_id, task_id, state, error?, result?}` 任务状态变更通知。
  - state ∈ {starting, running, finished, failed, cancelled}

Scheduler → Worker（调度器下行命令）:
- `RUN`          : `{type:"run", task_id, lease_epoch}` 下发一个任务。
- `CANCEL`       : `{type:"cancel", task_id}` 取消（worker 杀任务进程 + 代归档 + 清理 pod）。
- `RESTART`      : `{type:"restart", task_id, lease_epoch}` 重启任务。

**TCP 断联判定 worker 死亡**：调度器把该 worker 的在跑任务（DB lease 仍属于它）置为可回收，
待 worker 重连/或超时后回收重排。

## 3. 调度器职责（SchedulerV3）

1. **TCP server**：接受 worker 连接，维护 `{worker_id: WorkerConn}` 内存表（实时能力/在跑任务/最后心跳）。
2. **顺序派发**（非抢占）：从 DB 取 pending（FIFO）→ 选一个**空闲**（max_tasks=1 且无任务）worker → 写 DB lease(dispatcher_instance_id=worker, lease_epoch+1) → 发 `RUN` 命令。一个 worker 同一时刻最多 1 个任务。
3. **心跳监督**：worker 心跳超时 / TCP 断联 → 标记 worker offline → 其在跑任务待回收。
4. **回收**（对账式）：`status=running` 且 owner worker 已死/失联超过宽限 → 重新置 pending 重排（DB 终态账本兜底，rollout 安全）。
5. **运行时控制**：pause/drain/claim_enabled（沿用 V2 的 DB RuntimeControl）。
6. **cancel/restart**：API 收到 cancel/restart → 调度器发 `CANCEL`/`RESTART` 给 owner worker；worker 执行清理+代归档。

## 4. Worker 控制进程职责（WorkerControl）

1. **常驻主进程**（uvicorn + TCP client 线程）。
2. 收到 `RUN` → 若已有任务在跑则拒绝（单任务约束）；否则 **spawn 任务子进程**（运行 TaskRunner/Orchestrator），清理 pod（任务前，req 6）。
3. 周期向调度器上报 `HEARTBEAT`（task_id + state）。
4. 任务子进程结束 → 读其退出码/结果 → 发 `TASK_STATE` → **归档产物**（req 2：任务正常自己归档；被杀情况控制进程代归档）→ 清理 pod（任务后，req 6）。
5. 收到 `CANCEL` → 杀任务子进程 + 清理 pod 进程（白名单：探针 sidecar + 控制主进程，req 5/6）→ 代归档 → 上报 cancelled。
6. 收到 `RESTART` → 杀旧任务子进程 + 清理 + 重新 spawn。
7. TCP 断联 → 重连退避（控制进程自身不能因为调度器短暂重启就死）。

## 5. 持久化边界（rollout 安全）

- **DB 始终是任务账本**：pending/running/passed/failed/error、lease_epoch、dispatcher_instance_id、终态 result_json 都写 DB。
- 调度器重启：从 DB 重建「running 任务 → 其 owner」视图；worker 重连后对账；失联超时的 running 任务回收重排。**不丢任务**。
- 控制进程重启：worker 重建，调度器视其旧任务为孤儿（lease 仍在 DB）→ 宽限后回收 → 重新下发。
- TCP 控制面只为「实时派发/心跳/命令」提速与简化，不承担持久化。

## 6. 实施 Phase（主线分支 main）

- **P1** 协议 + SchedulerV3(TCP server) + WorkerControl(TCP client + 任务子进程) 骨架，独立模块，不接 task_service（可单测）。
- **P2** 接线：task_service 按角色启动 SchedulerV3(manager) / WorkerControl(runner)；API cancel/restart 改走 SchedulerV3。
- **P3** 任务子进程化：TaskRunner 单进程入口；产物归档（正常/代归档）；pod 进程清理白名单（探针+控制主进程）。
- **P4** 回收/对账/rollout 验证 + 4 项需求回归（领取/取消/重启/rollout）。
- **P5** 清理 V2 死代码（WorkerDispatcher/runner_assignment_loop/runner_registry 的派发路径）+ 文档。
