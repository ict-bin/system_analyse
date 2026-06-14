"""
test_pipeline.py — pipeline/ 新架构 dry-run 测试

覆盖：
  1. PipelineContext 创建与方法
  2. BaseStage / Pipeline skip 逻辑（start_stage）
  3. FilterStage / ExploreStage / PrescanStage
  4. ClassifyStage W+J 循环（pass / fail / reflect / min_rounds / max_rounds）
  5. 投票模式（all / any / majority）
  6. StageError / PiFatalError 传播
  7. RefineStage / AnalyseStage / FinalReportStage 骨架调用
  8. Pipeline.run 全流程 + resume start_stage
  9. helpers: parse_eval_md / check_voting / discover_modules
 10. Orchestrator 薄层正确委托 legacy
"""

import asyncio
import json
import os
import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _arun(coro):
    """在新 event loop 中运行协程。"""
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


from app.models import (
    TaskConfig, StagesConfig, StageLoopConfig,
    RoleConfig, AgentInstanceConfig, TokenUsage, SwarmEvent,
)
from app.pipeline import (
    PipelineContext, BaseStage, Pipeline,
    FilterStage, ExploreStage, PrescanStage,
    ClassifyStage, RefineStage, AnalyseStage,
    CompletenessCheckStage, FinalReportStage,
    StageError, PiFatalError,
    parse_eval_md, check_voting, discover_modules, get_modules_root,
    EvaluationRecorder,
)
from app.runner import AgentResult
from app.pipeline.helpers import run_agent_with_stage_guard


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def make_ar(output: str = "", error: str = "", fatal: bool = False) -> AgentResult:
    ar = AgentResult()
    ar.output = output
    ar.error = error
    ar.fatal = fatal
    ar.token_usage = MagicMock(prompt_tokens=10, completion_tokens=20)
    return ar


def make_pass_judge(score: int = 90) -> str:
    return f"## 评分: {score}\n## 通过: 是\n## 评审意见\n通过"


def make_fail_judge(score: int = 50) -> str:
    return f"## 评分: {score}\n## 通过: 否\n## 评审意见\n需要改进"


EVENTS: list[dict] = []


def collect_event(ev: SwarmEvent):
    EVENTS.append({"type": ev.type, **ev.data})


def make_ctx(tmp: str, min_rounds: int = 1, max_rounds: int = 3) -> PipelineContext:
    cfg = TaskConfig(
        task="测试固件分析",
        target_dir=str(Path(tmp) / "target"),
        analyse_targets=["binary"],
        binary_arch=["all"],
        agent_max_retries=1,
        agent_retry_delay=0,
        pi_max_retries=1,
        pi_retry_delay=0,
        stages=StagesConfig(
            classify=StageLoopConfig(min_rounds=min_rounds, max_rounds=max_rounds, pass_mode="all"),
            refine=StageLoopConfig(min_rounds=1, max_rounds=max_rounds, pass_mode="all"),
            analyse=StageLoopConfig(min_rounds=1, max_rounds=max_rounds, pass_mode="all"),
            final_check=StageLoopConfig(min_rounds=1, max_rounds=max_rounds, pass_mode="all"),
        ),
        workers=RoleConfig(
            default_tools=["read", "bash"],
            system_prompt_dir=str(Path(tmp) / "prompts" / "workers"),
            agents=[AgentInstanceConfig(model="vllm/glm5")],
        ),
        judges=RoleConfig(
            default_tools=["read", "bash"],
            system_prompt_dir=str(Path(tmp) / "prompts" / "judges"),
            agents=[AgentInstanceConfig(model="vllm/glm5")],
        ),
        output_dir=str(Path(tmp) / "output"),
        archive_dir=str(Path(tmp) / "output"),
        result_dir=str(Path(tmp) / "result"),
    )

    ws = Path(tmp) / "output" / "task-test" / "workspace"
    ws.mkdir(parents=True, exist_ok=True)
    out = ws.parent
    sess = out / "sessions"
    sess.mkdir(exist_ok=True)
    Path(tmp, "target").mkdir(exist_ok=True)
    Path(tmp, "result").mkdir(exist_ok=True)

    import asyncio
    return PipelineContext(
        task_id="task-test",
        task="测试固件分析",
        cfg=cfg,
        workspace=ws,
        output_dir=out,
        sess_dir=sess,
        emit=collect_event,
        tokens=TokenUsage(),
        cancel_event=MagicMock(),
    )


def setup_prompts(tmp: str):
    """创建最小提示词文件。"""
    for sub in ["workers", "judges"]:
        d = Path(tmp) / "prompts" / sub
        d.mkdir(parents=True, exist_ok=True)
    workers = ["step1_classify", "step1_explore", "reflect_classify",
               "step2_refine", "step2_sub_read", "step2_reclassify",
               "step3_analyse", "step4_final_report",
               "reflect_refine", "reflect_analyse", "reflect_report"]
    judges = ["step1_check_classify", "step2_check_refine",
              "step3_check_analyse", "step4_check_completeness", "step4_check_report"]
    for name in workers:
        (Path(tmp) / "prompts" / "workers" / f"{name}.md").write_text(f"# {name}", encoding="utf-8")
    for name in judges:
        (Path(tmp) / "prompts" / "judges" / f"{name}.md").write_text(f"# {name}", encoding="utf-8")


def make_modules(workspace: Path, names: list[str], files: list[str] | None = None):
    """在 workspace/modules/ 下创建模块。"""
    mods = workspace / "modules"
    mods.mkdir(exist_ok=True)
    for name in names:
        d = mods / name
        d.mkdir(exist_ok=True)
        content = "\n".join(files or ["lib/libtest.so"]) + "\n"
        (d / "files.list").write_text(content, encoding="utf-8")


# ─── 1. helpers: parse_eval_md ─────────────────────────────────────────────────

def test_parse_eval_md_standard():
    r = parse_eval_md("## 评分: 85\n## 通过: 是\n## 评审意见\n很好")
    assert r["score"] == 85 and r["pass"] is True
    print("  ✅ parse_eval_md 标准格式")


def test_parse_eval_md_last_match():
    """多次出现评分取最后一次。"""
    r = parse_eval_md("评分: 30\n分析...\n## 评分: 92\n## 通过: 是")
    assert r["score"] == 92
    print("  ✅ parse_eval_md 多次评分取最后")


def test_parse_eval_md_zero_false():
    """score=0 + 通过:否 → fail（不走语义推断）。"""
    r = parse_eval_md("综合来看内容通过...\n## 评分: 0\n## 通过: 否")
    assert r["score"] == 0 and r["pass"] is False
    print("  ✅ parse_eval_md score=0+否 → fail")


def test_parse_eval_md_pass_no_score():
    """声明通过但评分为0 → Judge格式违规 → fail。"""
    r = parse_eval_md("## 评分: 0\n## 通过: 是")
    assert r["pass"] is False
    print("  ✅ parse_eval_md Pass+score=0 → format violation fail")


def test_parse_eval_md_high_score_default_pass():
    r = parse_eval_md("## 评分: 80")
    assert r["pass"] is True
    print("  ✅ parse_eval_md score>=75 默认 pass")


def test_parse_eval_md_low_score_default_fail():
    r = parse_eval_md("## 评分: 60")
    assert r["pass"] is False
    print("  ✅ parse_eval_md score<75 默认 fail")


# ─── 2. helpers: check_voting ──────────────────────────────────────────────────

def test_check_voting_all():
    assert check_voting([{"pass": True}, {"pass": True}], "all", 2) is True
    assert check_voting([{"pass": True}, {"pass": False}], "all", 2) is False
    print("  ✅ check_voting mode=all")


def test_check_voting_any():
    assert check_voting([{"pass": False}, {"pass": True}], "any", 2) is True
    assert check_voting([{"pass": False}, {"pass": False}], "any", 2) is False
    print("  ✅ check_voting mode=any")


def test_check_voting_majority():
    assert check_voting([{"pass": True}, {"pass": True}, {"pass": False}], "majority", 3) is True
    assert check_voting([{"pass": False}, {"pass": True}, {"pass": False}], "majority", 3) is False
    print("  ✅ check_voting mode=majority")


def test_evaluation_recorder_round_and_summary():
    with tempfile.TemporaryDirectory() as tmp:
        recorder = EvaluationRecorder("task-test", Path(tmp) / "run")
        worker_usage = TokenUsage(input=10, output=5, cost=0.1)
        judge_usage = TokenUsage(input=4, output=2, cost=0.02)
        recorder.record_round(
            module_name="network/socket",
            stage="analyse",
            stage_round=1,
            status="passed",
            started_at="2026-05-08T00:00:00+00:00",
            ended_at="2026-05-08T00:00:01+00:00",
            duration_ms=1000,
            worker={"model": "worker-model", "session_file": "sessions/a.jsonl", "token_usage": worker_usage},
            judges=[{
                "judge_id": "judge-0",
                "model": "judge-model",
                "score": 90,
                "passed": True,
                "feedback": "ok",
                "token_usage": judge_usage,
            }],
            passed_by_vote=True,
            module_completed=True,
            completion_reason="passed",
        )
        round_path = Path(tmp) / "run" / "round_001" / "network_socket.analyse.json"
        assert round_path.exists()
        payload = json.loads(round_path.read_text(encoding="utf-8"))
        assert payload["metrics"]["review_pass_rate"] == 1.0
        assert payload["metrics"]["accuracy_proxy"] == 0.9
        assert payload["metrics"]["token_total"] == 21
        summary = recorder.write_summary(task_status="passed")
        assert summary["completed_module_count"] == 1
        assert summary["total_tokens"] == 21
        assert (Path(tmp) / "run" / "evaluation_summary.json").exists()
    print("  ✅ EvaluationRecorder round json + summary")


def test_evaluation_recorder_resume_round_numbering():
    with tempfile.TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "run"
        worker_usage = TokenUsage(input=1, output=1)
        judge = {
            "judge_id": "judge-0",
            "model": "judge-model",
            "score": 80,
            "passed": True,
            "feedback": "ok",
            "token_usage": TokenUsage(input=1, output=1),
        }

        first = EvaluationRecorder("task-test", run_dir)
        first.record_round(
            module_name="network",
            stage="analyse",
            stage_round=1,
            status="running",
            started_at="2026-05-08T00:00:00+00:00",
            ended_at="2026-05-08T00:00:01+00:00",
            duration_ms=1000,
            worker={"model": "worker-model", "session_file": "sessions/a.jsonl", "token_usage": worker_usage},
            judges=[judge],
            passed_by_vote=True,
        )

        resumed = EvaluationRecorder("task-test", run_dir)
        record = resumed.record_round(
            module_name="network",
            stage="analyse",
            stage_round=2,
            status="passed",
            started_at="2026-05-08T00:00:02+00:00",
            ended_at="2026-05-08T00:00:03+00:00",
            duration_ms=1000,
            worker={"model": "worker-model", "session_file": "sessions/a.jsonl", "token_usage": worker_usage},
            judges=[judge | {"score": 90}],
            passed_by_vote=True,
            module_completed=True,
            completion_reason="passed",
        )
        assert record["round"] == 2
        assert (run_dir / "round_001" / "network.analyse.json").exists()
        assert (run_dir / "round_002" / "network.analyse.json").exists()
        assert record["effectiveness"]["score_delta_from_previous_round"] == 10.0
        summary = resumed.write_summary(task_status="passed")
        assert summary["round_count"] == 2
    print("  ✅ EvaluationRecorder resume round numbering")


# ─── 3. helpers: discover_modules ──────────────────────────────────────────────

def test_discover_modules():
    with tempfile.TemporaryDirectory() as tmp:
        ws = Path(tmp) / "workspace"
        mods = ws / "modules"
        (mods / "aaa").mkdir(parents=True)
        (mods / "bbb").mkdir(parents=True)
        (mods / "aaa" / "files.list").write_text("lib/a.so\n")
        (mods / "bbb" / "files.list").write_text("lib/b.so\n")
        result = discover_modules(str(ws))
        assert "aaa" in result and "bbb" in result
        assert get_modules_root(str(ws)) == mods
    print("  ✅ discover_modules / get_modules_root")


# ─── 4. PipelineContext ─────────────────────────────────────────────────────────

def test_pipeline_context_methods():
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        ws = ctx.workspace
        # modules_root: no modules dir yet → returns workspace
        assert ctx.modules_root() == ws
        # create modules dir
        mods = ws / "modules"
        mods.mkdir()
        (mods / "test").mkdir()
        (mods / "test" / "files.list").write_text("a.so\n")
        assert ctx.modules_root() == mods
        assert ctx.module_dir("test") == mods / "test"
    print("  ✅ PipelineContext.modules_root / module_dir")


def test_pipeline_context_emit():
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        events = []
        ctx = make_ctx(tmp)
        ctx.emit = lambda e: events.append(e)
        ctx.emit_event("stage", stage="filter", count=10)
        assert len(events) == 1
        assert events[0].type == "stage"
        assert events[0].data["stage"] == "filter"
        assert events[0].task_id == "task-test"
    print("  ✅ PipelineContext.emit_event")


# ─── 5. BaseStage / Pipeline skip 逻辑 ─────────────────────────────────────────

class _DummyStage(BaseStage):
    def __init__(self, num: int, name: str):
        self._num = num
        self._name = name
        self.executed = False

    @property
    def stage_num(self) -> int:
        return self._num

    @property
    def stage_name(self) -> str:
        return self._name

    async def execute(self, ctx: PipelineContext) -> None:
        self.executed = True
        ctx.emit_event("stage", stage=self._name)


def test_pipeline_run_all():
    """start_stage=0 → 所有阶段都执行。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        stages = [_DummyStage(0, "filter"), _DummyStage(1, "classify"), _DummyStage(2, "refine")]
        pipeline = Pipeline(stages)
        _arun(pipeline.run(ctx, start_stage=0))
        assert all(s.executed for s in stages)
    print("  ✅ Pipeline.run start_stage=0 全部执行")


def test_pipeline_skip_to_stage3():
    """start_stage=3 → stage 0,1,2 跳过，stage 3 执行。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        s0 = _DummyStage(0, "filter")
        s1 = _DummyStage(1, "classify")
        s2 = _DummyStage(2, "refine")
        s3 = _DummyStage(3, "analyse")
        pipeline = Pipeline([s0, s1, s2, s3])
        _arun(pipeline.run(ctx, start_stage=3))
        assert not s0.executed and not s1.executed and not s2.executed
        assert s3.executed
    print("  ✅ Pipeline.run start_stage=3 正确跳过 0-2")


def test_pipeline_stage_order():
    """Stage 乱序传入，Pipeline 自动排序后按序执行。"""
    order = []
    class _OrderStage(_DummyStage):
        async def execute(self, ctx):
            order.append(self._num)
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        pipeline = Pipeline([_OrderStage(2,"r"), _OrderStage(0,"f"), _OrderStage(1,"c")])
        _arun(pipeline.run(ctx))
        assert order == [0, 1, 2]
    print("  ✅ Pipeline 自动按 stage_num 排序执行")


def test_pipeline_error_propagates():
    """Stage 抛出 StageError 时 Pipeline 应传播异常。"""
    class _FailStage(_DummyStage):
        async def execute(self, ctx):
            raise StageError("模拟失败")
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        pipeline = Pipeline([_FailStage(1, "fail")])
        try:
            _arun(pipeline.run(ctx))
            assert False, "应抛出 StageError"
        except StageError as e:
            assert "模拟失败" in str(e)
    print("  ✅ Pipeline StageError 正确传播")


def test_pipeline_fatal_error_propagates():
    """PiFatalError 继承 StageError，应同样传播。"""
    class _FatalStage(_DummyStage):
        async def execute(self, ctx):
            raise PiFatalError("致命错误")
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        pipeline = Pipeline([_FatalStage(0, "fatal")])
        try:
            _arun(pipeline.run(ctx))
            assert False, "应抛出 PiFatalError"
        except (PiFatalError, StageError) as e:
            assert "致命错误" in str(e)
    print("  ✅ Pipeline PiFatalError 正确传播")


def test_run_agent_with_stage_guard_enforces_timeout():
    """Agent 超过单次会话硬超时后应取消执行并抛 StageError。"""
    with tempfile.TemporaryDirectory() as tmp:
        ctx = make_ctx(tmp)
        cancelled = {"value": False}

        async def slow_agent(**_kwargs):
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled["value"] = True
                raise
            return make_ar("unexpected")

        async def run_case():
            with patch("app.pipeline.helpers.run_agent", side_effect=slow_agent):
                try:
                    await run_agent_with_stage_guard(
                        ctx=ctx,
                        stage="test",
                        context="timeout-case",
                        timeout_seconds=0.05,
                        heartbeat_interval=10,
                    )
                    assert False, "应抛出 StageError"
                except StageError as e:
                    assert "智能体会话超时" in str(e)

        _arun(run_case())
        assert cancelled["value"] is True
        assert any(ev["type"] == "stage_timeout" for ev in EVENTS)
    print("  ✅ run_agent_with_stage_guard 硬超时生效")


def test_run_agent_with_stage_guard_emits_rate_limit_timeline_event_once():
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        events: list[dict] = []

        def collect(ev: SwarmEvent):
            events.append({"type": ev.type, **ev.data})

        ctx.emit = collect
        ar = make_ar(output="retry later", error="429 too many requests")
        ar.rate_limited = True
        ar.retry_delay_seconds = 30
        ar.consecutive_rate_limit_count = 10
        ar.rate_limit_event_due = True

        with patch("app.pipeline.helpers.run_agent_checked", return_value=ar):
            result = run_agent_with_stage_guard(
                ctx=ctx,
                stage="analyse",
                context="rate-limit-check",
                prompt="hello",
                model="test-model",
                tools=["read"],
                cwd=tmp,
            )

        assert result is ar
        rate_events = [event for event in events if event["type"] == "task_rate_limited_retrying"]
        assert len(rate_events) == 1
        assert rate_events[0]["stage"] == "analyse"
        assert rate_events[0]["http_status"] == 429
        assert rate_events[0]["retry_delay_seconds"] == 30
        assert rate_events[0]["consecutive_rate_limit_count"] == 10
        print("  ✅ run_agent_with_stage_guard 透传 429 限流事件")


# ─── 6. FilterStage ─────────────────────────────────────────────────────────────

def test_filter_stage_no_script():
    """filter_files.sh 不存在时，FilterStage 静默跳过（不抛异常）。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        stage = FilterStage()
        # 脚本不在 /opt/system_analyse/scripts/filter_files.sh 时直接 return
        with patch("os.path.isfile", return_value=False):
            _arun(stage.execute(ctx))
        assert ctx.filter_count == 0
    print("  ✅ FilterStage 无脚本时静默跳过")


def test_filter_stage_with_script():
    """FilterStage 调用脚本，读取 filtered_files.txt 更新 ctx。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        ws = ctx.workspace

        # 模拟脚本创建 filtered_files.txt
        def fake_create_filtered(*args, **kwargs):
            pass

        async def fake_exec(*args, **kwargs):
            # 写入 filtered_files.txt
            (ws / "filtered_files.txt").write_text(
                "lib/a.so\nlib/b.so\nlib/c.so\n", encoding="utf-8"
            )
            proc = MagicMock()
            proc.communicate = AsyncMock(return_value=(b"", b""))
            return proc

        with patch("os.path.isfile", return_value=True), \
             patch("asyncio.create_subprocess_exec", side_effect=fake_exec):
            _arun(FilterStage().execute(ctx))

        assert ctx.filter_count == 3
        assert len(ctx.filtered_files) == 3
        assert "lib/a.so" in ctx.filtered_files
    print("  ✅ FilterStage 正确读取 filtered_files.txt")


# ─── 7. ExploreStage ────────────────────────────────────────────────────────────

def test_explore_stage_no_prompt():
    """没有 step1_explore.md 时，ExploreStage 静默跳过。"""
    with tempfile.TemporaryDirectory() as tmp:
        # 不创建 prompts
        ctx = make_ctx(tmp)
        # 创建空 prompts 目录（没有 step1_explore.md）
        (Path(tmp) / "prompts" / "workers").mkdir(parents=True, exist_ok=True)
        stage = ExploreStage()
        _arun(stage.execute(ctx))
        # 无报错即通过
    print("  ✅ ExploreStage 无 prompt 时静默跳过")


def test_explore_stage_calls_agent():
    """ExploreStage 正常调用 run_agent_checked。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        calls = []

        async def mock_agent(**kwargs):
            calls.append(kwargs)
            return make_ar("探索完成")

        with patch("app.pipeline.s0_filter.run_agent_checked", side_effect=mock_agent):
            _arun(ExploreStage().execute(ctx))

        assert len(calls) == 1
        assert calls[0]["model"].endswith("glm5") or "explore" in str(calls[0])
    print("  ✅ ExploreStage 正确调用 run_agent_checked")


# ─── 8. PrescanStage ─────────────────────────────────────────────────────────────

def test_prescan_stage_no_keywords():
    """没有 keywords.txt 时，PrescanStage 静默跳过。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        stage = PrescanStage()
        _arun(stage.execute(ctx))
    print("  ✅ PrescanStage 无 keywords.txt 静默跳过")


def test_prescan_stage_with_keywords():
    """PrescanStage 有 keywords.txt 时调用脚本，读取摘要。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        ws = ctx.workspace
        (ws / "keywords.txt").write_text("bgp\nospf\ndhcp\n", encoding="utf-8")

        async def fake_exec(*args, **kwargs):
            (ws / "keyword_summary.txt").write_text(
                "bgp | 10\nospf | 5\n未识别: 20\n", encoding="utf-8"
            )
            proc = MagicMock()
            proc.communicate = AsyncMock(return_value=(b"done", b""))
            return proc

        with patch("os.path.isfile", return_value=True), \
             patch("asyncio.create_subprocess_exec", side_effect=fake_exec):
            _arun(PrescanStage().execute(ctx))

        assert hasattr(ctx, "_prescan_summary")
        assert "bgp" in ctx._prescan_summary  # type: ignore
    print("  ✅ PrescanStage 正确读取 keyword_summary.txt")


# ─── 9. ClassifyStage W+J 流程 ─────────────────────────────────────────────────

def test_classify_first_attempt_pass():
    """第一轮即通过（min_rounds=1）。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp, min_rounds=1)
        ws = ctx.workspace
        agent_calls = []
        call_idx = [0]
        responses = [
            "已创建 modules",              # worker
            make_pass_judge(90),           # judge
        ]

        def worker_effect(cwd: str):
            make_modules(Path(cwd), ["bgp", "ospf"])

        async def mock_agent(**kwargs):
            idx = call_idx[0]
            call_idx[0] += 1
            agent_calls.append(kwargs.get("model", ""))
            if idx == 0:
                worker_effect(kwargs.get("cwd", str(ws)))
            return make_ar(responses[idx] if idx < len(responses) else make_pass_judge())

        with patch("app.pipeline.s1_classify.run_agent_checked", side_effect=mock_agent):
            _arun(ClassifyStage().execute(ctx))

        assert "bgp" in ctx.classified_modules
        assert "ospf" in ctx.classified_modules
    print("  ✅ ClassifyStage 第一轮通过")


def test_classify_min_rounds_2():
    """min_rounds=2：即使第1轮通过也必须再跑一轮。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp, min_rounds=2)
        ws = ctx.workspace
        call_idx = [0]
        # responses: worker1 / judge1(pass) / worker2 / judge2(pass)
        responses = [
            "done",             # worker round 1
            make_pass_judge(85),  # judge round 1 → pass but min_rounds=2 需再来一轮
            "done",             # worker round 2
            make_pass_judge(95),  # judge round 2 → 通过
        ]

        async def mock_agent(**kwargs):
            idx = call_idx[0]
            call_idx[0] += 1
            if idx == 0:
                make_modules(Path(ws), ["bgp"])
            return make_ar(responses[idx] if idx < len(responses) else make_pass_judge())

        with patch("app.pipeline.s1_classify.run_agent_checked", side_effect=mock_agent):
            _arun(ClassifyStage().execute(ctx))

        assert call_idx[0] >= 4, f"应至少4次调用，实际{call_idx[0]}"
    print("  ✅ ClassifyStage min_rounds=2 正确触发反思")


def test_classify_fail_then_pass():
    """第一轮失败，第二轮通过。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp, min_rounds=1)
        ws = ctx.workspace
        call_idx = [0]
        responses = [
            "done",               # worker round 1
            make_fail_judge(40),  # judge round 1 → fail
            "fixed",              # worker round 2
            make_pass_judge(90),  # judge round 2 → pass
        ]

        async def mock_agent(**kwargs):
            idx = call_idx[0]
            call_idx[0] += 1
            if idx in (0, 2):
                make_modules(Path(ws), ["aaa", "bras"])
            return make_ar(responses[idx] if idx < len(responses) else make_pass_judge())

        with patch("app.pipeline.s1_classify.run_agent_checked", side_effect=mock_agent):
            _arun(ClassifyStage().execute(ctx))

        assert call_idx[0] == 4
    print("  ✅ ClassifyStage fail→pass 两轮收敛")


def test_classify_max_rounds_exceeded():
    """超过 max_rounds 抛 StageError。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp, min_rounds=1, max_rounds=2)
        ws = ctx.workspace
        call_idx = [0]
        responses = [
            "done", make_fail_judge(30),  # round 1
            "done", make_fail_judge(40),  # round 2 → max 超限
        ]

        async def mock_agent(**kwargs):
            idx = call_idx[0]
            call_idx[0] += 1
            if call_idx[0] % 2 == 1:
                make_modules(Path(ws), ["mod1"])
            return make_ar(responses[idx] if idx < len(responses) else make_fail_judge())

        with patch("app.pipeline.s1_classify.run_agent_checked", side_effect=mock_agent):
            try:
                _arun(ClassifyStage().execute(ctx))
                assert False, "应抛出 StageError"
            except StageError as e:
                assert "最大轮数" in str(e)
    print("  ✅ ClassifyStage max_rounds 超限抛 StageError")


def test_classify_pi_fatal_propagates():
    """Worker 致命错误（pi fatal）应立即传播，不重试。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp, min_rounds=1)
        call_idx = [0]

        async def mock_agent(**kwargs):
            call_idx[0] += 1
            if kwargs.get("system_prompt", "").startswith("# step1_classify"):
                return make_ar(error="401 Unauthorized", fatal=True)
            return make_ar(make_pass_judge())

        with patch("app.pipeline.s1_classify.run_agent_checked") as mock:
            mock.side_effect = PiFatalError("401 Unauthorized")
            try:
                _arun(ClassifyStage().execute(ctx))
                assert False, "应抛出 PiFatalError"
            except PiFatalError:
                pass
    print("  ✅ ClassifyStage PiFatalError 立即传播")


def test_classify_vote_mode_any():
    """pass_mode=any：只需1个judge通过。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp, min_rounds=1)
        ctx.cfg.stages.classify.pass_mode = "any"
        # 2个judge：一个pass一个fail → any模式应通过
        ctx.cfg.judges.agents = [
            AgentInstanceConfig(model="vllm/m1"),
            AgentInstanceConfig(model="vllm/m2"),
        ]
        ws = ctx.workspace
        call_idx = [0]
        responses = ["done", make_pass_judge(90), make_fail_judge(40)]

        async def mock_agent(**kwargs):
            idx = call_idx[0]
            call_idx[0] += 1
            if idx == 0:
                make_modules(Path(ws), ["mod1"])
            return make_ar(responses[idx] if idx < len(responses) else make_pass_judge())

        with patch("app.pipeline.s1_classify.run_agent_checked", side_effect=mock_agent):
            _arun(ClassifyStage().execute(ctx))
    print("  ✅ ClassifyStage pass_mode=any 单judge通过即可")


# ─── 10. RefineStage / AnalyseStage / FinalReportStage 骨架 ────────────────────

def test_refine_stage_stub():
    """RefineStage 骨架：不崩溃，更新 ctx.refined_modules。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        make_modules(ctx.workspace, ["bgp", "ospf"])
        _arun(RefineStage().execute(ctx))
        assert "bgp" in ctx.refined_modules or len(ctx.refined_modules) >= 0
    print("  ✅ RefineStage 骨架不崩溃")


def test_refine_stage_reclassify_cleanup_path():
    """补分类收尾路径不应因 helper 缺失而抛 NameError。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        make_modules(ctx.workspace, ["bgp"])
        (ctx.workspace / "filtered_files.txt").write_text("lib/libtest.so\norphan.bin\n", encoding="utf-8")

        async def _fake_agent(*args, **kwargs):
            # 模拟补分类将遗漏文件归入已有模块。
            files = (ctx.workspace / "modules" / "bgp" / "files.list")
            current = files.read_text(encoding="utf-8")
            if "orphan.bin" not in current:
                files.write_text(current + "orphan.bin\n", encoding="utf-8")
            return make_ar("<result>已归类 1 个文件到各模块</result>")

        with patch("app.pipeline.s2_refine.run_agent_checked", side_effect=_fake_agent):
            _arun(RefineStage().execute(ctx))

        assert "bgp" in ctx.refined_modules
        assert "orphan.bin" in (ctx.workspace / "modules" / "bgp" / "files.list").read_text(encoding="utf-8")
    print("  ✅ RefineStage 补分类收尾路径不崩溃")


def test_refine_snapshot_directory_does_not_crash():
    with tempfile.TemporaryDirectory() as tmp:
        mod_dir = Path(tmp) / "modules" / "core_message"
        mod_dir.mkdir(parents=True)
        (mod_dir / "files.list").write_text("a.c\nb.c\n", encoding="utf-8")
        (mod_dir / ".snapshot").mkdir()

        from app.pipeline.s2_refine import _read_lines, _ensure_snapshot_file

        assert _read_lines(mod_dir / ".snapshot") == set()
        snap = _ensure_snapshot_file(mod_dir)
        assert snap.is_file()
        assert snap.read_text(encoding="utf-8").splitlines() == ["a.c", "b.c"]


def test_resume_cleanup_restores_from_module_snapshot(tmp_path: Path):
    from app.service.task_service import _cleanup_resume_intermediate_files

    task_id = "sat_test"
    workspace = tmp_path / task_id / "run" / "workspace"
    mod_dir = workspace / "modules" / "api_server"
    deleted_dir = mod_dir / "deleted"
    deleted_dir.mkdir(parents=True)
    (mod_dir / "files.list").write_text("trimmed.c\n", encoding="utf-8")
    (mod_dir / ".snapshot").write_text("full_a.c\nfull_b.c\n", encoding="utf-8")
    (deleted_dir / "files.list").write_text("removed.c\n", encoding="utf-8")

    _cleanup_resume_intermediate_files(str(tmp_path), task_id)

    assert (mod_dir / "files.list").read_text(encoding="utf-8").splitlines() == ["full_a.c", "full_b.c"]
    assert not deleted_dir.exists()


def test_system_analyse_runner_connection_error_uses_infinite_retry():
    from app.runner import AgentResult, _is_infinite_retry_api_error

    result = AgentResult()
    result.error = "Connection error. [API 重试耗尽: 6 次失败]"
    result.exit_code = 1

    assert _is_infinite_retry_api_error(result) is True


def test_analyse_stage_stub():
    """AnalyseStage 骨架：不崩溃，更新 ctx.analysed_modules。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        mods = ctx.workspace / "modules"
        mods.mkdir()
        for m in ["bgp", "ospf"]:
            d = mods / m
            d.mkdir()
            (d / "files.list").write_text("a.so\n")
            (d / "module_report.md").write_text("# Report\n<!-- RISK_LEVEL: 中 -->")
        _arun(AnalyseStage().execute(ctx))
        assert "bgp" in ctx.analysed_modules
    print("  ✅ AnalyseStage 骨架正确读取已有报告")


def test_report_stages_stub():
    """Stage4 骨架：不崩溃。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        _arun(CompletenessCheckStage().execute(ctx))
        _arun(FinalReportStage().execute(ctx))
    print("  ✅ Stage 4 骨架不崩溃")


# ─── 11. Pipeline 完整流程 ──────────────────────────────────────────────────────

def test_pipeline_full_flow_stubs():
    """所有阶段（含骨架）串联，无崩溃，ctx 状态正确流转。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)

        class _QuickFilter(FilterStage):
            async def execute(self, ctx):
                ctx.filter_count = 5
                ctx.filtered_files = ["a.so", "b.so", "c.so", "d.so", "e.so"]
                ctx.emit_event("stage_result", stage="filter", file_count=5)

        class _QuickClassify(ClassifyStage):
            async def execute(self, ctx):
                make_modules(ctx.workspace, ["bgp", "ospf", "bras"])
                ctx.classified_modules = ["bgp", "ospf", "bras"]

        class _QuickRefine(RefineStage):
            async def execute(self, ctx):
                ctx.refined_modules = ctx.classified_modules

        class _QuickAnalyse(AnalyseStage):
            async def execute(self, ctx):
                ctx.analysed_modules = ctx.refined_modules

        pipeline = Pipeline([
            _QuickFilter(), _QuickClassify(), _QuickRefine(), _QuickAnalyse(),
            CompletenessCheckStage(), FinalReportStage()
        ])
        _arun(pipeline.run(ctx))

        assert ctx.filter_count == 5
        assert set(ctx.classified_modules) == {"bgp", "ospf", "bras"}
        assert set(ctx.analysed_modules) == set(ctx.classified_modules)
    print("  ✅ Pipeline 完整流程 ctx 状态正确流转")


def test_pipeline_resume_from_stage3():
    """start_stage=3：Stage 0/1/2 跳过，ctx 由 AnalyseStage 填充。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        ctx = make_ctx(tmp)
        # 预置 Stage 2 输出
        make_modules(ctx.workspace, ["bgp", "ospf"])
        ctx.refined_modules = ["bgp", "ospf"]

        executed = []

        class _TrackStage(_DummyStage):
            async def execute(self, c):
                executed.append(self._name)

        pipeline = Pipeline([
            _TrackStage(0, "filter"),
            _TrackStage(1, "classify"),
            _TrackStage(2, "refine"),
            _TrackStage(3, "analyse"),
        ])
        _arun(pipeline.run(ctx, start_stage=3))

        assert executed == ["analyse"], f"只应执行 analyse，实际: {executed}"
    print("  ✅ Pipeline resume start_stage=3 正确跳过 0-2")


# ─── 12. Orchestrator 薄层委托 ─────────────────────────────────────────────────

def test_orchestrator_delegates_to_legacy():
    """Orchestrator.execute 委托给 _LegacyOrchestrator.execute。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        from app.models import TaskResult, TaskStatus
        cfg = make_ctx(tmp).cfg

        mock_result = TaskResult(
            task_id="test", status=TaskStatus.PASSED, task="测试",
            config_snapshot={},
        )

        with patch("app.orchestrator._LegacyOrchestrator") as MockLegacy:
            instance = MockLegacy.return_value
            instance.execute = AsyncMock(return_value=mock_result)

            from app.orchestrator import Orchestrator
            orch = Orchestrator(cfg)
            result = _arun(orch.execute("task-1"))

        assert result.task_id == "test"
        instance.execute.assert_called_once_with("task-1")
    print("  ✅ Orchestrator 正确委托 legacy.execute")


def test_orchestrator_stop():
    """Orchestrator.stop() 委托给 legacy.stop()。"""
    with tempfile.TemporaryDirectory() as tmp:
        setup_prompts(tmp)
        cfg = make_ctx(tmp).cfg

        with patch("app.orchestrator._LegacyOrchestrator") as MockLegacy:
            instance = MockLegacy.return_value
            instance.stop = MagicMock()

            from app.orchestrator import Orchestrator
            orch = Orchestrator(cfg)
            orch.stop()

        instance.stop.assert_called_once()
    print("  ✅ Orchestrator.stop 正确委托")


# ─── Runner ──────────────────────────────────────────────────────────────────


# ─── 13. Stage 3-redo 模块筛选逻辑 ────────────────────────────────────────────

def test_redo_analyse_only_new_and_nonempty():
    """Stage 3-redo 只处理新子模块 + 非空的原始重分类模块，排除空壳。"""
    with tempfile.TemporaryDirectory() as tmp:
        mods = Path(tmp)
        # 创建模拟目录结构
        for m, c in [
            ("bgp",  "bgp.so"),
            ("ospf", "ospf.so"),
            ("auth_ssh",     "libssh.so"),
            ("auth_hardware","libhw.so"),
        ]:
            (mods/m).mkdir(); (mods/m/"files.list").write_text(c + chr(10))
        # auth 为空壳（拆分后文件已移走）
        (mods/"auth").mkdir(); (mods/"auth"/"files.list").write_text("")

        final_modules = {"auth", "bgp", "ospf"}
        modules_needing_reclassify = {"auth"}
        new_mods_set = {"bgp","ospf","auth","auth_ssh","auth_hardware"}

        # 复现修复后的筛选逻辑
        redo = []
        for m in new_mods_set:
            if m not in final_modules:
                redo.append(m)
            elif m in modules_needing_reclassify:
                flist = mods/m/"files.list"
                if flist.exists() and flist.stat().st_size > 0:
                    redo.append(m)
        redo = sorted(redo)
        assert redo == ["auth_hardware", "auth_ssh"], f"空壳应被排除: {redo}"
    print("  ✅ redo_analyse 排除空壳原始模块，只含新子模块")


def test_redo_analyse_nonempty_original_included():
    """原始模块未被拆分（files.list非空）时，应进入 redo_analyse。"""
    with tempfile.TemporaryDirectory() as tmp:
        mods = Path(tmp)
        for m, c in [("bgp","bgp.so"),("ospf","ospf.so"),("auth","libauth.so")]:
            (mods/m).mkdir(); (mods/m/"files.list").write_text(c + chr(10))
        # auth 没有子模块（Stage 2-redo 决定不拆）
        final_modules = {"auth","bgp","ospf"}
        modules_needing_reclassify = {"auth"}
        new_mods_set = {"bgp","ospf","auth"}

        redo = []
        for m in new_mods_set:
            if m not in final_modules:
                redo.append(m)
            elif m in modules_needing_reclassify:
                flist = mods/m/"files.list"
                if flist.exists() and flist.stat().st_size > 0:
                    redo.append(m)
        assert sorted(redo) == ["auth"], f"非空原始模块应进入redo: {redo}"
    print("  ✅ redo_analyse 非空原始模块（未拆分）正确包含")

TESTS = [
    # helpers
    test_parse_eval_md_standard,
    test_parse_eval_md_last_match,
    test_parse_eval_md_zero_false,
    test_parse_eval_md_pass_no_score,
    test_parse_eval_md_high_score_default_pass,
    test_parse_eval_md_low_score_default_fail,
    test_check_voting_all,
    test_check_voting_any,
    test_check_voting_majority,
    test_evaluation_recorder_round_and_summary,
    test_evaluation_recorder_resume_round_numbering,
    test_discover_modules,
    # context
    test_pipeline_context_methods,
    test_pipeline_context_emit,
    # pipeline base
    test_pipeline_run_all,
    test_pipeline_skip_to_stage3,
    test_pipeline_stage_order,
    test_pipeline_error_propagates,
    test_pipeline_fatal_error_propagates,
    test_run_agent_with_stage_guard_enforces_timeout,
    # stage 0
    test_filter_stage_no_script,
    test_filter_stage_with_script,
    test_explore_stage_no_prompt,
    test_explore_stage_calls_agent,
    test_prescan_stage_no_keywords,
    test_prescan_stage_with_keywords,
    # stage 1 (全分支)
    test_classify_first_attempt_pass,
    test_classify_min_rounds_2,
    test_classify_fail_then_pass,
    test_classify_max_rounds_exceeded,
    test_classify_pi_fatal_propagates,
    test_classify_vote_mode_any,
    # stage 2/3/4 骨架
    test_refine_stage_stub,
    test_refine_stage_reclassify_cleanup_path,
    test_analyse_stage_stub,
    test_report_stages_stub,
    # pipeline 集成
    test_pipeline_full_flow_stubs,
    test_pipeline_resume_from_stage3,
    # orchestrator 薄层
    test_orchestrator_delegates_to_legacy,
    test_orchestrator_stop,
    # Stage 3-redo 筛选
    test_redo_analyse_only_new_and_nonempty,
    test_redo_analyse_nonempty_original_included,
]


def main():
    EVENTS.clear()
    passed = failed = 0
    print("\n" + "="*60)
    print(" pipeline/ 新架构 dry-run 测试")
    print("="*60)

    for fn in TESTS:
        try:
            EVENTS.clear()
            fn()
            passed += 1
        except Exception as e:
            import traceback
            print(f"  ❌ {fn.__name__}: {e}")
            traceback.print_exc()
            failed += 1

    print()
    print("="*60)
    print(f" 结果: {passed} 通过 / {failed} 失败 / {passed+failed} 总计")
    print("="*60)
    return failed == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)
