"""
pipeline/s2_refine.py — Stage 2: 模块细分 v3

架构变更:
  - 快照位置: modules/<mod>/.snapshot（每模块自包含）
  - LLM 并行处理，Python 串行提交（commit_queue）
  - merge 到 LLM 处理中的模块时，直接追加 .snapshot + files.list
  - 重试不回滚 Worker 产物，Worker 自己根据 Judge 反馈修改
  - Python _validate_module() 替代 check_module.sh

入: workspace/modules/*/files.list
出: workspace/modules/*/ (拆分/合并后)
    workspace/deleted.list
    ctx.refined_modules
"""
from __future__ import annotations

import subprocess
import shutil
import threading
import queue
import time
from pathlib import Path

from app.copy_utils import safe_copy2
from .base import BaseStage
from .context import PipelineContext
from .evaluation import utc_now_iso
from .helpers import (
    run_agent_with_stage_guard, parse_eval_md, check_voting,
    discover_modules, get_modules_root, load_prompt, load_granularity_prompt, build_granularity_hint,
    archive_file, max_iter, write_judge_feedback,
    SUB_WORKER_THRESHOLD, collect_file_summaries,
    load_details_for_module,
    StageError, PiFatalError, max_rounds_exceeded_treated_as_passed,
    enforce_filter_constraint,
    module_has_nonempty_files, split_plan_exists, list_split_candidate_modules,
    get_module_deleted_files, process_module_recover,
    read_module_files, read_split_merge_targets,
)


# ── Python 侧校验 ──────────────────────────────────────────────────────────

def _read_lines(path: Path) -> set[str]:
    if not path.exists():
        return set()
    if path.is_dir():
        return set()
    return {ln.strip() for ln in path.read_text("utf-8", errors="replace").splitlines() if ln.strip()}


def _write_lines(path: Path, lines: set[str]) -> None:
    """Write lines to a file. Creates parent directories if needed.

    .snapshot is always a flat file — never a directory.
    If path is a directory (corrupted state), it is deleted first.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.is_dir():
        shutil.rmtree(str(path), ignore_errors=True)
    path.write_text("\n".join(sorted(lines)) + "\n", encoding="utf-8")


def _snapshot_file_path(mod_dir: Path) -> Path:
    """Return .snapshot path — always a flat file in the module directory."""
    return mod_dir / ".snapshot"


def _create_snapshot_file(mod_dir: Path) -> Path:
    """Ensure .snapshot exists as a flat file.

    Only called once at S2 module entry (before any LLM work).
    - If .snapshot is a valid file → keep it
    - If .snapshot is a directory (corrupted from previous run) → delete & recreate
    - If .snapshot is missing → create from files.list
    """
    snapshot = mod_dir / ".snapshot"

    if snapshot.exists():
        if snapshot.is_file():
            return snapshot
        # Corrupted: directory form (previous-run residue, never created by current code)
        if snapshot.is_dir():
            shutil.rmtree(str(snapshot), ignore_errors=True)
            # If rmtree failed (e.g. read-only NFS), .snapshot is still a directory.
            # We must NOT call safe_copy2 on it — shutil.copy2 would copy INTO the
            # directory instead of overwriting, causing .snapshot/files.list EROFS.
            if snapshot.exists():
                raise StageError(
                    f"模块 {mod_dir.name} 的 .snapshot 是目录且无法删除"
                    f"（可能是只读文件系统残留），请使用 restart 重建任务"
                )

    # Create from current files.list
    files_list = mod_dir / "files.list"
    if files_list.exists() and files_list.is_file():
        safe_copy2(str(files_list), str(snapshot))
    return snapshot


def _validate_module(mod_dir: Path) -> dict:
    """
    校验单模块:
      .snapshot 文件集合 == files.list ∪ split/*/files.list ∪ split/_merge_to/*/files.list ∪ deleted/files.list
    返回 {pass: bool, missing: [...], extra: [...]} 供 Judge 参考，不抛异常。
    """
    mod_name = mod_dir.name
    snapshot = _read_lines(_snapshot_file_path(mod_dir))

    if not snapshot:
        return {"pass": False, "missing": ["NO_SNAPSHOT"], "extra": [],
                "snap_count": 0, "covered_count": 0, "mod_name": mod_name}

    kept = _read_lines(mod_dir / "files.list")
    deleted = _read_lines(mod_dir / "deleted" / "files.list")
    split_files: set[str] = set()
    for child_dir in sorted((mod_dir / "split").iterdir()) if (mod_dir / "split").exists() else []:
        if child_dir.name.startswith("_"):
            for sub in (child_dir / "files.list" for _ in [1]):
                continue
        fl = child_dir / "files.list" if child_dir.is_dir() else None
        if fl and fl.exists():
            split_files |= _read_lines(fl)

    merge_root = mod_dir / "split" / "_merge_to"
    if merge_root.exists():
        for d in sorted(merge_root.iterdir()):
            if d.is_dir():
                fl = d / "files.list"
                if fl.exists():
                    split_files |= _read_lines(fl)

    covered = kept | split_files | deleted
    missing = sorted(snapshot - covered)
    extra = sorted(covered - snapshot)

    return {
        "pass": len(missing) == 0 and len(extra) == 0,
        "missing": missing[:30],
        "extra": extra[:30],
        "snap_count": len(snapshot),
        "covered_count": len(covered),
        "mod_name": mod_name,
    }


# ── 提交逻辑 ────────────────────────────────────────────────────────────────

def _commit_one_module(mod_dir: Path, workspace: Path, in_progress: set[str]) -> tuple[dict, set[str]]:
    """
    提交单个模块的 split/merge/deleted。
    返回 (commit_info, merge_targets_in_progress):
      commit_info: {"applied", "new_modules", "merged_targets", "retained_parent"}
      merge_targets_in_progress: 在 LLM 处理中被合并到的目标模块名集合

    处理三种 split 情况:
      1. 全拆: mod → child1, child2 (原模块消失)
      2. 部分拆: mod → child + mod' (原模块保留部分文件)
      3. 合并: mod 部分文件 → _merge_to/target
    """
    mods_root = workspace / "modules"
    mod_name = mod_dir.name
    snapshot = _read_lines(_snapshot_file_path(mod_dir))
    deleted_set = _read_lines(mod_dir / "deleted" / "files.list")

    # ── 读取 Worker 产物 ──
    child_map: dict[str, set[str]] = {}
    retained_parent = False
    for child in list_split_candidate_modules(mod_dir):
        files = _read_lines(mod_dir / "split" / child / "files.list")
        if files:
            child_map[child] = files
            if child == mod_name:
                retained_parent = True

    merge_map: dict[str, set[str]] = {}
    merge_root = mod_dir / "split" / "_merge_to"
    if merge_root.exists():
        for d in sorted(merge_root.iterdir()):
            if d.is_dir():
                files = _read_lines(d / "files.list")
                if files:
                    merge_map[d.name] = files

    kept = _read_lines(mod_dir / "files.list")

    # ── 完整性校验 ──
    covered = set().union(*child_map.values()) if child_map else set()
    covered |= set().union(*merge_map.values()) if merge_map else set()
    covered |= deleted_set
    covered |= kept
    if snapshot and covered != snapshot:
        missing = snapshot - covered
        # ★ 允许已在其他模块中的文件隐式通过（redo 时子模块来源于上轮拆分）
        mods_root = workspace / "modules"
        truly_missing: set[str] = set()
        for f in missing:
            found = False
            for other_fl in mods_root.glob("*/files.list"):
                if other_fl.parent.name == mod_name:
                    continue
                if f in (other_fl.read_text("utf-8", errors="replace") or ""):
                    found = True
                    break
            if not found and f in (workspace / "deleted.list").read_text(encoding="utf-8", errors="replace"):
                found = True
            if not found:
                truly_missing.add(f)
        extra = covered - snapshot
        if truly_missing or extra:
            raise StageError(
                f"提交前校验失败: {mod_name} missing={len(truly_missing)} extra={len(extra)}"
                + (f" missing示例={sorted(truly_missing)[:5]}" if truly_missing else "")
                + (f" extra示例={sorted(extra)[:5]}" if extra else "")
            )

    # ── 执行提交 ──
    new_modules: list[str] = []
    merged_targets: list[str] = []
    merge_targets_in_progress: set[str] = set()

    for child, files in child_map.items():
        if child == mod_name:
            _write_lines(mod_dir / "files.list", files)
        else:
            target_dir = mods_root / child
            existing = _read_lines(target_dir / "files.list")
            _write_lines(target_dir / "files.list", existing | files)
            new_modules.append(child)
            # 如果目标在 LLM 处理中，追加到其 .snapshot
            if child in in_progress:
                snap = _create_snapshot_file(target_dir)
                if snap.exists() and snap.is_file():
                    snap_set = _read_lines(snap)
                    _write_lines(snap, snap_set | files)

    for target, files in merge_map.items():
        target_dir = mods_root / target
        existing = _read_lines(target_dir / "files.list")
        _write_lines(target_dir / "files.list", existing | files)
        merged_targets.append(target)
        # 如果目标在 LLM 处理中，追加到其 .snapshot + 记录待处理
        if target in in_progress:
            snap = _create_snapshot_file(target_dir)
            if snap.exists() and snap.is_file():
                snap_set = _read_lines(snap)
                _write_lines(snap, snap_set | files)
            merge_targets_in_progress.add(target)

    # 处理原模块
    kept_parent_files = False
    if mod_name not in child_map:
        kept_lines = _read_lines(mod_dir / "files.list")
        # ★ 从 kept 中移除已拆出到子模块的文件 + 已合并到其他模块的文件
        split_out = set().union(*child_map.values()) if child_map else set()
        merge_out = set().union(*merge_map.values()) if merge_map else set()
        kept_lines -= split_out
        kept_lines -= merge_out
        if kept_lines:
            _write_lines(mod_dir / "files.list", kept_lines)
            kept_parent_files = True
        elif deleted_set:
            (mod_dir / "files.list").unlink(missing_ok=True)
        else:
            shutil.rmtree(str(mod_dir), ignore_errors=True)
    else:
        _write_lines(mod_dir / "files.list", child_map[mod_name])

    # 追加 deleted
    if deleted_set:
        with open(str(workspace / "deleted.list"), "a", encoding="utf-8") as f:
            for fp in sorted(deleted_set):
                f.write(fp + "\n")

    # 清理
    for p in [".snapshot", "split", "deleted"]:
        path = mod_dir / p
        if path.is_file():
            path.unlink(missing_ok=True)
        elif path.is_dir():
            shutil.rmtree(str(path), ignore_errors=True)

    return ({
        "applied": True,
        "new_modules": sorted(set(new_modules)),
        "merged_targets": sorted(set(merged_targets)),
        "retained_parent": retained_parent or kept_parent_files,
    }, merge_targets_in_progress)


# ── Stage ────────────────────────────────────────────────────────────────────

class RefineStage(BaseStage):
    stage_num = 2
    stage_name = "细分"

    def __init__(self):
        super().__init__()
        self._commit_children: set[str] = set()  # commit 产生的新子模块名

    def _reset(self) -> None:
        self._refined: set[str] = set()
        self._in_progress: set[str] = set()
        self._pending_merge_targets: set[str] = set()
        self._commit_children: set[str] = set()  # commit 产生的新子模块
        self._errors: list[BaseException] = []
        self._queue: queue.Queue = queue.Queue()
        self._commit_queue: queue.Queue = queue.Queue()
        self._ctx: PipelineContext | None = None

    @staticmethod
    def _module_refine_artifacts_valid(workspace: Path, mod_name: str) -> bool:
        mod_dir = get_modules_root(str(workspace)) / mod_name
        fl = mod_dir / "files.list"
        if not fl.exists():
            return False
        try:
            if not any(ln.strip() for ln in fl.read_text("utf-8", errors="replace").splitlines()):
                return False
        except Exception:
            import traceback
            traceback.print_exc()
            return False
        return not (mod_dir / ".snapshot").exists()

    def execute(self, ctx: PipelineContext) -> None:
        cp = ctx.checkpoint
        self._reset()
        self._ctx = ctx
        cfg = ctx.cfg
        workspace = ctx.workspace

        if cp and cp.is_done("s2_refine"):
            ctx.refined_modules = discover_modules(str(workspace))
            ctx.emit_event("log", level="info",
                           msg=f"[S2] checkpoint 已完成，跳过({len(ctx.refined_modules)}个模块)")
            return

        all_modules = discover_modules(str(workspace))
        for mod in all_modules:
            if mod not in self._refined:
                self._queue.put(mod)

        parallel = max(1, cfg.parallel_modules)

        # 启动 LLM workers (并行)
        llm_workers = [threading.Thread(target=self._llm_worker, daemon=True) for _ in range(parallel)]
        for w in llm_workers:
            w.start()

        # 启动 commit worker (串行)
        commit_thread = threading.Thread(target=self._commit_worker, daemon=True)
        commit_thread.start()

        # 等待 LLM 全部完成
        self._queue.join()

        # 停止 LLM workers (send sentinel values to daemon threads)
        for _ in llm_workers:
            self._queue.put(None)
        for w in llm_workers:
            w.join()

        # 标记 commit 队列结束
        self._commit_queue.put(None)

        # 等待 commit 完成
        commit_thread.join()

        if self._errors:
            for e in self._errors:
                if isinstance(e, PiFatalError):
                    raise e
            raise self._errors[0]

        # 全局完整性检查
        if not (cp and cp.is_done("s2_global_check")):
            self._global_completeness_check()
            if cp:
                cp.mark_done("s2_global_check")
        else:
            ctx.emit_event("log", level="info", msg="[S2] 全局检查 checkpoint 已完成，跳过")

        ctx.refined_modules = discover_modules(str(workspace))
        if cp:
            cp.mark_done("s2_refine", module_count=len(ctx.refined_modules))

    # ── LLM Worker (并行) ──────────────────────────────────────────────────
    def _llm_worker(self) -> None:
        while True:
            mod_name = self._queue.get()
            if mod_name is None:  # sentinel to stop
                self._queue.task_done()
                return
            self._in_progress.add(mod_name)
            try:
                if mod_name not in self._refined:
                    self._refine_one(mod_name)
            except PiFatalError as e:
                self._errors.append(e)
            except StageError as e:
                ctx = self._ctx
                if ctx and ctx.continue_on_module_failure:
                    ctx.record_soft_module_failure(
                        stage="refine",
                        module_name=mod_name,
                        error=str(e),
                        artifact_paths=[str(ctx.module_dir(mod_name) / "files.list")],
                        extra={"soft_failed": True},
                        record_round="已达最大轮数" not in str(e),
                    )
                else:
                    self._errors.append(e)
            except Exception as e:
                ctx = self._ctx
                if ctx is not None:
                    try:
                        import traceback as _tb
                        ctx.record_module_program_error(
                            stage="refine",
                            module_name=mod_name,
                            error_type=type(e).__name__,
                            error_message=f"{e}",
                            traceback_text=_tb.format_exc(),
                        )
                    except Exception:
                        import traceback
                        traceback.print_exc()
                        pass
                fatal = PiFatalError(f"S2 refine {mod_name}: {type(e).__name__}: {e}")
                fatal.fatal = True
                self._errors.append(fatal)
            finally:
                # LLM 完成后，如果被 merge 过 → 也要入 commit 队列
                if mod_name in self._pending_merge_targets:
                    mod_dir = get_modules_root(str(self._ctx.workspace)) / mod_name
                    self._commit_queue.put((mod_dir, False, []))
                    self._pending_merge_targets.discard(mod_name)
                self._in_progress.discard(mod_name)
                self._queue.task_done()

    # ── Commit Worker (串行) ───────────────────────────────────────────────
    def _commit_worker(self) -> None:
        while True:
            item = self._commit_queue.get()
            if item is None:
                self._commit_queue.task_done()
                break
            mod_dir, was_split, new_ones = item
            mod_name = mod_dir.name
            ctx = self._ctx
            workspace = ctx.workspace
            try:
                commit_info, merge_in_progress = _commit_one_module(mod_dir, workspace, self._in_progress)
                new_ones = list(commit_info.get("new_modules") or [])
                for nm in new_ones:
                    self._commit_children.add(nm)  # ★ 记录给 Orchestrator
                # 记录被 merge 的 LLM 处理中模块（等 LLM 结束后入队列）
                for target in merge_in_progress:
                    self._pending_merge_targets.add(target)

                if mod_dir.exists() and not (mod_dir / "files.list").exists():
                    shutil.rmtree(str(mod_dir), ignore_errors=True)

                # 新子模块入 LLM 队列（去重）
                for nm in new_ones:
                    if nm not in self._refined and nm not in self._in_progress:
                        self._in_progress.add(nm)
                        self._queue.put(nm)

                self._refined.add(mod_name)
                cp = ctx.checkpoint if ctx else None
                if cp:
                    cp.mark_done(f"s2_modules/{mod_name}",
                                 split=was_split, new_modules=new_ones)
            except Exception as exc:
                ctx.emit_event("log", level="error",
                               msg=f"[S2] 提交失败 {mod_name}: {exc}")
                self._errors.append(exc)
            finally:
                self._commit_queue.task_done()

    # ── 单模块 LLM 处理 ────────────────────────────────────────────────────
    def _refine_one(self, mod_name: str) -> None:
        ctx = self._ctx
        cp = ctx.checkpoint
        cfg = ctx.cfg
        workspace = ctx.workspace
        s_cfg = cfg.stages.refine
        w_base = ctx.make_w_base()
        j_base = ctx.make_j_base()

        mod_dir = get_modules_root(str(workspace)) / mod_name
        if not (mod_dir / "files.list").exists():
            return

        fc = sum(1 for _ in (mod_dir / "files.list").read_text("utf-8", errors="replace").splitlines() if _.strip())
        if fc == 0:
            ctx.emit_event("log", level="warn", msg=f"[跳过] {mod_name} 0 文件，移除空模块")
            shutil.rmtree(str(mod_dir), ignore_errors=True)
            return

        # checkpoint skip
        if cp and cp.is_done(f"s2_modules/{mod_name}"):
            if not self._module_refine_artifacts_valid(workspace, mod_name):
                cp.clear(f"s2_modules/{mod_name}")
            else:
                self._refined.add(mod_name)
                return

        refine_session = ctx.session_path("refine", f"{mod_name}.jsonl")

        # ── 文件摘要 ──
        files_list = [l.strip() for l in (mod_dir / "files.list").read_text("utf-8", errors="replace").splitlines() if l.strip()]
        sub_prompt = load_prompt(cfg, "step2_sub_read", "workers")
        file_summary = ""
        details_dir = ctx.details_dir

        if details_dir.exists():
            summary_from_details, unclear_files = load_details_for_module(details_dir, files_list, cfg.target_dir)
            ctx.emit_event("log", level="info",
                           msg=f"[S2] {mod_name}: {fc}个文件, {len(unclear_files)}个需LLM补充")
            if unclear_files and sub_prompt:
                supplement = collect_file_summaries(
                    ctx=ctx, mod_name=mod_name, mod_dir=mod_dir,
                    sub_prompt_template=sub_prompt, parallel=cfg.parallel_sub_workers,
                    sub_model=cfg.workers.model_for("sub_read"), target_dir=cfg.target_dir,
                    files_override=unclear_files,
                )
                file_summary = (summary_from_details + "\n" + supplement) if summary_from_details else supplement
            else:
                file_summary = summary_from_details
        elif sub_prompt and fc > SUB_WORKER_THRESHOLD:
            file_summary = collect_file_summaries(
                ctx=ctx, mod_name=mod_name, mod_dir=mod_dir,
                sub_prompt_template=sub_prompt, parallel=cfg.parallel_sub_workers,
                sub_model=cfg.workers.model_for("sub_read"), target_dir=cfg.target_dir,
            )

        granularity = getattr(cfg, "module_granularity", "fine") or "fine"
        w_sys_prompt = load_granularity_prompt(cfg, "step2_refine", granularity, "workers")
        j_sys_prompt = load_granularity_prompt(cfg, "step2_check_refine", granularity, "judges")
        reflect_prompt = load_granularity_prompt(cfg, "reflect_refine", granularity, "workers")

        _gran_hint = build_granularity_hint(granularity)
        if _gran_hint and _gran_hint not in w_sys_prompt:
            w_sys_prompt += _gran_hint
        if _gran_hint and _gran_hint not in j_sys_prompt:
            j_sys_prompt += _gran_hint

        # ── 初始化 .snapshot（W+J 需要参考原始文件清单）──
        _create_snapshot_file(mod_dir)

        feedback = ""
        for attempt in range(max_iter(s_cfg)):
            round_started = utc_now_iso()
            round_start_ts = time.time()

            # ★ 不再 restore_module_for_retry — Worker 自己改 split/deleted

            ctx.emit_event("stage", stage=2, module=mod_name, attempt=attempt + 1)

            prompt_parts = [
                f"当前正式已存在模块: {', '.join(sorted(discover_modules(str(workspace))))}",
                f"检查模块 `{mod_name}` 是否需要细分。",
                f"如需拆分 → `modules/{mod_name}/split/<child>/files.list`",
                f"如需合并 → `modules/{mod_name}/split/_merge_to/<target>/files.list`",
                f"如需排除 → `modules/{mod_name}/deleted/files.list`",
                f"Judge 通过后 Python 自动提交。",
            ]
            if file_summary:
                prompt_parts.append("\n\n## 文件摘要\n\n" + file_summary)
            if feedback:
                prompt_parts.append("\n\n" + feedback)

            ar = run_agent_with_stage_guard(
                ctx=ctx, stage="refine",
                context=f"s2-refine-{mod_name}-a{attempt+1}",
                heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, session=refine_session: {
                    "module": module, "attempt": attempt_no, "heartbeat": beat, "session_file": session,
                },
                prompt="\n".join(prompt_parts),
                model=ctx.wm("refine"), system_prompt=w_sys_prompt,
                session_file=refine_session, **w_base,
            )
            ctx.tokens += ar.token_usage

            split_new_modules = list_split_candidate_modules(mod_dir)
            new_ones = [nm for nm in split_new_modules if nm != mod_name]
            was_split = split_plan_exists(mod_dir)
            ctx.emit_event("stage_result", stage=2, module=mod_name, split=was_split, new_modules=new_ones)

            # ── Python 校验（替代 check_module.sh）──
            _create_snapshot_file(mod_dir)
            py_validation = _validate_module(mod_dir)

            # ── Judge ──
            del_files = get_module_deleted_files(mod_dir)
            deleted_summary = ""
            if del_files:
                preview = sorted(del_files)[:30]
                more = f"\n  ...(共 {len(del_files)} 个)" if len(del_files) > 30 else ""
                deleted_summary = (
                    f"\n\n## 本轮提议排除文件（modules/{mod_name}/deleted/files.list）"
                    f"\n共 {len(del_files)} 个" + "".join(f"\n  - {f}" for f in preview) + more
                )

            judge_results = []
            judge_records = []
            for j_idx, j_item in enumerate(ctx.j_cfgs):
                j_model = ctx.jm("refine", j_item)
                judge_session = ctx.session_path("judges", "refine", mod_name,
                                                 f"refine-a{attempt + 1}-j{j_idx}.jsonl")
                # 注入 Python 校验结果
                judge_prompt = (
                    f"评审 Worker 对模块 `{mod_name}` 的细分判断。"
                    f"{deleted_summary}\n\n"
                    f"## Python 侧校验结果\n"
                    f"  snapshot={py_validation['snap_count']} files, "
                    f"covered={py_validation['covered_count']}, "
                    f"missing={len(py_validation.get('missing',[]))}, "
                    f"extra={len(py_validation.get('extra',[]))}\n"
                )
                if py_validation.get("missing"):
                    judge_prompt += f"  MISSING: {py_validation['missing'][:10]}\n"
                if py_validation.get("extra"):
                    judge_prompt += f"  EXTRA: {py_validation['extra'][:10]}\n"

                j_ar = run_agent_with_stage_guard(
                    ctx=ctx, stage="refine",
                    context=f"s2-judge-{mod_name}-j{j_idx}-a{attempt+1}",
                    heartbeat_payload_factory=lambda beat, module=mod_name, attempt_no=attempt + 1, judge_id=j_idx, session=judge_session: {
                        "module": module, "attempt": attempt_no, "heartbeat": beat,
                        "judge_id": f"judge-{judge_id}", "session_file": session,
                    },
                    prompt=judge_prompt, model=j_model,
                    system_prompt=j_sys_prompt, tools=cfg.judges.default_tools,
                    cwd=str(workspace), session_file=judge_session, **j_base,
                )
                ctx.tokens += j_ar.token_usage
                parsed = parse_eval_md(j_ar.output or "")
                judge_results.append(parsed)
                judge_records.append({
                    "judge_id": f"judge-{j_idx}", "model": j_model,
                    "score": parsed["score"], "passed": parsed["pass"],
                    "feedback": parsed["feedback"], "session_file": judge_session,
                    "token_usage": j_ar.token_usage,
                })
                ctx.emit_event("judge_eval", stage=2, judge_id=f"judge-{j_idx}",
                               module=mod_name, passed=parsed["pass"], score=parsed["score"])
                archive_file(ctx.output_dir, f"s2-{mod_name}-a{attempt+1}-j{j_idx}.md",
                             f"Score: {parsed['score']}\nPass: {parsed['pass']}\n\n"
                             f"{parsed['feedback']}\n\n---\n## Raw Output\n\n{j_ar.output[:3000]}")

            voted_pass = check_voting(judge_results, s_cfg.pass_mode, ctx.j_count)
            final_pass = voted_pass and attempt + 1 >= s_cfg.min_rounds
            max_reached = attempt + 1 >= max_iter(s_cfg)
            forced_pass = max_reached and max_rounds_exceeded_treated_as_passed(cfg)

            ctx.record_evaluation_round(
                module_name=mod_name, stage="refine", stage_round=attempt + 1,
                status=("passed" if (final_pass or forced_pass)
                        else "failed" if max_reached
                        else "needs_reflection" if voted_pass
                        else "needs_retry"),
                started_at=round_started, ended_at=utc_now_iso(),
                duration_ms=(time.time() - round_start_ts) * 1000,
                worker={"model": ctx.wm("refine"), "session_file": refine_session,
                        "token_usage": ar.token_usage, "error": ar.error},
                judges=judge_records, passed_by_vote=voted_pass,
                module_completed=False,
                completion_reason=("passed" if final_pass
                                   else "max_rounds_exceeded_treated_as_passed" if forced_pass
                                   else "max_rounds_exceeded" if max_reached else ""),
                needed_reflection=not final_pass,
                artifact_paths=[str(mod_dir / "files.list")],
                extra={"file_count": fc, "split": was_split, "new_modules": new_ones},
            )

            if final_pass or forced_pass:
                # ★ 有变动才入 commit 队列；无变动直接通过
                has_split = (mod_dir / "split").exists() and any((mod_dir / "split").iterdir())
                has_deleted = (mod_dir / "deleted").exists()
                if has_split or has_deleted:
                    self._commit_queue.put((mod_dir, was_split, new_ones))
                else:
                    # 无变动 — 但如果被其他模块 merge 过，仍需走 commit
                    # 已在 _llm_worker finally 中处理
                    self._refined.add(mod_name)
                    if cp:
                        cp.mark_done(f"s2_modules/{mod_name}", split=False, new_modules=[])
                return

            if voted_pass:
                ctx.emit_event("reflect", stage=2, module=mod_name, round=attempt + 1)
                feedback = (f"# 自查要求（第 {attempt+1} 轮，需至少 {s_cfg.min_rounds} 轮）\n\n"
                            + reflect_prompt)
                jfb = "\n".join(f"judge-{i}: {r['feedback']}"
                                for i, r in enumerate(judge_results))
                feedback += "\n\n## Judge 上轮意见\n\n" + jfb
            else:
                # Judge 不通过 — 不恢复 Worker 产物，让 Worker 自己改
                recovered = process_module_recover(mod_dir)
                if recovered:
                    ctx.emit_event("log", level="info",
                                   msg=f"[S2] {mod_name}: recovered {len(recovered)} files from deleted/")
                fb_rel = write_judge_feedback(workspace, "s2_refine", mod_name, attempt + 1, judge_results)
                ctx.emit_event("log", level="info", msg=f"[S2] judge 意见 → {fb_rel}")
                guidance = "\n\n请根据评审意见调整 split/merge/deleted 内容。"
                feedback = "请先阅读 judge 完整意见：\n" + f"```\nread {fb_rel}\n```\n" + guidance

            if forced_pass and not final_pass:
                has_split = (mod_dir / "split").exists() and any((mod_dir / "split").iterdir())
                has_deleted = (mod_dir / "deleted").exists()
                if has_split or has_deleted:
                    self._pending_merge_targets.discard(mod_name)
                    self._commit_queue.put((mod_dir, was_split, new_ones))
                else:
                    self._refined.add(mod_name)
                    if cp:
                        cp.mark_done(f"s2_modules/{mod_name}", split=False, new_modules=[])
                return

        raise StageError(f"Stage 2 模块 {mod_name} 细分未通过，已达最大轮数")

    # ── 全局完整性检查（保持不变）──────────────────────────────────────────
    def _global_completeness_check(self) -> None:
        ctx = self._ctx
        cfg = ctx.cfg
        workspace = ctx.workspace

        filtered_txt = workspace / "filtered_files.txt"
        if not filtered_txt.exists():
            return

        all_target = set(l.strip() for l in filtered_txt.read_text("utf-8").splitlines() if l.strip())
        confirmed_deleted = ctx.load_confirmed_deleted()
        if confirmed_deleted:
            all_target -= confirmed_deleted
            ctx.emit_event("log", level="info",
                           msg=f"[S2全局检查] 工作集: {len(all_target)} (已排除 {len(confirmed_deleted)} 个已确认排除)")

        mods_root = get_modules_root(str(workspace))
        all_classified: set[str] = set()
        for flist in mods_root.glob("*/files.list"):
            if flist.name == "files.list.snapshot":
                continue
            for l in flist.read_text("utf-8").splitlines():
                if l.strip():
                    all_classified.add(l.strip())
        missing_files = sorted(all_target - all_classified)

        if not missing_files:
            ctx.emit_event("log", level="info",
                           msg=f"Stage2 全局检查: 全部 {len(all_target)} 个文件已归类")
            return

        ctx.emit_event("log", level="warn",
                       msg=f"Stage2 全局检查: {len(missing_files)} 个文件未归类，启动补分类")

        mod_summary_lines = ["## 已有模块"]
        for flist in sorted(mods_root.glob("*/files.list")):
            mod_name = flist.parent.name
            sample = next((l.strip() for l in flist.read_text("utf-8").splitlines() if l.strip()), "(空)")
            mod_summary_lines.append(f"- {mod_name} | {Path(sample).name}")
        mod_summary = "\n".join(mod_summary_lines)

        reclass_prompt_tmpl = load_prompt(cfg, "step2_reclassify", "workers")
        max_rc = min(3, max_iter(cfg.stages.refine))
        reclassify_sessions_dir = ctx.sess_dir / "reclassify"
        reclassify_sessions_dir.mkdir(parents=True, exist_ok=True)

        w_base = ctx.make_w_base()
        reclass_prompt = f"## 待归类文件（{len(missing_files)} 个）\n\n" + "\n".join(missing_files) + f"\n\n{mod_summary}"

        for rc_attempt in range(max_rc):
            session_file = str(reclassify_sessions_dir / f"reclassify-a{rc_attempt + 1}.jsonl")
            ctx.emit_event("stage", stage="2-reclassify", attempt=rc_attempt + 1,
                           missing_count=len(missing_files), session_file=session_file)

            rc_ar = run_agent_with_stage_guard(
                ctx=ctx, stage="2-reclassify",
                heartbeat_payload_factory=lambda beat, attempt=rc_attempt + 1, count=len(missing_files): {
                    "attempt": attempt, "heartbeat": beat, "missing_count": count, "session_file": session_file,
                },
                context=f"s2-reclassify-a{rc_attempt+1}",
                prompt=reclass_prompt, model=ctx.wm("classify"),
                tools=w_base["tools"], system_prompt=reclass_prompt_tmpl,
                cwd=str(workspace), thinking_level=w_base.get("thinking_level", "off"),
                session_file=session_file, cancel_event=w_base.get("cancel_event"),
                max_retries=w_base.get("max_retries", 3), retry_delay=w_base.get("retry_delay", 10),
                pi_max_retries=w_base.get("pi_max_retries", -1), pi_retry_delay=w_base.get("pi_retry_delay", 10),
            )
            ctx.tokens += rc_ar.token_usage

            all_classified2: set[str] = set()
            for flist in mods_root.glob("*/files.list"):
                for l in flist.read_text("utf-8").splitlines():
                    if l.strip():
                        all_classified2.add(l.strip())
            still_missing = sorted(all_target - all_classified2 - ctx.load_confirmed_deleted())
            ctx.emit_event("stage_result", stage="2-reclassify", attempt=rc_attempt + 1,
                           status="completed", missing_count=len(still_missing))
            if not still_missing:
                break
            missing_files = still_missing
            reclass_prompt = f"## 仍未归类文件（{len(missing_files)} 个）\n\n" + "\n".join(missing_files) + f"\n\n{mod_summary}"

        if ctx.filtered_files:
            removed = enforce_filter_constraint(workspace, set(ctx.filtered_files))
            if removed:
                ctx.emit_event("log", level="warn",
                               msg=f"[S2过滤约束] 删除 {removed} 个超出 filtered_files.txt 的文件条目")

        # 补快照
        snap_dir = workspace / ".s2_snapshots"
        snap_dir.mkdir(exist_ok=True)
        snap_created = 0
        for flist in mods_root.glob("*/files.list"):
            mod_name = flist.parent.name
            if flist.stat().st_size > 0 and not (snap_dir / f"{mod_name}.snapshot").exists():
                safe_copy2(str(flist), str(snap_dir / f"{mod_name}.snapshot"))
                snap_created += 1
        if snap_created:
            ctx.emit_event("log", level="info", msg=f"[S2全局检查] 补创建 {snap_created} 个模块快照")
