import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.models import (
    AgentInstanceConfig,
    RoleConfig,
    StageLoopConfig,
    StagesConfig,
    SwarmEvent,
    TaskConfig,
    TokenUsage,
)
from app.pipeline import (
    BaseStage,
    Pipeline,
    PipelineContext,
    StageError,
    check_voting,
    discover_modules,
    get_modules_root,
    parse_eval_md,
)
from app.pipeline.helpers import run_agent_with_stage_guard
from app.runner import AgentResult


def _make_ctx(tmp: str) -> PipelineContext:
    cfg = TaskConfig(
        task="test",
        target_dir=str(Path(tmp) / "target"),
        analyse_targets=["binary"],
        binary_arch=["all"],
        agent_max_retries=1,
        agent_retry_delay=0,
        pi_max_retries=1,
        pi_retry_delay=0,
        stages=StagesConfig(
            classify=StageLoopConfig(min_rounds=1, max_rounds=2, pass_mode="all"),
            refine=StageLoopConfig(min_rounds=1, max_rounds=2, pass_mode="all"),
            analyse=StageLoopConfig(min_rounds=1, max_rounds=2, pass_mode="all"),
            final_check=StageLoopConfig(min_rounds=1, max_rounds=2, pass_mode="all"),
        ),
        workers=RoleConfig(
            default_tools=["read"],
            system_prompt_dir=str(Path(tmp) / "prompts" / "workers"),
            agents=[AgentInstanceConfig(model="vllm/glm5")],
        ),
        judges=RoleConfig(
            default_tools=["read"],
            system_prompt_dir=str(Path(tmp) / "prompts" / "judges"),
            agents=[AgentInstanceConfig(model="vllm/glm5")],
        ),
        output_dir=str(Path(tmp) / "output"),
        archive_dir=str(Path(tmp) / "output"),
        result_dir=str(Path(tmp) / "result"),
    )
    workspace = Path(tmp) / "workspace"
    output_dir = Path(tmp) / "output"
    sess_dir = output_dir / "sessions"
    Path(tmp, "target").mkdir(parents=True, exist_ok=True)
    sess_dir.mkdir(parents=True, exist_ok=True)
    events: list[SwarmEvent] = []
    return PipelineContext(
        task_id="task-test",
        task="test",
        cfg=cfg,
        workspace=workspace,
        output_dir=output_dir,
        sess_dir=sess_dir,
        emit=events.append,
        tokens=TokenUsage(),
    )


def _make_agent_result(output: str = "", error: str = "") -> AgentResult:
    ar = AgentResult()
    ar.output = output
    ar.error = error
    ar.token_usage = MagicMock(prompt_tokens=10, completion_tokens=20)
    return ar


def test_parse_eval_md_current_semantics():
    assert parse_eval_md("## 评分: 85\n## 通过: 是") == {
        "score": 85,
        "pass": True,
        "feedback": "## 评分: 85\n## 通过: 是",
    }
    assert parse_eval_md("## 评分: 0\n## 通过: 否")["pass"] is False
    assert parse_eval_md("## 评分: 80")["pass"] is True
    assert parse_eval_md("## 评分: 60")["pass"] is False
    assert parse_eval_md("分类合理，文件完整，检查通过，没有问题。")["pass"] is False
    assert parse_eval_md("## 评分: 0\n## 通过: 是")["feedback"].startswith("Judge 格式违规")


def test_check_voting_modes():
    assert check_voting([{"pass": True}, {"pass": True}], "all", 2) is True
    assert check_voting([{"pass": True}, {"pass": False}], "all", 2) is False
    assert check_voting([{"pass": False}, {"pass": True}], "any", 2) is True
    assert check_voting([{"pass": False}, {"pass": False}], "any", 2) is False
    assert check_voting([{"pass": True}, {"pass": True}, {"pass": False}], "majority", 3) is True
    assert check_voting([{"pass": False}, {"pass": True}, {"pass": False}], "majority", 3) is False


def test_discover_modules_flattens_nested_children():
    with tempfile.TemporaryDirectory() as tmp:
        workspace = Path(tmp)
        root = get_modules_root(workspace)
        parent = root / "parent"
        nested = parent / "child_a"
        nested.mkdir(parents=True)
        (nested / "files.list").write_text("a.c\n", encoding="utf-8")
        child_b = root / "child_b"
        child_b.mkdir()
        (child_b / "files.list").write_text("b.c\n", encoding="utf-8")

        modules = discover_modules(workspace)

        assert modules == ["child_a", "child_b"]
        assert (root / "child_a" / "files.list").exists()


class _Stage(BaseStage):
    def __init__(self, num: int, name: str, marker: list[str], *, filter_count: int | None = None):
        self._num = num
        self._name = name
        self._marker = marker
        self._filter_count = filter_count

    @property
    def stage_num(self) -> int:
        return self._num

    @property
    def stage_name(self) -> str:
        return self._name

    def execute(self, ctx: PipelineContext) -> None:
        self._marker.append(self._name)
        if self._filter_count is not None:
            ctx.filter_count = self._filter_count


def test_pipeline_runs_sync_stages_in_order():
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _make_ctx(tmp)
        seen: list[str] = []
        pipeline = Pipeline([
            _Stage(2, "refine", seen),
            _Stage(0, "filter", seen),
            _Stage(1, "classify", seen),
        ])

        result = pipeline.run(ctx)

        assert result is ctx
        assert seen == ["filter", "classify", "refine"]


def test_pipeline_stops_after_empty_filter_stage():
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _make_ctx(tmp)
        seen: list[str] = []
        pipeline = Pipeline([
            _Stage(0, "文件过滤", seen, filter_count=0),
            _Stage(1, "分类", seen),
        ])

        pipeline.run(ctx)

        assert seen == ["文件过滤"]


def test_run_agent_with_stage_guard_emits_retry_event_once():
    with tempfile.TemporaryDirectory() as tmp:
        ctx = _make_ctx(tmp)
        events: list[SwarmEvent] = []
        ctx.emit = events.append
        ar = _make_agent_result(output="retry")
        ar.rate_limit_event_due = True
        ar.retry_delay_seconds = 30
        ar.consecutive_rate_limit_count = 2

        with patch("app.pipeline.helpers.run_agent_checked", return_value=ar):
            result = run_agent_with_stage_guard(
                ctx=ctx,
                stage="analyse",
                context="rate-limit",
                prompt="hello",
                model="test-model",
                tools=["read"],
                cwd=tmp,
                session_file=str(Path(tmp) / "sess.jsonl"),
            )

        assert result is ar
        assert [ev.type for ev in events] == ["task_rate_limited_retrying"]


def test_pipeline_stage_error_propagates():
    class _FailStage(_Stage):
        def execute(self, ctx: PipelineContext) -> None:
            raise StageError("boom")

    with tempfile.TemporaryDirectory() as tmp:
        ctx = _make_ctx(tmp)
        pipeline = Pipeline([_FailStage(0, "filter", [])])

        try:
            pipeline.run(ctx)
            assert False, "expected StageError"
        except StageError as exc:
            assert str(exc) == "boom"
