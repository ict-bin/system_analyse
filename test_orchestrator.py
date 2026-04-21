"""
orchestrator dry-run 测试：用 mock 替换 run_agent，验证所有调度路径。

测试场景：
  1. 正常流程（Stage 0→1→2→3→4a→4b→完成）
  2. Stage 1 反思循环（min_rounds=2）
  3. Stage 2 模块拆分 + 新模块入队
  4. Stage 3 重分类触发 Stage 2-redo + 3-redo
  5. Stage 4a 缺失模块触发补做
  6. max_rounds 超限抛 StageError
  7. 致命错误直接终止
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

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.models import TaskConfig, StagesConfig, StageLoopConfig, RoleConfig, AgentInstanceConfig
from app.orchestrator import Orchestrator, StageError, PiFatalError, _parse_eval_md, _check_voting
from app.runner import AgentResult


# ─── Mock 工具 ────────────────────────────────────────────────────────────────

class CallTracker:
    """记录所有 run_agent 调用和 emit 事件。"""
    def __init__(self):
        self.calls: list[dict] = []
        self.events: list[dict] = []
        self.agent_responses: list[str] = []
        self._call_idx = 0

    def next_response(self) -> str:
        if self._call_idx < len(self.agent_responses):
            r = self.agent_responses[self._call_idx]
            self._call_idx += 1
            return r
        return "## 评分: 100\n## 通过: 是\n## 评审意见\n默认通过"

    def on_event(self, event):
        self.events.append({"type": event.type, **event.data})

    def get_stages(self) -> list[str]:
        return [e.get("stage", "") for e in self.events if e["type"] == "stage"]

    def get_judge_scores(self) -> list[int]:
        return [e.get("score", 0) for e in self.events if e["type"] == "judge_eval"]


def make_config(tmp_dir: str, min_rounds=1, max_rounds=-1) -> TaskConfig:
    """构造测试配置。"""
    return TaskConfig(
        task="测试任务",
        target_dir=os.path.join(tmp_dir, "target"),
        analyse_targets=["all"],
        agent_max_retries=1,
        agent_retry_delay=0.1,
        pi_max_retries=1,
        pi_retry_delay=0.1,
        stages=StagesConfig(
            classify=StageLoopConfig(min_rounds=min_rounds, max_rounds=max_rounds, pass_mode="all"),
            refine=StageLoopConfig(min_rounds=min_rounds, max_rounds=max_rounds, pass_mode="all"),
            analyse=StageLoopConfig(min_rounds=min_rounds, max_rounds=max_rounds, pass_mode="all"),
            final_check=StageLoopConfig(min_rounds=min_rounds, max_rounds=max_rounds, pass_mode="all"),
        ),
        workers=RoleConfig(
            default_tools=["read", "bash"],
            system_prompt_dir=os.path.join(tmp_dir, "prompts", "workers"),
            agents=[AgentInstanceConfig(model="test-model")],
        ),
        judges=RoleConfig(
            default_tools=["read", "bash"],
            system_prompt_dir=os.path.join(tmp_dir, "prompts", "judges"),
            agents=[AgentInstanceConfig(model="test-model")],
        ),
        output_dir=os.path.join(tmp_dir, "output"),
        archive_dir=os.path.join(tmp_dir, "output"),
        result_dir=os.path.join(tmp_dir, "result"),
    )


def setup_workspace(tmp_dir: str, modules: dict[str, list[str]] | None = None):
    """创建测试目录结构和 prompt 文件。"""
    target = Path(tmp_dir) / "target"
    target.mkdir(parents=True, exist_ok=True)
    # 创建一些测试文件
    for name in ["file1.so", "file2.ko", "file3.sh"]:
        (target / name).write_text(f"# {name}", encoding="utf-8")

    # prompts
    for sub in ["workers", "judges"]:
        d = Path(tmp_dir) / "prompts" / sub
        d.mkdir(parents=True, exist_ok=True)
    for name in ["step1_classify", "step1_explore", "step2_refine", "step2_sub_read",
                  "step3_analyse", "step4_final_report", "reflect_classify",
                  "reflect_refine", "reflect_analyse", "reflect_report"]:
        (Path(tmp_dir) / "prompts" / "workers" / f"{name}.md").write_text("test prompt", encoding="utf-8")
    for name in ["step1_check_classify", "step2_check_refine", "step3_check_analyse",
                  "step4_check_completeness", "step4_check_report"]:
        (Path(tmp_dir) / "prompts" / "judges" / f"{name}.md").write_text("test prompt", encoding="utf-8")

    # output/result
    (Path(tmp_dir) / "output").mkdir(exist_ok=True)
    (Path(tmp_dir) / "result").mkdir(exist_ok=True)


def create_mock_agent(tracker: CallTracker, workspace_effects=None):
    """创建 mock 的 run_agent，支持 workspace 副作用。"""
    call_count = [0]

    async def mock_run_agent(**kwargs):
        idx = call_count[0]
        call_count[0] += 1
        tracker.calls.append(kwargs)

        # 执行副作用（模拟 Worker 创建文件/目录）
        if workspace_effects and idx in workspace_effects:
            workspace_effects[idx](kwargs.get("cwd", ""))

        resp = tracker.next_response()
        ar = AgentResult()
        ar.output = resp
        ar.token_usage = MagicMock(prompt_tokens=10, completion_tokens=10)
        ar.error = None
        ar.fatal = False
        return ar

    return mock_run_agent


# ─── 测试 _parse_eval_md ─────────────────────────────────────────────────────

def test_parse_eval_md():
    """验证评分解析的各种边界情况。"""
    print("=== Test: _parse_eval_md ===")

    # 1. 标准格式
    r = _parse_eval_md("## 评分: 85\n## 通过: 是\n## 评审意见\n很好")
    assert r["score"] == 85 and r["pass"] == True, f"标准格式失败: {r}"

    # 2. 多次出现评分 → 取最后一次
    r = _parse_eval_md("评分: 30\n分析...\n## 评分: 0\n## 通过: 否\n## 评审意见\n失败")
    assert r["score"] == 0 and r["pass"] == False, f"多次评分应取最后: {r}"

    # 3. score=0 + 明确"通过:否" → 不走语义推断
    r = _parse_eval_md("文件完整、合理、正确、通过标准...\n## 评分: 0\n## 通过: 否")
    assert r["pass"] == False and r["score"] == 0, f"score=0+通过否 应返回fail: {r}"

    # 4. 只有正面词无评分 → 语义推断
    r = _parse_eval_md("分类合理，文件完整，检查通过，没有问题。")
    assert r["pass"] == True and r["score"] == 75, f"语义推断应pass: {r}"

    # 5. RESULT: FAIL
    r = _parse_eval_md("RESULT: FAIL\nMissing files: 5")
    assert r["pass"] == False, f"RESULT FAIL 应 not pass: {r}"

    # 6. 分数>=70 无明确通过 → 默认通过
    r = _parse_eval_md("## 评分: 80")
    assert r["pass"] == True and r["score"] == 80, f"score>=70 无通过标记: {r}"

    # 7. score=0 无明确"通过:否" → 走语义推断
    r = _parse_eval_md("不合理，不正确，遗漏严重")
    assert r["pass"] == False, f"纯负面词应 fail: {r}"

    print("  ✅ _parse_eval_md 全部通过")


# ─── 测试 _check_voting ──────────────────────────────────────────────────────

def test_check_voting():
    print("=== Test: _check_voting ===")

    # all 模式：全部通过
    assert _check_voting([{"pass": True}, {"pass": True}], "all", 2) == True
    assert _check_voting([{"pass": True}, {"pass": False}], "all", 2) == False

    # majority 模式：>50%
    assert _check_voting([{"pass": True}, {"pass": False}, {"pass": True}], "majority", 3) == True
    assert _check_voting([{"pass": True}, {"pass": False}, {"pass": False}], "majority", 3) == False

    # 边界：1 个 judge
    assert _check_voting([{"pass": True}], "all", 1) == True
    assert _check_voting([{"pass": False}], "majority", 1) == False

    print("  ✅ _check_voting 全部通过")


# ─── 测试 1: 正常完整流程 ─────────────────────────────────────────────────────

def test_normal_flow():
    print("=== Test: 正常完整流程 (min_rounds=1) ===")
    tmp_dir = tempfile.mkdtemp(prefix="orch_test_")

    try:
        setup_workspace(tmp_dir)
        cfg = make_config(tmp_dir, min_rounds=1)
        tracker = CallTracker()

        # 模拟工作流：
        # call 0: explore Worker
        # call 1: classify Worker (创建模块目录)
        # call 2: classify Judge
        # call 3: refine Worker (mod_a)
        # call 4: refine Judge (mod_a)
        # call 5: analyse Worker (mod_a)
        # call 6: analyse Judge (mod_a)
        # call 7: 4a completeness Judge
        # call 8: 4b report Worker
        # call 9: 4b report Judge

        def create_modules(cwd):
            ws = Path(cwd)
            for mod in ["mod_a"]:
                d = ws / mod
                d.mkdir(exist_ok=True)
                (d / "files.list").write_text("file1.so\n", encoding="utf-8")

        def create_report(cwd):
            ws = Path(cwd)
            for mod in ["mod_a"]:
                d = ws / mod
                if d.exists():
                    (d / "module_report.md").write_text(
                        "<!-- RISK_LEVEL: 中 -->\n<!-- RISK_SCORE: 50 -->\n# Report\n",
                        encoding="utf-8")

        def create_final(cwd):
            (Path(cwd) / "final_report.md").write_text("# Final Report", encoding="utf-8")

        effects = {
            0: lambda cwd: None,  # explore
            1: create_modules,    # classify
            5: create_report,     # analyse
            8: create_final,      # final report
        }

        tracker.agent_responses = [
            "<result>探索完成</result>",                                    # 0: explore
            "<result>分类完成: 1 模块</result>",                            # 1: classify Worker
            "## 评分: 100\n## 通过: 是\n## 评审意见\nPASS",                # 2: classify Judge
            "<result>无需细分</result>",                                    # 3: refine Worker
            "## 评分: 90\n## 通过: 是\n## 评审意见\n合理",                  # 4: refine Judge
            "<result>分析完成</result>",                                    # 5: analyse Worker
            "## 评分: 85\n## 通过: 是\n## 评审意见\nOK",                   # 6: analyse Judge
            "## 评分: 100\n## 通过: 是\n## 评审意见\n所有模块完成",          # 7: 4a Judge
            "<result>报告生成</result>",                                    # 8: 4b Worker
            "## 评分: 90\n## 通过: 是\n## 评审意见\n报告合格",              # 9: 4b Judge
        ]

        mock_agent = create_mock_agent(tracker, effects)

        with patch("app.orchestrator.run_agent", mock_agent), \
             patch("app.orchestrator.os.path.isfile", return_value=False):  # 跳过 filter/prescan 脚本
            orch = Orchestrator(cfg, on_event=tracker.on_event)
            result = asyncio.run(orch.execute("test-001"))

        assert result.status.value == "passed", f"应成功但状态为 {result.status.value}: {result.error}"

        stages = tracker.get_stages()
        assert "explore" in stages, f"缺少 explore 阶段: {stages}"
        assert 1 in stages, f"缺少 Stage 1: {stages}"
        assert 2 in stages, f"缺少 Stage 2: {stages}"
        assert 3 in stages, f"缺少 Stage 3: {stages}"
        assert "4a" in stages, f"缺少 Stage 4a: {stages}"
        assert "4b" in stages, f"缺少 Stage 4b: {stages}"

        # 验证 flag 文件
        flag = (Path(tmp_dir) / "result" / "flag").read_text()
        assert flag == "1", f"flag 应为 1: {flag}"

        # 验证 modules.list
        mlist = Path(tmp_dir) / "result" / "modules.list"
        assert mlist.exists(), "modules.list 不存在"
        content = mlist.read_text().strip()
        assert "mod_a" in content, f"modules.list 应含 mod_a: {content}"

        print(f"  ✅ 正常流程通过 (calls={len(tracker.calls)}, stages={stages})")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ─── 测试 2: min_rounds=2 反思循环 ───────────────────────────────────────────

def test_reflect_loop():
    print("=== Test: min_rounds=2 反思循环 ===")
    tmp_dir = tempfile.mkdtemp(prefix="orch_test_")

    try:
        setup_workspace(tmp_dir)
        cfg = make_config(tmp_dir, min_rounds=2)
        tracker = CallTracker()

        def create_modules(cwd):
            ws = Path(cwd)
            d = ws / "mod_a"
            d.mkdir(exist_ok=True)
            (d / "files.list").write_text("file1.so\n", encoding="utf-8")

        def create_report(cwd):
            ws = Path(cwd)
            d = ws / "mod_a"
            if d.exists():
                (d / "module_report.md").write_text(
                    "<!-- RISK_LEVEL: 低 -->\n<!-- RISK_SCORE: 20 -->\n# Report\n",
                    encoding="utf-8")

        def create_final(cwd):
            (Path(cwd) / "final_report.md").write_text("# Final", encoding="utf-8")

        effects = {
            0: lambda cwd: None,
            1: create_modules,
            # call 3: classify Worker 反思轮（不需要再创建）
            7: create_report,
            # call 9: analyse Worker 反思轮
            13: create_final,
        }

        tracker.agent_responses = [
            "<result>探索完成</result>",                                    # 0: explore
            "<result>分类完成</result>",                                    # 1: classify Worker R1
            "## 评分: 100\n## 通过: 是\n## 评审意见\nPASS",                # 2: classify Judge R1 → pass_count=1, 需反思
            "<result>反思后分类完成</result>",                              # 3: classify Worker R2
            "## 评分: 100\n## 通过: 是\n## 评审意见\nPASS",                # 4: classify Judge R2 → pass_count=2, break
            "<result>无需细分</result>",                                    # 5: refine Worker R1
            "## 评分: 90\n## 通过: 是\n## 评审意见\nOK",                   # 6: refine Judge R1 → pass_count=1, 反思
            "<result>反思后确认</result>",                                  # 7: refine Worker R2
            "## 评分: 95\n## 通过: 是\n## 评审意见\nOK",                   # 8: refine Judge R2 → pass_count=2, break
            "<result>分析完成</result>",                                    # 9: analyse Worker R1
            "## 评分: 85\n## 通过: 是\n## 评审意见\nOK",                   # 10: analyse Judge R1 → pass_count=1
            "<result>反思后分析</result>",                                  # 11: analyse Worker R2
            "## 评分: 90\n## 通过: 是\n## 评审意见\nOK",                   # 12: analyse Judge R2 → pass_count=2
            "## 评分: 100\n## 通过: 是\n## 评审意见\n完整",                 # 13: 4a Judge
            "<result>报告</result>",                                        # 14: 4b Worker R1
            "## 评分: 80\n## 通过: 是\n## 评审意见\nOK",                   # 15: 4b Judge R1 → pass_count=1
            "<result>修正报告</result>",                                    # 16: 4b Worker R2
            "## 评分: 90\n## 通过: 是\n## 评审意见\nOK",                   # 17: 4b Judge R2 → pass_count=2
        ]

        mock_agent = create_mock_agent(tracker, effects)

        with patch("app.orchestrator.run_agent", mock_agent), \
             patch("app.orchestrator.os.path.isfile", return_value=False):
            orch = Orchestrator(cfg, on_event=tracker.on_event)
            result = asyncio.run(orch.execute("test-002"))

        assert result.status.value == "passed", f"应成功: {result.error}"

        # 验证反思事件
        reflect_events = [e for e in tracker.events if e["type"] == "reflect"]
        assert len(reflect_events) >= 4, f"min_rounds=2 应至少 4 次反思(S1+S2+S3+S4b): got {len(reflect_events)}"

        # 验证 feedback 包含 Judge 意见
        for call in tracker.calls:
            prompt = call.get("prompt", "")
            if "Judge 上轮意见" in prompt:
                assert "judge-0:" in prompt, f"反思 feedback 缺 judge 具体意见"

        print(f"  ✅ 反思循环通过 (calls={len(tracker.calls)}, reflects={len(reflect_events)})")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ─── 测试 3: max_rounds 超限 ─────────────────────────────────────────────────

def test_max_rounds_exceeded():
    print("=== Test: max_rounds=2 超限 ===")
    tmp_dir = tempfile.mkdtemp(prefix="orch_test_")

    try:
        setup_workspace(tmp_dir)
        cfg = make_config(tmp_dir, min_rounds=1, max_rounds=2)
        tracker = CallTracker()

        def create_modules(cwd):
            ws = Path(cwd)
            d = ws / "mod_a"
            d.mkdir(exist_ok=True)
            (d / "files.list").write_text("file1.so\n", encoding="utf-8")

        effects = {0: lambda cwd: None, 1: create_modules}

        # classify 永远失败
        tracker.agent_responses = [
            "<result>探索完成</result>",                       # 0: explore
            "<result>分类</result>",                           # 1: classify Worker R1
            "## 评分: 30\n## 通过: 否\n## 评审意见\n不合格",   # 2: classify Judge R1
            "<result>修正</result>",                           # 3: classify Worker R2
            "## 评分: 40\n## 通过: 否\n## 评审意见\n仍不合格", # 4: classify Judge R2
        ]

        mock_agent = create_mock_agent(tracker, effects)

        with patch("app.orchestrator.run_agent", mock_agent), \
             patch("app.orchestrator.os.path.isfile", return_value=False):
            orch = Orchestrator(cfg, on_event=tracker.on_event)
            result = asyncio.run(orch.execute("test-003"))

        assert result.status.value == "failed", f"应失败但状态为 {result.status.value}"
        assert "Stage 1" in (result.error or ""), f"错误应提及 Stage 1: {result.error}"

        flag = (Path(tmp_dir) / "result" / "flag").read_text()
        assert flag == "0", f"flag 应为 0: {flag}"

        print(f"  ✅ max_rounds 超限通过 (status={result.status.value})")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ─── 测试 4: Stage 2 拆分 + 新模块入队 ───────────────────────────────────────

def test_stage2_split():
    print("=== Test: Stage 2 模块拆分 + 并行队列 ===")
    tmp_dir = tempfile.mkdtemp(prefix="orch_test_")

    try:
        setup_workspace(tmp_dir)
        cfg = make_config(tmp_dir, min_rounds=1)
        tracker = CallTracker()

        call_count = [0]

        def create_initial_modules(cwd):
            ws = Path(cwd)
            d = ws / "big_mod"
            d.mkdir(exist_ok=True)
            (d / "files.list").write_text("file1.so\nfile2.ko\nfile3.sh\n", encoding="utf-8")

        def split_module(cwd):
            """Worker 拆分 big_mod → sub_a + sub_b"""
            ws = Path(cwd)
            big = ws / "big_mod"
            if big.exists():
                shutil.rmtree(str(big))
            for name, content in [("sub_a", "file1.so\n"), ("sub_b", "file2.ko\nfile3.sh\n")]:
                d = ws / name
                d.mkdir(exist_ok=True)
                (d / "files.list").write_text(content, encoding="utf-8")

        def create_reports(cwd):
            ws = Path(cwd)
            for mod in ["sub_a", "sub_b"]:
                d = ws / mod
                if d.exists():
                    (d / "module_report.md").write_text(
                        f"<!-- RISK_LEVEL: 高 -->\n<!-- RISK_SCORE: 75 -->\n# {mod}\n",
                        encoding="utf-8")

        effects = {
            0: lambda cwd: None,
            1: create_initial_modules,
            3: split_module,   # refine Worker 拆分
            # sub_a, sub_b 的 refine 不需要再拆
        }

        tracker.agent_responses = [
            "<result>探索完成</result>",                           # 0: explore
            "<result>1模块</result>",                              # 1: classify Worker
            "## 评分: 100\n## 通过: 是",                          # 2: classify Judge
            "<result>拆分为sub_a和sub_b</result>",                # 3: refine big_mod Worker → split
            "## 评分: 90\n## 通过: 是",                           # 4: refine big_mod Judge
            "<result>sub_a无需细分</result>",                     # 5: refine sub_a Worker
            "## 评分: 85\n## 通过: 是",                           # 6: refine sub_a Judge
            "<result>sub_b无需细分</result>",                     # 7: refine sub_b Worker
            "## 评分: 85\n## 通过: 是",                           # 8: refine sub_b Judge
        ]
        # 后面用默认 pass 响应（analyse、4a、4b）

        mock_agent = create_mock_agent(tracker, effects)

        # 需要在 analyse 阶段创建 report 和 final_report
        orig_mock = mock_agent
        analyse_count = [0]
        async def mock_with_reports(**kwargs):
            r = await orig_mock(**kwargs)
            prompt = kwargs.get("prompt", "")
            cwd = kwargs.get("cwd", "")
            if "分析模块" in prompt:
                analyse_count[0] += 1
                create_reports(cwd)
            if "final_report" in prompt.lower() or "总报告" in prompt:
                (Path(cwd) / "final_report.md").write_text("# Final", encoding="utf-8")
            return r

        with patch("app.orchestrator.run_agent", mock_with_reports), \
             patch("app.orchestrator.os.path.isfile", return_value=False):
            orch = Orchestrator(cfg, on_event=tracker.on_event)
            result = asyncio.run(orch.execute("test-004"))

        assert result.status.value == "passed", f"应成功: {result.error}"

        # 验证拆分后新模块入队
        stage2_modules = [e.get("module") for e in tracker.events
                         if e["type"] == "stage" and e.get("stage") == 2]
        assert "big_mod" in stage2_modules, f"应处理 big_mod: {stage2_modules}"
        assert "sub_a" in stage2_modules, f"拆分后 sub_a 应入队: {stage2_modules}"
        assert "sub_b" in stage2_modules, f"拆分后 sub_b 应入队: {stage2_modules}"

        # 验证 modules.list 包含 sub_a、sub_b，不含 big_mod
        mlist = (Path(tmp_dir) / "result" / "modules.list").read_text().strip().split("\n")
        assert "sub_a" in mlist, f"modules.list 应含 sub_a: {mlist}"
        assert "sub_b" in mlist, f"modules.list 应含 sub_b: {mlist}"

        print(f"  ✅ Stage 2 拆分通过 (stage2_modules={stage2_modules})")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ─── 测试 5: _parse_eval_md 边界 ─────────────────────────────────────────────

def test_parse_edge_cases():
    print("=== Test: _parse_eval_md 边界情况 ===")

    # 空输出
    r = _parse_eval_md("")
    assert r["pass"] == False and r["score"] == 0, f"空输出: {r}"

    # JSON fallback
    r = _parse_eval_md('Some text {"pass": true, "score": 88, "feedback": "ok"} end')
    assert r["pass"] == True and r["score"] == 88, f"JSON fallback: {r}"

    # 混合：先写 score=0 再写 score=100 → 取最后
    r = _parse_eval_md("## 评分: 0\n...\n## 评分: 100\n## 通过: 是")
    assert r["score"] == 100 and r["pass"] == True, f"取最后评分: {r}"

    # RESULT: PASS 但无评分 → Judge 格式不合规，应判 fail
    r = _parse_eval_md("RESULT: PASS")
    assert r["pass"] == False and r["score"] == 0, f"无评分的 RESULT:PASS 应 fail: {r}"

    print("  ✅ 边界情况全部通过")


# ─── 测试 6: Stage 2-redo cwd 用 workspace ────────────────────────────────────

def test_stage2_redo_cwd():
    """验证 Stage 2-redo 的 Judge cwd 是否正确。"""
    print("=== Test: Stage 2-redo Judge cwd ===")
    tmp_dir = tempfile.mkdtemp(prefix="orch_test_")

    try:
        setup_workspace(tmp_dir)
        cfg = make_config(tmp_dir, min_rounds=1)
        tracker = CallTracker()

        def create_modules(cwd):
            ws = Path(cwd)
            d = ws / "mod_a"
            d.mkdir(exist_ok=True)
            (d / "files.list").write_text("file1.so\n", encoding="utf-8")

        effects = {0: lambda cwd: None, 1: create_modules}

        # Stage 3 Judge 返回 [需要重新分类]
        tracker.agent_responses = [
            "<result>探索</result>",                                        # 0: explore
            "<result>分类</result>",                                        # 1: classify Worker
            "## 评分: 100\n## 通过: 是",                                   # 2: classify Judge
            "<result>无需细分</result>",                                    # 3: refine Worker
            "## 评分: 90\n## 通过: 是",                                    # 4: refine Judge
            "<result>分析完成</result>",                                    # 5: analyse Worker
            "## 评分: 60\n## 通过: 否\n## 评审意见\n[需要重新分类] mod_a",  # 6: analyse Judge → 触发 reclassify
            "<result>重新细分</result>",                                    # 7: 2-redo Worker
            "## 评分: 90\n## 通过: 是",                                    # 8: 2-redo Judge
        ]
        # 后续默认 pass

        mock_agent = create_mock_agent(tracker, effects)
        orig_mock = mock_agent
        async def mock_with_reports(**kwargs):
            r = await orig_mock(**kwargs)
            cwd = kwargs.get("cwd", "")
            prompt = kwargs.get("prompt", "")
            if "分析模块" in prompt:
                ws = Path(cwd)
                for mod in ["mod_a"]:
                    d = ws / mod
                    if d.exists():
                        (d / "module_report.md").write_text(
                            "<!-- RISK_LEVEL: 中 -->\n# Report\n", encoding="utf-8")
            if "final_report" in prompt.lower() or "总报告" in prompt:
                (Path(cwd) / "final_report.md").write_text("# Final", encoding="utf-8")
            return r

        with patch("app.orchestrator.run_agent", mock_with_reports), \
             patch("app.orchestrator.os.path.isfile", return_value=False):
            orch = Orchestrator(cfg, on_event=tracker.on_event)
            result = asyncio.run(orch.execute("test-006"))

        # 验证 2-redo 阶段的 Judge 调用存在
        redo_stages = [e for e in tracker.events if e.get("stage") == "2-redo"]
        assert len(redo_stages) > 0, "应触发 Stage 2-redo"

        # 验证 2-redo Judge 的 cwd（检查 tracker.calls）
        redo_judge_calls = [c for c in tracker.calls
                          if "重新细分" in c.get("prompt", "") and "评审" in c.get("prompt", "")]
        for c in redo_judge_calls:
            cwd = c.get("cwd", "")
            assert "workspace" in cwd or "mod_a" in cwd, f"2-redo Judge cwd 异常: {cwd}"

        print(f"  ✅ Stage 2-redo cwd 验证通过 (status={result.status.value})")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ─── 测试 9: parallel_modules=2 并行处理不互相干扰 ────────────────────

def test_parallel_modules():
    print("=== Test: parallel_modules=2 并行处理 ===")
    tmp_dir = tempfile.mkdtemp(prefix="orch_test_")

    try:
        setup_workspace(tmp_dir)
        cfg = make_config(tmp_dir, min_rounds=1)
        cfg.parallel_modules = 2  # 开启并行
        tracker = CallTracker()

        def create_two_modules(cwd):
            ws = Path(cwd)
            for mod, files in [("mod_a", "file1.so\n"), ("mod_b", "file2.ko\n")]:
                d = ws / mod
                d.mkdir(exist_ok=True)
                (d / "files.list").write_text(files, encoding="utf-8")

        def create_reports(cwd):
            ws = Path(cwd)
            for mod in ["mod_a", "mod_b"]:
                d = ws / mod
                if d.exists():
                    (d / "module_report.md").write_text(
                        f"<!-- RISK_LEVEL: 中 -->\n# {mod}\n", encoding="utf-8")

        effects = {0: lambda cwd: None, 1: create_two_modules}

        # 两个模块并行进行 Stage 2、3，各模块各 1 次 Worker + 1 次 Judge
        tracker.agent_responses = [
            "<result>探索完成</result>",                    # 0: explore
            "<result>分类</result>",                            # 1: classify Worker
            "## 评分: 100\n## 通过: 是",                        # 2: classify Judge
            # Stage 2: 两模块并行，顺序不确定，用默认 pass
        ]

        orig_mock = create_mock_agent(tracker, effects)
        async def mock_with_all(**kwargs):
            r = await orig_mock(**kwargs)
            cwd = kwargs.get("cwd", "")
            prompt = kwargs.get("prompt", "")
            if "分析模块" in prompt:
                create_reports(cwd)
            if "总报告" in prompt or "final_report" in prompt.lower():
                (Path(cwd) / "final_report.md").write_text("# Final", encoding="utf-8")
            return r

        with patch("app.orchestrator.run_agent", mock_with_all), \
             patch("app.orchestrator.os.path.isfile", return_value=False):
            orch = Orchestrator(cfg, on_event=tracker.on_event)
            result = asyncio.run(orch.execute("test-009"))

        assert result.status.value == "passed", f"应成功: {result.error}"

        # 验证两个模块都被处理了
        s2_modules = [e.get("module") for e in tracker.events
                     if e["type"] == "stage" and e.get("stage") == 2]
        assert "mod_a" in s2_modules, f"mod_a 应被处理: {s2_modules}"
        assert "mod_b" in s2_modules, f"mod_b 应被处理: {s2_modules}"

        s3_modules = [e.get("module") for e in tracker.events
                     if e["type"] == "stage" and e.get("stage") == 3]
        assert "mod_a" in s3_modules, f"mod_a 应分析: {s3_modules}"
        assert "mod_b" in s3_modules, f"mod_b 应分析: {s3_modules}"

        print(f"  ✅ 并行处理通过 (s2_modules={s2_modules}, s3_modules={s3_modules})")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ─── 运行全部测试 ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Orchestrator Dry-Run 调度逻辑测试")
    print("=" * 60)
    print()

    tests = [
        test_parse_eval_md,
        test_check_voting,
        test_parse_edge_cases,
        test_normal_flow,
        test_reflect_loop,
        test_max_rounds_exceeded,
        test_stage2_split,
        test_stage2_redo_cwd,
        test_parallel_modules,
    ]

    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"  ❌ {t.__name__}: {e}")
            import traceback
            traceback.print_exc()
        print()

    print("=" * 60)
    print(f"  结果: {passed} 通过, {failed} 失败")
    print("=" * 60)
    sys.exit(1 if failed else 0)
