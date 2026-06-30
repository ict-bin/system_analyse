#!/usr/bin/env python3
"""test_models_pi_scenarios.py — 在 worker pod 内按微服务方式验证 4 场景。

每场景:
  1. 生成 models.json（两源: configcenter + AIGW aliases）
  2. 按 secret 规则处理 apiKey（有 secret 替换全部，无保持 DB）
  3. 起 pi --no-session --model <model> -p "回复两个字:你好"，验证连通

用法(在 runner pod 内 /app 目录):
  PYTHONPATH=/app python3 /tmp/test_models_pi_scenarios.py
"""
import json, os, subprocess, sys, time
from pathlib import Path

sys.path.insert(0, "/app")
from app.config import get_service_yaml
from app.service.llm_provider_sync import sync_providers_to_pi

PI_DIR = Path(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent"))
WSK = os.environ.get("TEST_WSK", "wsk_ehc6vw1o23e55lfivnxpx6jz82cxi2k4")

def gen_models(substitute_secret=None):
    """两源生成 models.json；有 secret 则替换全部 provider apiKey。"""
    svc = get_service_yaml()
    sync_providers_to_pi(base_url=svc.configcenter.base_url,
                         token=svc.auth_service.service_machine_token,
                         timeout=svc.configcenter.timeout)
    if substitute_secret:
        p = PI_DIR / "models.json"
        d = json.loads(p.read_text("utf-8"))
        for pcfg in d.get("providers", {}).values():
            if isinstance(pcfg, dict):
                pcfg["apiKey"] = substitute_secret
        p.write_text(json.dumps(d, ensure_ascii=False, indent=2), "utf-8")
        print(f"  [key] 已替换全部 provider apiKey -> {substitute_secret[:12]}..")
    else:
        print("  [key] 保持 DB apiKey（无替换）")

def run_pi(model, label):
    """起 pi 发 hello，返回是否连通。"""
    print(f"  [pi] model={model} ...", end=" ", flush=True)
    try:
        r = subprocess.run(
            ["pi", "--no-session", "--model", model, "-p", "回复两个字:你好"],
            capture_output=True, text=True, timeout=60, cwd="/tmp",
        )
        out = (r.stdout or "").strip()
        if r.returncode == 0 and out:
            print(f"OK (rc=0, out={out[:40]!r})")
            return True
        err = (r.stderr or "").strip()
        print(f"FAIL (rc={r.returncode}, err={err[:100]!r}, out={out[:40]!r})")
        return False
    except subprocess.TimeoutExpired:
        print("FAIL (timeout 60s)")
        return False

def scenario(name, has_key, model, expect_subst):
    print(f"\n=== {name} ===")
    print(f"  有key={has_key} model={model} 期望: {'wsk替换' if expect_subst else 'sk保持'}")
    gen_models(substitute_secret=WSK if has_key else None)
    # 打印该 model 的 apiKey 供核对
    d = json.loads((PI_DIR/"models.json").read_text("utf-8"))
    prov = model.split("/")[0] if "/" in model else model
    pcfg = d.get("providers", {}).get(prov, {})
    key = str(pcfg.get("apiKey", ""))[:14]
    print(f"  [核对] {prov} apiKey={key}.. models={[m['id'] for m in pcfg.get('models',[])][:3]}")
    return run_pi(model, name)

def main():
    results = {}
    # 1. 有key 无模型 → auto (gaiasec/auto)
    results["s1 有key无模型→auto"] = scenario("s1 有key无模型→auto", True, "gaiasec/auto", True)
    # 2. 有key 有模型 → 该模型 (gaiasec/auto)
    results["s2 有key有模型→该模型"] = scenario("s2 有key有模型→该模型", True, "gaiasec/auto", True)
    # 3. 无key 无模型 → 参数配置界面 (local_minimax, sk)
    results["s3 无key无模型→服务默认"] = scenario("s3 无key无模型→服务默认", False, "local_minimax/MiniMax/MiniMax-M2.5", False)
    # 4. 无key 有模型 → 该模型 (local_minimax, sk)
    results["s4 无key有模型→该模型"] = scenario("s4 无key有模型→该模型", False, "local_minimax/MiniMax/MiniMax-M2.5", False)
    print("\n=== 汇总 ===")
    for k, v in results.items():
        print(f"  {'✅' if v else '❌'} {k}")
    ok = all(results.values())
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1

if __name__ == "__main__":
    sys.exit(main())
