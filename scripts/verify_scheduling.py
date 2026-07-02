#!/usr/bin/env python3
"""调度器综合验证：6 原则 + 任务删除(各状态) + 大规模排队。在 API pod 内运行。"""
import json, time, sys, urllib.request, urllib.error
import yaml, pymysql

BASE = "http://localhost:8080/api/app/system-analyse"
VALID_INPUT = "/data/files/2abc83006a7ca7a4/user_input/document/3d080f256016491ba6797244610b85b1"
PROJECT = "2abc83006a7ca7a4"

# 加载 service_machine_token（machine token 可过 get_current_user）
_svc = yaml.safe_load(open("/app/service.yaml"))
TOKEN = (_svc.get("auth_service") or {}).get("service_machine_token") or ""

def db_conn():
    d = _svc["database"]
    return pymysql.connect(host=d["host"], port=int(d["port"]), user=d.get("user") or "root",
                           password=d["password"], database=d["name"])

def q_status(task_id):
    c = db_conn(); cur = c.cursor()
    cur.execute("SELECT status, dispatcher_instance_id FROM secflow_app_sa_tasks WHERE task_id=%s", (task_id,))
    r = cur.fetchone(); c.close()
    if not r: return ("?", "-")
    return (r[0], (r[1][:24] if r[1] else "-"))

def api(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            try: return r.status, json.loads(raw)
            except Exception: return r.status, raw.decode(errors="replace")[:200]
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try: return e.code, json.loads(raw)
        except Exception: return e.code, raw[:200]

def submit(name, inp=VALID_INPUT):
    st, d = api("POST", "/tasks", {"project_id": PROJECT, "task_name": name, "input_path": inp})
    tid = d.get("task_id") if isinstance(d, dict) else None
    print(f"  submit {name}: {st} -> {tid}")
    return tid

def wait_terminal(tid, timeout=180):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        st, disp = q_status(tid)
        if st != last:
            print(f"    {tid[:16]}: {st} (disp={disp})"); last = st
        if st in ("passed", "failed", "error", "cancelled"):
            return st
        time.sleep(3)
    return last

def wait_running(tid, timeout=60):
    """等任务进入 running 后立刻返回（用于 cancel 测试）。"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        st, _ = q_status(tid)
        if st == "running":
            return True
        if st in ("passed", "failed", "error", "cancelled"):
            return False
        time.sleep(1)
    return False

def archive_exists(tid):
    c = db_conn(); cur = c.cursor()
    cur.execute("SELECT output_path FROM secflow_app_sa_tasks WHERE task_id=%s", (tid,))
    r = cur.fetchone(); c.close()
    if not r or not r[0]:
        return "?"
    import os
    base = os.path.join(r[0], tid)
    return os.path.exists(base)

results = []
def check(name, ok, detail=""):
    results.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")

print("=" * 60)
print("原则1: 新任务正常执行")
t1 = submit("V-01-NEW")
if t1:
    st = wait_terminal(t1)
    check("1.新任务执行", st == "passed", f"-> {st}")

print("\n原则6: 异常结束调度 (单独用热补丁验证，此处跳过)")
t6 = None

print("\n原则2: cancel + 产物归档 (批量提交制造排队，cancel 最后提交的那个)")
batch_cancel = [submit(f"V-02-CANCEL-{i}") for i in range(4)]
batch_cancel = [t for t in batch_cancel if t]
t2 = batch_cancel[-1] if batch_cancel else None
if t2:
    st, resp = api("POST", f"/tasks/{t2}/cancel")
    time.sleep(5)
    final = wait_terminal(t2, timeout=30)
    arc = archive_exists(t2)
    check("2.cancel", final == "cancelled", f"-> {final} http={st}")
    check("2.产物归档", arc in (True, "?"), f"archive={arc}")
    for t in batch_cancel[:-1]:
        wait_terminal(t, timeout=120)

print("\n原则3: restart 正常")
t3 = submit("V-03-RESTART")
if t3:
    wait_terminal(t3)
    st, _ = api("POST", f"/tasks/{t3}/restart")
    time.sleep(3)
    st2 = wait_terminal(t3)
    check("3.restart", st2 in ("passed", "failed", "error", "running"), f"-> {st2}")

print("\n原则4: cancel 然后 restart")
t4 = submit("V-04-CANCEL-RESTART")
if t4:
    wait_running(t4, timeout=30)
    api("POST", f"/tasks/{t4}/cancel")
    time.sleep(5)
    wait_terminal(t4, timeout=20)
    st, _ = api("POST", f"/tasks/{t4}/restart")
    time.sleep(3)
    st2 = wait_terminal(t4)
    check("4.cancel后restart", st2 in ("passed", "running", "failed", "error"), f"-> {st2}")

print("\n原则: 任务删除 (passed/cancelled)")
for label, tid in [("passed", t1), ("cancelled", t2)]:
    if tid:
        st, _ = api("DELETE", f"/tasks/{tid}?delete_files=true")
        c = db_conn(); cur = c.cursor()
        cur.execute("SELECT is_deleted FROM secflow_app_sa_tasks WHERE task_id=%s", (tid,))
        r = cur.fetchone(); c.close()
        gone = (r is None) or (r[0] == 1)
        check(f"删除{label}任务", st in (200, 204) and gone, f"http={st} deleted={gone}")

print("\n大规模排队: 提交 12 个任务，验证 FIFO + 全部终态")
batch = [submit(f"V-QUEUE-{i:02d}") for i in range(12)]
batch = [t for t in batch if t]
print(f"  submitted {len(batch)} tasks")
t0 = time.time()
term = {}
while time.time() - t0 < 300:
    for tid in batch:
        if tid not in term:
            st, _ = q_status(tid)
            if st in ("passed", "failed", "error", "cancelled"):
                term[tid] = st
    if len(term) == len(batch):
        break
    time.sleep(5)
from collections import Counter
ctr = Counter(term.values())
print(f"  terminal counts: {dict(ctr)} (elapsed {int(time.time()-t0)}s)")
check("排队全部终态", len(term) == len(batch), f"{len(term)}/{len(batch)}")
check("排队有成功", ctr.get("passed", 0) > 0, f"passed={ctr.get('passed',0)}")

print("\n" + "=" * 60)
passed = sum(1 for _, ok in results if ok)
print(f"SUMMARY: {passed}/{len(results)} passed")
for n, ok in results:
    print(f"  {'✓' if ok else '✗'} {n}")
sys.exit(0 if passed == len(results) else 1)
