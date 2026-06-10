import sys; sys.stdout.reconfigure(encoding='utf-8')
import tempfile, shutil
from pathlib import Path

root = Path(tempfile.mkdtemp())
ws = root / "workspace"
(ws / "modules").mkdir(parents=True)
(ws / "filtered_files.txt").write_text("\n")
(ws / "deleted.list").write_text("")

def mkmod(name, files):
    d = ws / "modules" / name
    d.mkdir(parents=True)
    (d / "files.list").write_text("\n".join(files) + "\n")
    return d

mkmod("access",   ["src/a/a1.cpp","src/a/a2.cpp"])
mkmod("catalog",  ["src/c/c1.cpp","src/c/c2.cpp"])
mkmod("libpq",    ["src/l/pq1.cpp","src/l/pq2.cpp"])
mkmod("executor", ["src/e/e1.cpp","src/e/e2.cpp","src/e/e3.cpp"])
mkmod("storage",  ["src/s/buf.cpp","src/s/page.cpp","src/s/smgr.cpp"])
mkmod("network",  ["src/n/tcp.cpp","src/n/udp.cpp","src/n/dns.cpp"])
mkmod("adminpack",["contrib/a/ap.cpp"])
mkmod("pgcrypto", ["contrib/p/pg.cpp"])

from app.pipeline.s2_refine import _commit_one_module

print("=== S2 VERIFICATION ===\n")

# 1. Full split
d = ws / "modules" / "network"
(d / ".snapshot").write_text((d / "files.list").read_text())
(d / "files.list").unlink()
(d / "split/libcomm").mkdir(parents=True)
(d / "split/libcomm/files.list").write_text("src/n/tcp.cpp\nsrc/n/udp.cpp\n")
(d / "split/postgres").mkdir(parents=True)
(d / "split/postgres/files.list").write_text("src/n/dns.cpp\n")
info, mip = _commit_one_module(d, ws, set())
assert not d.exists() and info['new_modules'] == ['libcomm','postgres']
print("1. Full split: PASS")

# 2. Partial split
d = ws / "modules" / "storage"
(d / ".snapshot").write_text((d / "files.list").read_text())
(d / "files.list").write_text("src/s/smgr.cpp\n")
(d / "split/buffer").mkdir(parents=True)
(d / "split/buffer/files.list").write_text("src/s/buf.cpp\n")
(d / "split/page").mkdir(parents=True)
(d / "split/page/files.list").write_text("src/s/page.cpp\n")
info, mip = _commit_one_module(d, ws, set())
assert d.exists() and info['retained_parent']
assert (ws/"modules/buffer/files.list").exists()
print("2. Partial split: PASS")

# 3. Merge to in-progress
d = ws / "modules" / "executor"
(d / ".snapshot").write_text((d / "files.list").read_text())
(d / "files.list").write_text("src/e/e1.cpp\n")
(d / "split/_merge_to/libpq").mkdir(parents=True)
(d / "split/_merge_to/libpq/files.list").write_text("src/e/e2.cpp\nsrc/e/e3.cpp\n")
info, mip = _commit_one_module(d, ws, {"libpq"})
assert mip == {"libpq"}
assert "e2.cpp" in (ws/"modules/libpq/files.list").read_text()
print("3. Merge to in-progress: PASS")

# 4. Delete
d = ws / "modules" / "adminpack"
(d / ".snapshot").write_text((d / "files.list").read_text())
(d / "files.list").unlink()
(d / "deleted").mkdir(parents=True)
(d / "deleted/files.list").write_text("contrib/a/ap.cpp\n")
info, mip = _commit_one_module(d, ws, set())
assert "ap.cpp" in (ws/"deleted.list").read_text()
print("4. Delete: PASS")

# 5. No changes modules stay
assert (ws/"modules/access/files.list").exists()
assert (ws/"modules/catalog/files.list").exists()
assert (ws/"modules/pgcrypto/files.list").exists()
print("5. No-change modules preserved: PASS")

# 6. Post-merge commit preserves files
libpq_d = ws / "modules" / "libpq"
(libpq_d/".snapshot").write_text((libpq_d/"files.list").read_text())
info, mip = _commit_one_module(libpq_d, ws, set())
libpq_files = (libpq_d/"files.list").read_text()
assert "e2.cpp" in libpq_files and "e3.cpp" in libpq_files
assert not (libpq_d/".snapshot").exists()
print("6. Post-merge commit: PASS")

# 7. Final state
mods = {d.name for d in (ws/"modules").iterdir() if d.is_dir() and (d/"files.list").exists()}
assert "network" not in mods and "adminpack" not in mods and "executor" not in mods
print(f"7. Final modules: {sorted(mods)}")
assert mods == {"access","catalog","libpq","pgcrypto","storage","buffer","page","libcomm","postgres"}
print("7. Final state correct: PASS")

print("\nALL 7 TESTS PASSED")
