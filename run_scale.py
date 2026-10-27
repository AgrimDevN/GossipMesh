"""
Run the whole evaluation suite at several cluster sizes, one after another.

    python3 run_scale.py                       # 20, then 10, then 5 nodes, 20 trials each
    python3 run_scale.py --sizes 20 --trials 3 # quick check that 20 nodes works on your machine
    python3 run_scale.py --sizes 20 10 5 --trials 20 --workers 1

Results are appended to results/v2_results.jsonl (one row per trial, with a `nodes` column)
and results/summary.md is rebuilt after every size, so you can stop at any time and still
have a valid summary of what finished. Safe to re-run: it only appends.
Large clusters always use 1 worker: 20 Python processes on a laptop already compete for CPU.
"""
import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))

ap = argparse.ArgumentParser()
ap.add_argument("--sizes", type=int, nargs="+", default=[20, 10, 5])
ap.add_argument("--trials", type=int, default=20)
ap.add_argument("--workers", type=int, default=1)
a = ap.parse_args()

t0 = time.time()
for n in a.sizes:
    workers = a.workers if n <= 5 else 1          # never run two big clusters side by side
    print(f"\n===== {n} nodes, {a.trials} trials, {workers} worker(s) =====", flush=True)
    cmd = [sys.executable, os.path.join(HERE, "eval_v2.py"), "suite",
           "--trials", str(a.trials), "--nodes", str(n), "--workers", str(workers)]
    rc = subprocess.call(cmd)
    print(f"----- {n} nodes finished (exit {rc}), elapsed {(time.time() - t0) / 3600:.2f} h", flush=True)
subprocess.call([sys.executable, os.path.join(HERE, "eval_v2.py"), "analyze"])
print("\nDone. Send me results/summary.md and results/v2_results.jsonl.")
