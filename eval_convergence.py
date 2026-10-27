"""
Objective 6 evaluation harness: failure-detection convergence time for
GossipMesh's adaptive controller vs. a fixed-parameter baseline.

Convergence time = time from crash until EVERY surviving node has logged
the crashed node(s) as DEAD.

Scenarios
  single : one node (E) crashes in a settled cluster.
  double : E crashes, then C crashes 1.0 s later (a churn burst).

Modes
  adaptive : normal controller (interval/fanout driven by churn rate).
  fixed    : GOSSIPMESH_FIXED=1 -> interval/fanout pinned to the base constants.

Settle time is 12 s (> CHURN_WINDOW=8 s + 2 s warm-up) so startup joins have
left the sliding window and the cluster really is "quiet" at crash time.

Usage:
    python3 eval_convergence.py --trials 8 --mode adaptive --scenario single
    python3 eval_convergence.py --trials 8 --mode fixed    --scenario double
"""
import subprocess, sys, os, time, argparse, statistics

HERE = os.path.dirname(os.path.abspath(__file__))
NODES = {"A": 9101, "B": 9102, "C": 9103, "D": 9104, "E": 9105}
BOOTSTRAP = ",".join(f"{nid}={port}" for nid, port in NODES.items())
SETTLE = 12.0


def run_trial(mode, scenario):
    logdir = os.path.join(HERE, "eval_logs")
    os.makedirs(logdir, exist_ok=True)
    procs = {}
    env = os.environ.copy()
    if mode == "fixed":
        env["GOSSIPMESH_FIXED"] = "1"

    for nid, port in NODES.items():
        f = open(os.path.join(logdir, f"{nid}.log"), "w")
        cmd = [sys.executable, "-u", os.path.join(HERE, "node.py"),
               "--id", nid, "--port", str(port)]
        cmd += ["--bootstrap", BOOTSTRAP] if nid == "A" else ["--seed", f"127.0.0.1:{NODES['A']}"]
        procs[nid] = (subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env=env), f)
        time.sleep(0.15)

    time.sleep(SETTLE)

    crash_t = time.time()
    victims = ["E"] if scenario == "single" else ["E", "C"]
    procs["E"][0].kill()
    if scenario == "double":
        time.sleep(1.0)
        procs["C"][0].kill()

    survivors = [n for n in NODES if n not in victims]
    need = {(s, v) for s in survivors for v in victims}
    seen = {}
    deadline = crash_t + 25
    while time.time() < deadline and len(seen) < len(need):
        time.sleep(0.1)
        for s in survivors:
            with open(os.path.join(logdir, f"{s}.log")) as fh:
                txt = fh.read()
            for v in victims:
                if (s, v) not in seen and f"{v} -> DEAD" in txt:
                    seen[(s, v)] = time.time() - crash_t

    for nid, (p, f) in procs.items():
        p.kill()
        f.close()
    time.sleep(0.5)
    return max(seen.values()) if len(seen) == len(need) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--mode", choices=["adaptive", "fixed"], default="adaptive")
    ap.add_argument("--scenario", choices=["single", "double"], default="single")
    args = ap.parse_args()

    results = []
    for i in range(args.trials):
        t = run_trial(args.mode, args.scenario)
        print(f"trial {i+1}/{args.trials}: {t if t is None else round(t,2)}", flush=True)
        if t:
            results.append(t)

    if results:
        sd = statistics.stdev(results) if len(results) > 1 else 0.0
        print(f"RESULT mode={args.mode} scenario={args.scenario} "
              f"mean={statistics.mean(results):.2f}s sd={sd:.2f} "
              f"min={min(results):.2f} max={max(results):.2f} n={len(results)}")


if __name__ == "__main__":
    main()
