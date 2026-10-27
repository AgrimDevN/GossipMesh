"""
GossipMesh v2 evaluation harness (Phase 1).

Four experiments, each run for any of the four modes
(adaptive, fixed-slow, fixed-base, fixed-fast):

  convergence  time from crash until EVERY survivor logs the victim(s) DEAD
               scenarios: single (E) | double (E, then C 1 s later)
  bandwidth    messages/s and bytes/s per node during a CALM window and a
               CHURN window (E and C crash-and-restart in turn)
  loss         false SUSPECT / DEAD verdicts when every node drops p% of its
               outbound packets and nobody has actually crashed
               (add --no-indirect / --no-guard for ablations)
  partition    link A<->B cut while both are alive: do the others wrongly
               suspect them? (indirect probing on vs off)

Every trial appends one JSON line to results/v2_results.jsonl.
`analyze` turns that file into results/summary.md (mean, SD, p95, Welch p).

Examples
  python3 eval_v2.py convergence --scenario single --modes adaptive fixed-base --trials 10
  python3 eval_v2.py bandwidth   --trials 3
  python3 eval_v2.py loss        --loss 0.3 --trials 5
  python3 eval_v2.py partition   --trials 5
  python3 eval_v2.py suite --quick          # small smoke run of everything
  python3 eval_v2.py suite --trials 20      # the full run (takes a while)
  python3 eval_v2.py suite --trials 20 --nodes 20 --workers 1   # 20-node cluster
  python3 eval_v2.py analyze

Trials run in parallel (--workers, default 2), each on its own port block,
so don't set --workers higher than your CPU count or timings get noisy.
"""
import argparse
import json
import math
import os
import queue
import re
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(HERE, "results")
RESULTS_FILE = os.environ.get("GOSSIPMESH_RESULTS", os.path.join(RESULTS_DIR, "v2_results.jsonl"))
MODES = ["adaptive", "fixed-slow", "fixed-base", "fixed-fast"]
import string
ALL_NAMES = list(string.ascii_uppercase)           # up to 26 nodes: A..Z
N_NODES = 5                                        # set by --nodes (see set_nodes)
NAMES = ALL_NAMES[:N_NODES]
SETTLE = 25.0        # s: > warm-up 2 + churn window 8 + idle ramp 10, so adaptive is fully idle


def set_nodes(n):
    """Set the cluster size for every experiment in this process (>= 5, <= 26)."""
    global N_NODES, NAMES, SETTLE
    if not 5 <= n <= len(ALL_NAMES):
        raise SystemExit(f"--nodes must be between 5 and {len(ALL_NAMES)}")
    N_NODES = n
    NAMES = ALL_NAMES[:n]
    # bigger clusters need longer to join, converge and for adaptive to go fully idle
    SETTLE = 25.0 + 1.0 * max(0, n - 5)


def detect_deadline():
    """Max seconds to wait for every survivor to log the victim DEAD."""
    return 35.0 + 3.0 * max(0, N_NODES - 5)
_write_lock = threading.Lock()


# ----------------------------------------------------------------------
# Cluster helper
# ----------------------------------------------------------------------
class Cluster:
    def __init__(self, base_port, mode, extra_env=None, per_node_args=None):
        self.ports = {n: base_port + i + 1 for i, n in enumerate(NAMES)}
        self.mode = mode
        self.env = os.environ.copy()
        self.env.update(extra_env or {})
        self.per_node_args = per_node_args or {}
        self.dir = tempfile.mkdtemp(prefix=f"gm_{base_port}_")
        self.procs = {}

    def log_path(self, n):
        return os.path.join(self.dir, f"{n}.log")

    def stats_path(self, n):
        return os.path.join(self.dir, f"{n}.stats.json")

    def start(self, n):
        bootstrap = ",".join(f"{k}={v}" for k, v in self.ports.items())
        cmd = [sys.executable, "-u", os.path.join(HERE, "node.py"),
               "--id", n, "--port", str(self.ports[n]), "--mode", self.mode,
               "--stats-file", self.stats_path(n)]
        cmd += ["--bootstrap", bootstrap] if n == "A" else ["--seed", f"127.0.0.1:{self.ports['A']}"]
        cmd += self.per_node_args.get(n, [])
        f = open(self.log_path(n), "a")
        self.procs[n] = (subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env=self.env), f)

    def start_all(self):
        for n in NAMES:
            self.start(n)
            time.sleep(0.05)       # whole cluster must be bound within the 2 s warm-up

    def kill(self, n):
        p, f = self.procs[n]
        p.kill()
        p.wait()

    def stop(self):
        for p, f in self.procs.values():
            try:
                p.kill()
            except OSError:
                pass
            f.close()
        time.sleep(0.3)

    def text(self, n):
        try:
            with open(self.log_path(n)) as fh:
                return fh.read()
        except OSError:
            return ""

    def stats(self, n):
        try:
            with open(self.stats_path(n)) as fh:
                return json.load(fh)
        except (OSError, ValueError):
            return None


def record(row):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with _write_lock, open(RESULTS_FILE, "a") as fh:
        fh.write(json.dumps(row) + "\n")


# ----------------------------------------------------------------------
# Experiments (each returns a dict that is stored as one result row)
# ----------------------------------------------------------------------
def trial_convergence(base_port, mode, scenario):
    c = Cluster(base_port, mode)
    try:
        c.start_all()
        time.sleep(SETTLE)
        victims = ["E"] if scenario == "single" else ["E", "C"]
        survivors = [n for n in NAMES if n not in victims]
        offset = {n: len(c.text(n)) for n in NAMES}   # ignore anything logged before the crash
        t0 = time.time()
        c.kill("E")
        if scenario == "double":
            time.sleep(1.0)
            c.kill("C")
        need = {(s, v) for s in survivors for v in victims}
        seen = {}
        deadline = t0 + detect_deadline()
        while time.time() < deadline and len(seen) < len(need):
            time.sleep(0.1)
            for s in survivors:
                txt = c.text(s)[offset[s]:]
                for v in victims:
                    if (s, v) not in seen and f"{v} -> DEAD" in txt:
                        seen[(s, v)] = time.time() - t0
        ok = len(seen) == len(need)
        return {"value": max(seen.values()) if ok else None,
                "first": min(seen.values()) if seen else None}
    finally:
        c.stop()


def _delta_rate(a, b, keys=("sent", "bytes")):
    dt = b["t"] - a["t"]
    return {k: (b[k] - a[k]) / dt for k in keys}


def trial_bandwidth(base_port, mode, calm=30.0, churn_flap=True):
    c = Cluster(base_port, mode)
    steady = ["A", "B", "D"]            # never killed -> comparable across windows
    try:
        c.start_all()
        time.sleep(SETTLE)
        s0 = {n: c.stats(n) for n in steady}
        time.sleep(calm)
        s1 = {n: c.stats(n) for n in steady}
        # churn window: E and C crash and come back, one after the other
        c.kill("E"); time.sleep(6)
        c.start("E"); time.sleep(4)
        c.kill("C"); time.sleep(6)
        c.start("C"); time.sleep(4)
        s2 = {n: c.stats(n) for n in steady}
        if any(v is None for d in (s0, s1, s2) for v in d.values()):
            return {"value": None}
        calm_r = [_delta_rate(s0[n], s1[n]) for n in steady]
        churn_r = [_delta_rate(s1[n], s2[n]) for n in steady]
        avg = lambda rs, k: sum(r[k] for r in rs) / len(rs)
        return {"calm_msgs_s": avg(calm_r, "sent"), "calm_bytes_s": avg(calm_r, "bytes"),
                "churn_msgs_s": avg(churn_r, "sent"), "churn_bytes_s": avg(churn_r, "bytes"),
                "value": avg(calm_r, "sent")}
    finally:
        c.stop()


def _count(c, pattern, nodes=None):
    nodes = NAMES if nodes is None else nodes     # NOT a default arg: NAMES changes with --nodes
    return sum(len(re.findall(pattern, c.text(n))) for n in nodes)


def trial_loss(base_port, mode, loss, indirect=True, guard=True, duration=45.0):
    env = {"GOSSIPMESH_LOSS": str(loss), "GOSSIPMESH_FAULT_AFTER": "15"}
    if not indirect:
        env["GOSSIPMESH_NO_INDIRECT"] = "1"
    if not guard:
        env["GOSSIPMESH_NO_GUARD"] = "1"
    c = Cluster(base_port, mode, extra_env=env)
    try:
        c.start_all()
        time.sleep(15 + duration)
        # nobody crashed, so EVERY verdict below is a false positive
        return {"value": _count(c, r"-> DEAD"),
                "false_dead": _count(c, r"-> DEAD"),
                "false_suspect": _count(c, r"-> SUSPECT"),
                "refutations": _count(c, r"REFUTING"),
                "guard_engaged": _count(c, r"OVERLOAD GUARD engaged")}
    finally:
        c.stop()


def trial_partition(base_port, mode, indirect=True, duration=40.0):
    env = {"GOSSIPMESH_FAULT_AFTER": "15"}
    if not indirect:
        env["GOSSIPMESH_NO_INDIRECT"] = "1"
    # cut the A<->B link only (A drops everything to/from B's port)
    c = Cluster(base_port, mode, extra_env=env)
    c.per_node_args = {"A": ["--block-ports", str(c.ports["B"])]}
    try:
        c.start_all()
        time.sleep(15 + duration)
        return {"value": _count(c, r"-> SUSPECT"),
                "false_suspect": _count(c, r"-> SUSPECT"),
                "false_dead": _count(c, r"-> DEAD"),
                "refutations": _count(c, r"REFUTING")}
    finally:
        c.stop()


# ----------------------------------------------------------------------
# Scheduling: run many trials in parallel, each on its own port block
# ----------------------------------------------------------------------
def run_jobs(jobs, workers, port_base):
    """jobs: list of (label_dict, callable(base_port) -> dict). Appends results."""
    slots = queue.Queue()
    for i in range(workers):
        slots.put(port_base + 100 * i)

    def work(job):
        meta, fn = job
        base = slots.get()
        try:
            t = time.time()
            res = fn(base)
        finally:
            slots.put(base)
        row = dict(meta, nodes=N_NODES, **res, wall=round(time.time() - t, 1), ts=time.time())
        record(row)
        print("  ", {k: (round(v, 2) if isinstance(v, float) else v)
                     for k, v in row.items() if k not in ("ts", "wall")}, flush=True)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(work, jobs))


def jobs_convergence(modes, scenarios, trials):
    jobs = []
    for i in range(trials):                     # interleave modes so drift hits all equally
        for sc in scenarios:
            for m in modes:
                jobs.append(({"exp": "convergence", "mode": m, "config": sc, "trial": i},
                             lambda b, m=m, sc=sc: trial_convergence(b, m, sc)))
    return jobs


def jobs_bandwidth(modes, trials):
    return [({"exp": "bandwidth", "mode": m, "config": "calm+churn", "trial": i},
             lambda b, m=m: trial_bandwidth(b, m))
            for i in range(trials) for m in modes]


def jobs_loss(modes, losses, trials, ablate):
    jobs = []
    configs = [("full", True, True)]
    if ablate:
        configs += [("no-indirect", False, True), ("no-guard", True, False)]
    for i in range(trials):
        for p in losses:
            for m in modes:
                for name, ind, gd in configs:
                    jobs.append(({"exp": "loss", "mode": m, "config": f"loss={p:g},{name}", "trial": i},
                                 lambda b, m=m, p=p, ind=ind, gd=gd: trial_loss(b, m, p, ind, gd)))
    return jobs


def jobs_partition(modes, trials):
    jobs = []
    for i in range(trials):
        for m in modes:
            for name, ind in (("indirect-on", True), ("indirect-off", False)):
                jobs.append(({"exp": "partition", "mode": m, "config": name, "trial": i},
                             lambda b, m=m, ind=ind: trial_partition(b, m, ind)))
    return jobs


# ----------------------------------------------------------------------
# Analysis
# ----------------------------------------------------------------------
def welch_p(a, b):
    """Two-sided Welch t-test p-value (scipy if present, else own implementation)."""
    if len(a) < 2 or len(b) < 2:
        return None
    try:
        from scipy import stats
        return float(stats.ttest_ind(a, b, equal_var=False).pvalue)
    except ImportError:
        pass
    ma, mb = statistics.mean(a), statistics.mean(b)
    va, vb = statistics.variance(a) / len(a), statistics.variance(b) / len(b)
    if va + vb == 0:
        return 1.0 if ma == mb else 0.0
    t = (ma - mb) / math.sqrt(va + vb)
    df = (va + vb) ** 2 / (va ** 2 / (len(a) - 1) + vb ** 2 / (len(b) - 1))
    x = df / (df + t * t)
    return _betainc(df / 2, 0.5, x)


def _betainc(a, b, x):
    """Regularised incomplete beta via continued fraction (Numerical Recipes)."""
    if x <= 0 or x >= 1:
        return float(x >= 1)
    lbeta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
    front = math.exp(lbeta + a * math.log(x) + b * math.log(1 - x))

    def cf(a, b, x):
        tiny = 1e-30
        c, d = 1.0, 1.0 - (a + b) * x / (a + 1)
        d = 1 / (d if abs(d) > tiny else tiny)
        h = d
        for m in range(1, 200):
            m2 = 2 * m
            aa = m * (b - m) * x / ((a + m2 - 1) * (a + m2))
            d = 1 + aa * d; d = 1 / (d if abs(d) > tiny else tiny)
            c = 1 + aa / c; c = c if abs(c) > tiny else tiny
            h *= d * c
            aa = -(a + m) * (a + b + m) * x / ((a + m2) * (a + m2 + 1))
            d = 1 + aa * d; d = 1 / (d if abs(d) > tiny else tiny)
            c = 1 + aa / c; c = c if abs(c) > tiny else tiny
            delta = d * c
            h *= delta
            if abs(delta - 1) < 1e-10:
                break
        return h

    if x < (a + 1) / (a + b + 2):
        return front * cf(a, b, x) / a
    return 1 - front * cf(b, a, 1 - x) / b


def p95(xs):
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, math.ceil(0.95 * len(xs)) - 1))
    return xs[k]


def fmt_p(p):
    return "-" if p is None else (f"{p:.3f}" if p >= 0.001 else "<0.001")


def load_rows():
    if not os.path.exists(RESULTS_FILE):
        return []
    with open(RESULTS_FILE) as fh:
        return [json.loads(l) for l in fh if l.strip()]


def analyze():
    rows = load_rows()
    out = ["# GossipMesh v2 results (generated by `eval_v2.py analyze`)", "",
           "Mean (SD) over n trials; p95 = 95th percentile; p = Welch two-sided t-test vs "
           "`fixed-base` (the v1 baseline). Treat p > 0.05 as *no demonstrated difference*.", ""]

    def group(exp):
        g = {}
        for r in rows:
            if r["exp"] == exp:
                key = f'{r.get("nodes", 5)} nodes, {r["config"]}'
                g.setdefault(key, {}).setdefault(r["mode"], []).append(r)
        return g

    # convergence
    g = group("convergence")
    if g:
        out += ["## Failure-detection convergence (s, crash -> all survivors DEAD)", "",
                "| cluster / scenario | mode | n | mean (SD) | p95 | failed | p vs fixed-base |", "|---|---|---|---|---|---|---|"]
        for cfg, modes in sorted(g.items()):
            base = [r["value"] for r in modes.get("fixed-base", []) if r.get("value") is not None]
            for m in MODES:
                if m not in modes:
                    continue
                vals = [r["value"] for r in modes[m] if r.get("value") is not None]
                failed = len(modes[m]) - len(vals)
                if not vals:
                    out.append(f"| {cfg} | {m} | 0 | - | - | {failed} | - |")
                    continue
                sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
                p = "-" if m == "fixed-base" else fmt_p(welch_p(vals, base))
                out.append(f"| {cfg} | {m} | {len(vals)} | {statistics.mean(vals):.2f} ({sd:.2f}) | "
                           f"{p95(vals):.2f} | {failed} | {p} |")
        out.append("")

    # bandwidth
    g = group("bandwidth")
    if g:
        out += ["## Bandwidth per node (never-killed nodes A, B, D)", "",
                "| cluster | mode | n | calm msgs/s | calm B/s | churn msgs/s | churn B/s | calm saving vs fixed-base |",
                "|---|---|---|---|---|---|---|---|"]
        for cfg, modes in sorted(g.items()):
            size = cfg.split(",")[0]
            base_calm = [r["calm_msgs_s"] for r in modes.get("fixed-base", []) if r.get("calm_msgs_s")]
            bc = statistics.mean(base_calm) if base_calm else None
            for m in MODES:
                rs = [r for r in modes.get(m, []) if r.get("calm_msgs_s")]
                if not rs:
                    continue
                mean = lambda k: statistics.mean(r[k] for r in rs)
                save = "-" if bc is None or m == "fixed-base" else f"{100 * (1 - mean('calm_msgs_s') / bc):+.0f}%"
                out.append(f"| {size} | {m} | {len(rs)} | {mean('calm_msgs_s'):.2f} | {mean('calm_bytes_s'):.0f} | "
                           f"{mean('churn_msgs_s'):.2f} | {mean('churn_bytes_s'):.0f} | {save} |")
        out.append("")

    # loss and partition
    for exp, title in (("loss", "False verdicts under packet loss (nobody crashed; lower is better)"),
                       ("partition", "False verdicts with the A<->B link cut (lower is better)")):
        g = group(exp)
        if not g:
            continue
        out += [f"## {title}", "",
                "| config | mode | n | false DEAD (mean) | false SUSPECT (mean) | refutations (mean) | guard engaged (mean) |",
                "|---|---|---|---|---|---|---|"]
        for cfg, modes in sorted(g.items()):
            for m in MODES:
                rs = [r for r in modes.get(m, []) if r.get("false_dead") is not None]
                if not rs:
                    continue
                mean = lambda k: statistics.mean(r.get(k, 0) for r in rs)
                out.append(f"| {cfg} | {m} | {len(rs)} | {mean('false_dead'):.1f} | {mean('false_suspect'):.1f} | "
                           f"{mean('refutations'):.1f} | {mean('guard_engaged'):.1f} |")
        out.append("")

    text = "\n".join(out)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(os.path.join(RESULTS_DIR, "summary.md"), "w") as fh:
        fh.write(text + "\n")
    print(text)


# ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["convergence", "bandwidth", "loss", "partition", "suite", "analyze"])
    ap.add_argument("--modes", nargs="+", choices=MODES, default=MODES)
    ap.add_argument("--scenario", choices=["single", "double", "both"], default="both")
    ap.add_argument("--trials", type=int, default=10)
    ap.add_argument("--loss", type=float, nargs="+", default=[0.3])
    ap.add_argument("--ablate", action="store_true", help="loss: also run no-indirect and no-guard variants")
    ap.add_argument("--nodes", type=int, default=5,
                    help="cluster size, 5..26 (default 5). Use --workers 1 for 20 nodes.")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--port-base", type=int, default=20000)
    ap.add_argument("--quick", action="store_true", help="suite: tiny smoke run (not for conclusions)")
    a = ap.parse_args()

    scen = ["single", "double"] if a.scenario == "both" else [a.scenario]
    if a.cmd == "analyze":
        return analyze()
    set_nodes(a.nodes)
    if a.nodes > 10 and a.workers > 1:
        print(f"WARNING: {a.nodes} nodes x {a.workers} workers = {a.nodes * a.workers} processes; "
              f"on a laptop this competes for CPU and can itself cause false failures. Use --workers 1.",
              flush=True)
    if a.cmd == "convergence":
        jobs = jobs_convergence(a.modes, scen, a.trials)
    elif a.cmd == "bandwidth":
        jobs = jobs_bandwidth(a.modes, a.trials)
    elif a.cmd == "loss":
        jobs = jobs_loss(a.modes, a.loss, a.trials, a.ablate)
    elif a.cmd == "partition":
        jobs = jobs_partition(a.modes, a.trials)
    else:  # suite
        n = 2 if a.quick else a.trials
        nb = 1 if a.quick else max(3, a.trials // 4)
        nf = 2 if a.quick else max(5, a.trials // 2)
        jobs = (jobs_convergence(a.modes, ["single", "double"], n) + jobs_bandwidth(a.modes, nb) +
                jobs_loss(["adaptive", "fixed-base"], a.loss, nf, True) +
                jobs_partition(["adaptive"], nf))
    print(f"running {len(jobs)} trials on {a.workers} workers, {a.nodes} nodes each ...", flush=True)
    run_jobs(jobs, a.workers, a.port_base)
    analyze()


if __name__ == "__main__":
    main()
