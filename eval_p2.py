"""
GossipMesh v2 evaluation harness - Phase 2.

Builds on eval_v2.py (cluster launcher, scheduler, statistics) and does NOT modify it.
Results go to results/phase2_results.jsonl (override with env GOSSIPMESH_RESULTS), so
the Phase 1.1 file results/v2_results.jsonl is never mixed with Phase 2 rows.

Experiments
  loss      false SUSPECT / DEAD verdicts when every node drops p% of its packets and
            nobody crashed, for each guard mode (off | legacy | health).
  lossconv  THE trade-off test: same loss, but E really crashes. Measures how long
            detection takes AND how many false DEAD verdicts hit the nodes that are
            alive. A guard that simply never suspects anyone scores 0 on `loss`;
            this experiment is what exposes it.
  adapt     ablation of the controller: --adapt interval | fanout | both, against
            fixed-base. Convergence (crash -> all survivors DEAD) and bandwidth.
  trace     feedback-loop study: loss switches on after --onset seconds; every node's
            interval / fanout / health / traffic is sampled once per second and saved
            to results/trace_*.jsonl for plotting. Summary rows go to the results file.
  analyze   rebuild results/summary_phase2.md from the results file.

Examples (all of these also work with --nodes 20 --workers 1)
  python3 eval_p2.py loss     --loss 0.3 --trials 5
  python3 eval_p2.py lossconv --loss 0.3 --trials 5
  python3 eval_p2.py adapt    --trials 10 --bw-trials 3
  python3 eval_p2.py trace    --loss 0.3 --trials 2
  python3 eval_p2.py analyze
"""
import argparse
import json
import os
import re
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
# must be set BEFORE importing eval_v2, which reads it at import time
os.environ.setdefault("GOSSIPMESH_RESULTS", os.path.join(HERE, "results", "phase2_results.jsonl"))
sys.path.insert(0, HERE)
import eval_v2 as ev  # noqa: E402

GUARDS = ["off", "legacy", "health"]
# Phase 3 variants (env sets applied to every node). Only loss / lossconv / trace honour them.
VARIANTS = {
    "base": {},
    "buddy": {"GOSSIPMESH_BUDDY": "1"},
    "conc": {"GOSSIPMESH_CONCURRENT": "1"},
    "fix": {"GOSSIPMESH_BUDDY": "1", "GOSSIPMESH_CONCURRENT": "1"},
    "st10": {"GOSSIPMESH_SUSPECT_TIMEOUT": "10.5"},   # control: constant long timeout instead of a guard
}
ACTIVE_VARIANT = "base"
ADAPTS = ["interval", "fanout", "both"]
def summary_file(variant="base"):
    return os.path.join(ev.RESULTS_DIR, "summary_phase2.md" if variant == "base" else f"summary_phase2_{variant}.md")


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def env_for(loss=None, fault_after=None, guard=None, adapt=None):
    e = dict(VARIANTS[ACTIVE_VARIANT])
    if loss is not None:
        e["GOSSIPMESH_LOSS"] = str(loss)
    if fault_after is not None:
        e["GOSSIPMESH_FAULT_AFTER"] = str(fault_after)
    if guard:
        e["GOSSIPMESH_GUARD_MODE"] = guard
    if adapt:
        e["GOSSIPMESH_ADAPT"] = adapt
    return e


def label(mode, adapt=None):
    """Display label used in result rows: 'adaptive/interval', 'fixed-base', ..."""
    return f"adaptive/{adapt}" if mode == "adaptive" and adapt else mode


# ----------------------------------------------------------------------
# experiments (each returns a dict merged into one result row)
# ----------------------------------------------------------------------
def trial_loss(base_port, mode, loss, guard, duration=45.0):
    c = ev.Cluster(base_port, mode, extra_env=env_for(loss=loss, fault_after=15, guard=guard))
    try:
        c.start_all()
        time.sleep(15 + duration)
        # nobody crashed, so EVERY verdict below is a false positive
        return {"value": ev._count(c, r"-> DEAD"),
                "false_dead": ev._count(c, r"-> DEAD"),
                "false_suspect": ev._count(c, r"-> SUSPECT"),
                "refutations": ev._count(c, r"REFUTING"),
                "orig_suspect": ev._count(c, r"unreachable \(direct\+indirect failed\) -> SUSPECT"),
                "dead_pairs": _dead_pairs(c, None),
                "guard_engaged": ev._count(c, r"OVERLOAD GUARD engaged")}
    finally:
        c.stop()


def _dead_pairs(c, exclude):
    """Distinct (observer, target) pairs with at least one DEAD verdict (gossip copies and flaps collapse)."""
    pairs = set()
    for n in ev.NAMES:
        for v in re.findall(r"\] ([A-Z]) -> DEAD", c.text(n)):
            if v != exclude:
                pairs.add((n, v))
    return len(pairs)


def _last_state(txt, node):
    m = re.findall(rf"\] {node} -> (ALIVE|SUSPECT|DEAD)", txt)
    return m[-1] if m else None


def trial_lossconv(base_port, mode, loss, guard):
    """Crash E while the whole cluster is under packet loss."""
    c = ev.Cluster(base_port, mode, extra_env=env_for(loss=loss, fault_after=15, guard=guard))
    try:
        c.start_all()
        time.sleep(ev.SETTLE)
        victim = "E"
        survivors = [n for n in ev.NAMES if n != victim]
        pre = {n: c.text(n) for n in ev.NAMES}
        offset = {n: len(pre[n]) for n in ev.NAMES}
        # Phase 3 metric fix: a survivor that ALREADY held E as DEAD before the kill (a loss-induced false
        # verdict) says nothing about detection speed, so it is excluded instead of scored as t=0.
        pre_dead = {n for n in survivors if _last_state(pre[n], victim) == "DEAD"}
        eligible = [n for n in survivors if n not in pre_dead]
        seen = {}
        t0 = time.time()
        c.kill(victim)
        deadline = t0 + 1.5 * ev.detect_deadline()      # loss legitimately slows detection: allow 50% more
        while time.time() < deadline and len(seen) < len(eligible):
            time.sleep(0.25)
            for s in eligible:
                if s not in seen and re.search(rf"\] {victim} -> DEAD", c.text(s)[offset[s]:]):
                    seen[s] = time.time() - t0
        allt = {n: c.text(n) for n in ev.NAMES}
        # every node except the victim is alive for the whole run: a DEAD/SUSPECT verdict about
        # any of them (before or after the crash) is a false positive
        fd = sum(1 for t in allt.values() for v in re.findall(r"\] ([A-Z]) -> DEAD", t) if v != victim)
        fs = sum(1 for t in allt.values() for v in re.findall(r"\] ([A-Z]) -> SUSPECT", t) if v != victim)
        fs += sum(1 for t in allt.values()
                  for v in re.findall(r"\] ([A-Z]) unreachable \(direct\+indirect failed\) -> SUSPECT", t) if v != victim)
        complete = bool(eligible) and len(seen) == len(eligible)
        return {"value": max(seen.values()) if complete else None,
                "first": min(seen.values()) if seen else None,
                "detected": len(seen), "survivors": len(survivors), "excluded_pre_dead": len(pre_dead),
                "eligible": len(eligible),
                # per-node typical detection time: unlike `value` (max over eligible nodes) it does not
                # grow with the number of eligible nodes, so arms with 2 and 19 eligible are comparable
                "node_median": round(statistics.median(seen.values()), 2) if seen else None,
                "orig_suspect": sum(len(re.findall(r"unreachable \(direct\+indirect failed\) -> SUSPECT", t)) for t in allt.values()),
                "dead_pairs": _dead_pairs(c, victim),
                "false_dead_others": fd, "false_suspect_others": fs,
                "guard_engaged": ev._count(c, r"OVERLOAD GUARD engaged")}
    finally:
        c.stop()


def trial_convergence(base_port, mode, scenario, adapt):
    c = ev.Cluster(base_port, mode, extra_env=env_for(adapt=adapt))
    try:
        c.start_all()
        time.sleep(ev.SETTLE)
        victims = ["E"] if scenario == "single" else ["E", "C"]
        survivors = [n for n in ev.NAMES if n not in victims]
        offset = {n: len(c.text(n)) for n in ev.NAMES}
        t0 = time.time()
        c.kill("E")
        if scenario == "double":
            time.sleep(1.0)
            c.kill("C")
        need = {(s, v) for s in survivors for v in victims}
        seen = {}
        deadline = t0 + ev.detect_deadline()
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


def trial_bandwidth(base_port, mode, adapt, calm=30.0):
    c = ev.Cluster(base_port, mode, extra_env=env_for(adapt=adapt))
    steady = ["A", "B", "D"]
    try:
        c.start_all()
        time.sleep(ev.SETTLE)
        s0 = {n: c.stats(n) for n in steady}
        time.sleep(calm)
        s1 = {n: c.stats(n) for n in steady}
        c.kill("E"); time.sleep(6)
        c.start("E"); time.sleep(4)
        c.kill("C"); time.sleep(6)
        c.start("C"); time.sleep(4)
        s2 = {n: c.stats(n) for n in steady}
        if any(v is None for d in (s0, s1, s2) for v in d.values()):
            return {"value": None}
        calm_r = [ev._delta_rate(s0[n], s1[n]) for n in steady]
        churn_r = [ev._delta_rate(s1[n], s2[n]) for n in steady]
        avg = lambda rs, k: sum(r[k] for r in rs) / len(rs)
        return {"calm_msgs_s": avg(calm_r, "sent"), "calm_bytes_s": avg(calm_r, "bytes"),
                "churn_msgs_s": avg(churn_r, "sent"), "churn_bytes_s": avg(churn_r, "bytes"),
                "value": avg(calm_r, "sent")}
    finally:
        c.stop()


def _rate(samples, a, b):
    rs = [x for x in samples if a <= x["t"] <= b]
    if len(rs) < 2 or rs[-1]["t"] <= rs[0]["t"]:
        return None
    return (rs[-1]["sent"] - rs[0]["sent"]) / (rs[-1]["t"] - rs[0]["t"])


def trial_trace(base_port, mode, loss, guard, adapt, trial, onset=30.0, total=100.0):
    """Loss switches on `onset` s after start. Sample every node once per second."""
    c = ev.Cluster(base_port, mode, extra_env=env_for(loss=loss, fault_after=onset, guard=guard, adapt=adapt))
    out_rows = []
    try:
        t_start = time.time()
        c.start_all()
        while time.time() - t_start < total:
            time.sleep(1.0)
            for n in ev.NAMES:
                s = c.stats(n)
                if s:
                    out_rows.append({"t": s["t"], "t_rel": round(s["t"] - t_start, 2), "node": n,
                                     "interval": s["interval"], "fanout": s["fanout"],
                                     "health": s.get("health"), "degraded": s.get("degraded"),
                                     "overloaded": s.get("overloaded"), "churn": s.get("churn"),
                                     "sent": s["sent"], "bytes": s["bytes"]})
        false_dead = ev._count(c, r"-> DEAD")
        guard_engaged = ev._count(c, r"OVERLOAD GUARD engaged")
    finally:
        c.stop()

    os.makedirs(ev.RESULTS_DIR, exist_ok=True)
    fname = f"trace_{ev.N_NODES}n_{mode}_{guard}_{adapt}_loss{loss:g}_t{trial}.jsonl"
    with open(os.path.join(ev.RESULTS_DIR, fname), "w") as fh:
        for r in out_rows:
            fh.write(json.dumps(r) + "\n")

    by = {}
    for r in out_rows:
        by.setdefault(r["node"], []).append(r)

    def cluster_rate(a, b):
        v = [x for x in (_rate(by[n], t_start + a, t_start + b) for n in by) if x is not None]
        return sum(v) / len(v) if v else None

    pre = cluster_rate(max(0.0, onset - 10.0), onset)   # the 10 s before the loss switches on
    post = cluster_rate(total - 30.0, total)
    peak, t = 0.0, onset
    while t + 5.0 <= total:
        m = cluster_rate(t, t + 5.0)
        peak = max(peak, m or 0.0)
        t += 5.0
    tail = [r for r in out_rows if r["t_rel"] >= total - 30.0]
    mean = lambda k: (sum(float(r[k] or 0) for r in tail) / len(tail)) if tail else None
    return {"value": post, "pre_msgs_s": pre, "post_msgs_s": post, "peak_msgs_s": peak,
            "post_fanout": mean("fanout"), "post_interval": mean("interval"),
            "post_health": mean("health"), "post_degraded_frac": mean("degraded"),
            "false_dead": false_dead, "guard_engaged": guard_engaged, "trace_file": fname}


# ----------------------------------------------------------------------
# job builders
# ----------------------------------------------------------------------
def jobs_loss(modes, guards, losses, trials):
    jobs = []
    for i in range(trials):
        for p in losses:
            for m in modes:
                for g in guards:
                    jobs.append(({"exp": "loss2", "mode": m, "config": f"loss={p:g}", "guard": g, "trial": i},
                                 lambda b, m=m, p=p, g=g: trial_loss(b, m, p, g)))
    return jobs


def jobs_lossconv(modes, guards, losses, trials):
    jobs = []
    for i in range(trials):
        for p in losses:
            for m in modes:
                for g in guards:
                    jobs.append(({"exp": "lossconv", "mode": m, "config": f"loss={p:g}", "guard": g, "trial": i},
                                 lambda b, m=m, p=p, g=g: trial_lossconv(b, m, p, g)))
    return jobs


def jobs_adapt(adapts, scenarios, trials, bw_trials):
    jobs = []
    variants = [("adaptive", a) for a in adapts] + [("fixed-base", None)]
    for i in range(trials):
        for sc in scenarios:
            for m, a in variants:
                jobs.append(({"exp": "adapt_conv", "mode": label(m, a), "config": sc, "trial": i},
                             lambda b, m=m, sc=sc, a=a: trial_convergence(b, m, sc, a)))
    for i in range(bw_trials):
        for m, a in variants:
            jobs.append(({"exp": "adapt_bw", "mode": label(m, a), "config": "calm+churn", "trial": i},
                         lambda b, m=m, a=a: trial_bandwidth(b, m, a)))
    return jobs


def jobs_trace(modes, guards, adapts, losses, trials, onset, total):
    jobs = []
    for i in range(trials):
        for p in losses:
            for m in modes:
                for g in guards:
                    for a in (adapts if m == "adaptive" else ["both"]):
                        jobs.append(({"exp": "trace", "mode": m, "config": f"loss={p:g}", "guard": g,
                                      "adapt": a, "trial": i},
                                     lambda b, m=m, p=p, g=g, a=a, i=i: trial_trace(b, m, p, g, a, i, onset, total)))
    return jobs


# ----------------------------------------------------------------------
# analysis
# ----------------------------------------------------------------------
def _ms(vals):
    if not vals:
        return "-"
    sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
    return f"{statistics.mean(vals):.1f} ({sd:.1f})"


def _mean(vals, nd=1):
    return "-" if not vals else f"{statistics.mean(vals):.{nd}f}"


def _by(rows, *keys):
    g = {}
    for r in rows:
        g.setdefault(tuple(r.get(k) for k in keys), []).append(r)
    return g


def mwu_p(a, b, max_exact=200000, mc=20000):
    """Two-sided Mann-Whitney U permutation p (exact when feasible, else Monte-Carlo). No scipy needed."""
    import itertools, math, random
    n1, n2 = len(a), len(b)
    vals = list(a) + list(b)
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    ranks = [0.0] * len(vals)
    i = 0
    while i < len(order):                       # mid-ranks for ties
        j = i
        while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        for k in range(i, j + 1):
            ranks[order[k]] = (i + j) / 2 + 1
        i = j + 1
    mean = n1 * (n1 + n2 + 1) / 2
    obs = abs(sum(ranks[:n1]) - mean)
    idx = range(n1 + n2)
    hits = tot = 0
    if math.comb(n1 + n2, n1) <= max_exact:
        for c in itertools.combinations(idx, n1):
            tot += 1
            hits += abs(sum(ranks[k] for k in c) - mean) >= obs - 1e-9
    else:
        rnd = random.Random(1)
        for _ in range(mc):
            c = rnd.sample(idx, n1)
            tot += 1
            hits += abs(sum(ranks[k] for k in c) - mean) >= obs - 1e-9
    return hits / tot


def _p(a, b):
    return ev.fmt_p(mwu_p(a, b)) if len(a) > 1 and len(b) > 1 else "-"


def analyze(variant="base"):
    rows = [r for r in ev.load_rows() if r.get("variant", "base") == variant]
    out = ["# GossipMesh v2 - Phase 2 results (generated by `eval_p2.py analyze`)", "",
           "Mean (SD) over n trials. p = two-sided Mann-Whitney permutation test (no normality assumption) against the reference row named in "
           "each table. Treat p > 0.05 as *no demonstrated difference*. ~10 tests per table, no multiplicity "
           f"correction: only p < 0.005 is solid. Variant: `{variant}`.", ""]

    def order(mode):
        return ["adaptive", "fixed-base"].index(mode) if mode in ("adaptive", "fixed-base") else 9

    # ---- false verdicts under loss
    rs = [r for r in rows if r["exp"] == "loss2" and r.get("false_dead") is not None]
    if rs:
        out += ["## False verdicts under packet loss (nobody crashed; lower is better)", "",
                "Guard `off` is the reference for the p column (false DEAD).", "",
                "| cluster, loss | mode | guard | n | false DEAD | distinct DEAD pairs | false SUSPECT | originated SUSPECT | refutations | guard engaged | p vs off |",
                "|---|---|---|---|---|---|---|---|---|---|---|"]
        for (nodes, cfg, mode), grp in sorted(_by(rs, "nodes", "config", "mode").items(),
                                              key=lambda kv: (kv[0][0], kv[0][1], order(kv[0][2]))):
            ref = [r["false_dead"] for r in grp if r.get("guard") == "off"]
            for g in GUARDS:
                x = [r for r in grp if r.get("guard") == g]
                if not x:
                    continue
                fd = [r["false_dead"] for r in x]
                out.append(f"| {nodes} nodes, {cfg} | {mode} | {g} | {len(x)} | {_ms(fd)} | "
                           f"{_ms([r['dead_pairs'] for r in x if 'dead_pairs' in r])} | "
                           f"{_ms([r['false_suspect'] for r in x])} | "
                           f"{_ms([r['orig_suspect'] for r in x if 'orig_suspect' in r])} | {_ms([r['refutations'] for r in x])} | "
                           f"{_mean([r['guard_engaged'] for r in x])} | {'-' if g == 'off' else _p(fd, ref)} |")
        out.append("")

    # ---- crash under loss
    rs = [r for r in rows if r["exp"] == "lossconv"]
    if rs:
        out += ["## Crash detection under packet loss (E really crashes; read BOTH columns)", "",
                "`all detected` = share (a trial with ZERO eligible survivors counts as not detected, but that is a broken cluster view, not a detection failure; `value` is the MAX over eligible nodes so it is biased upward when many nodes are eligible: compare `per-node median` instead)  of trials in which every ELIGIBLE survivor (one that did not already hold E DEAD before the kill) logged E DEAD after it. "
                "Time is over those complete trials only. `false DEAD (alive nodes)` counts DEAD verdicts about "
                "nodes that never crashed. A guard is only better if it lowers false DEAD without costing "
                "detection. Reference for p (time) is guard `off`.", "",
                "| cluster, loss | mode | guard | n | all detected | detect time s | median s | p95 s | per-node median s | eligible | E already DEAD pre-kill (excluded) | standing DEAD share at kill | false DEAD (alive nodes) | distinct DEAD pairs | false SUSPECT (alive nodes) | guard engaged | p vs off |",
                "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for (nodes, cfg, mode), grp in sorted(_by(rs, "nodes", "config", "mode").items(),
                                              key=lambda kv: (kv[0][0], kv[0][1], order(kv[0][2]))):
            ref = [r["value"] for r in grp if r.get("guard") == "off" and r.get("value") is not None]
            for g in GUARDS:
                x = [r for r in grp if r.get("guard") == g]
                if not x:
                    continue
                t = [r["value"] for r in x if r.get("value") is not None]
                done = f"{len(t)}/{len(x)}"
                out.append(f"| {nodes} nodes, {cfg} | {mode} | {g} | {len(x)} | {done} | {_ms(t)} | "
                           f"{(f'{statistics.median(t):.1f}' if t else '-')} | "
                           f"{(f'{ev.p95(t):.1f}' if t else '-')} | "
                           f"{_ms([r['node_median'] for r in x if r.get('node_median') is not None])} | "
                           f"{_mean([r['eligible'] for r in x if 'eligible' in r])} | "
                           f"{_mean([r.get('excluded_pre_dead', 0) for r in x])} | "
                           f"{100 * statistics.mean([r.get('excluded_pre_dead', 0) / max(1, r.get('survivors', 19)) for r in x]):.0f}% | "
                           f"{_ms([r['false_dead_others'] for r in x])} | "
                           f"{_ms([r['dead_pairs'] for r in x if 'dead_pairs' in r])} | "
                           f"{_ms([r['false_suspect_others'] for r in x])} | "
                           f"{_mean([r['guard_engaged'] for r in x])} | {'-' if g == 'off' else _p(t, ref)} |")
        out.append("")

    # ---- adapt ablation
    rs = [r for r in rows if r["exp"] == "adapt_conv"]
    if rs:
        out += ["## Controller ablation: convergence (s, crash -> all survivors DEAD)", "",
                "Reference for p is `adaptive/both` (the full controller).", "",
                "| cluster, scenario | variant | n | mean (SD) | p95 | failed | p vs adaptive/both |",
                "|---|---|---|---|---|---|---|"]
        vorder = ["adaptive/interval", "adaptive/fanout", "adaptive/both", "fixed-base"]
        for (nodes, cfg), grp in sorted(_by(rs, "nodes", "config").items()):
            ref = [r["value"] for r in grp if r["mode"] == "adaptive/both" and r.get("value") is not None]
            for v in vorder:
                x = [r for r in grp if r["mode"] == v]
                if not x:
                    continue
                t = [r["value"] for r in x if r.get("value") is not None]
                out.append(f"| {nodes} nodes, {cfg} | {v} | {len(t)} | {_ms(t)} | "
                           f"{(f'{ev.p95(t):.1f}' if t else '-')} | {len(x) - len(t)} | "
                           f"{'-' if v == 'adaptive/both' else _p(t, ref)} |")
        out.append("")
    rs = [r for r in rows if r["exp"] == "adapt_bw" and r.get("calm_msgs_s")]
    if rs:
        out += ["## Controller ablation: bandwidth per node (never-killed nodes A, B, D)", "",
                "| cluster | variant | n | calm msgs/s | calm B/s | churn msgs/s | churn B/s | calm saving vs fixed-base |",
                "|---|---|---|---|---|---|---|---|"]
        vorder = ["adaptive/interval", "adaptive/fanout", "adaptive/both", "fixed-base"]
        for (nodes,), grp in sorted(_by(rs, "nodes").items()):
            base = [r["calm_msgs_s"] for r in grp if r["mode"] == "fixed-base"]
            bc = statistics.mean(base) if base else None
            for v in vorder:
                x = [r for r in grp if r["mode"] == v]
                if not x:
                    continue
                m = lambda k: statistics.mean(r[k] for r in x)
                save = "-" if bc is None or v == "fixed-base" else f"{100 * (1 - m('calm_msgs_s') / bc):+.0f}%"
                out.append(f"| {nodes} nodes | {v} | {len(x)} | {m('calm_msgs_s'):.2f} | {m('calm_bytes_s'):.0f} | "
                           f"{m('churn_msgs_s'):.2f} | {m('churn_bytes_s'):.0f} | {save} |")
        out.append("")

    # ---- feedback-loop traces
    rs = [r for r in rows if r["exp"] == "trace" and r.get("post_msgs_s") is not None]
    if rs:
        out += ["## Feedback-loop traces (loss switches on at the onset; per-node means across the cluster)", "",
                "`peak` is the highest 5 s traffic after the onset; `after` is the last 30 s. peak >> after "
                "means the controller escalated and then calmed down; after >> before with a high fanout means "
                "it is still escalating (the cascade). Raw per-second data is in the trace_*.jsonl files.", "",
                "| cluster, loss | mode | guard | adapt | n | msgs/s before | peak | after | fanout after | interval after s | health after | share degraded | false DEAD |",
                "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for (nodes, cfg, mode, guard, adapt), x in sorted(_by(rs, "nodes", "config", "mode", "guard", "adapt").items(),
                                                          key=lambda kv: (kv[0][0], kv[0][1], order(kv[0][2]), kv[0][3], kv[0][4])):
            g = lambda k, nd=2: _mean([r[k] for r in x if r.get(k) is not None], nd)
            out.append(f"| {nodes} nodes, {cfg} | {mode} | {guard} | {adapt} | {len(x)} | {g('pre_msgs_s')} | "
                       f"{g('peak_msgs_s')} | {g('post_msgs_s')} | {g('post_fanout')} | {g('post_interval')} | "
                       f"{g('post_health')} | {g('post_degraded_frac')} | {_mean([r['false_dead'] for r in x], 0)} |")
        out.append("")

    text = "\n".join(out)
    os.makedirs(ev.RESULTS_DIR, exist_ok=True)
    with open(summary_file(variant), "w") as fh:
        fh.write(text + "\n")
    print(text)


# ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["loss", "lossconv", "adapt", "trace", "analyze"])
    ap.add_argument("--modes", nargs="+", choices=["adaptive", "fixed-base", "fixed-slow", "fixed-fast"],
                    default=["adaptive", "fixed-base"])
    ap.add_argument("--guards", nargs="+", choices=GUARDS, default=GUARDS)
    ap.add_argument("--adapts", nargs="+", choices=ADAPTS, default=ADAPTS,
                    help="adapt / trace: which controller variants to run")
    ap.add_argument("--scenario", choices=["single", "double", "both"], default="single", help="adapt only")
    ap.add_argument("--loss", type=float, nargs="+", default=[0.3])
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--bw-trials", type=int, default=3, help="adapt only: bandwidth trials per variant")
    ap.add_argument("--onset", type=float, default=30.0, help="trace only: seconds before loss switches on")
    ap.add_argument("--duration", type=float, default=100.0, help="trace only: total seconds per trial")
    ap.add_argument("--variant", choices=list(VARIANTS), default="base",
                    help="Phase 3 protocol variant for loss / lossconv / trace (base = Phase 2 behaviour)")
    ap.add_argument("--nodes", type=int, default=5, help="cluster size 5..26. Use --workers 1 for 20 nodes.")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--port-base", type=int, default=20000)
    a = ap.parse_args()

    global ACTIVE_VARIANT
    if a.cmd == "analyze":
        return analyze(a.variant)
    if a.variant != "base" and a.cmd == "adapt":
        ap.error("--variant is not supported by the adapt stage")
    ACTIVE_VARIANT = a.variant
    ev.set_nodes(a.nodes)
    if a.nodes > 10 and a.workers > 1:
        print(f"WARNING: {a.nodes} nodes x {a.workers} workers competes for CPU and can itself cause "
              f"false failures. Use --workers 1.", flush=True)
    scen = ["single", "double"] if a.scenario == "both" else [a.scenario]
    if a.cmd == "loss":
        jobs = jobs_loss(a.modes, a.guards, a.loss, a.trials)
    elif a.cmd == "lossconv":
        jobs = jobs_lossconv(a.modes, a.guards, a.loss, a.trials)
    elif a.cmd == "adapt":
        jobs = jobs_adapt(a.adapts, scen, a.trials, a.bw_trials)
    else:
        jobs = jobs_trace(a.modes, a.guards, a.adapts, a.loss, a.trials, a.onset, a.duration)
    for meta, _fn in jobs:
        meta["variant"] = a.variant
    print(f"running {len(jobs)} trials on {a.workers} workers, {a.nodes} nodes each, "
          f"results -> {ev.RESULTS_FILE}", flush=True)
    ev.run_jobs(jobs, a.workers, a.port_base)
    analyze(a.variant)


if __name__ == "__main__":
    main()
