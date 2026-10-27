# Change log

## v2 - Phase 2
- `node.py`: `--guard-mode off|legacy|health` (new local-health guard: EWMA of direct-probe failures + self-refutations, hysteresis, retries before suspecting, stretched suspicion timers, controller held at baseline) and `--adapt both|interval|fanout` (controller ablation). Defaults reproduce Phase 1.1. `_protocol_round` split into `_probe_target` + round. Stats file exports `health`, `degraded`, `churn`, `guard_mode`, `adapt`.
- `eval_p2.py` (new, `eval_v2.py` untouched): experiments `loss`, `lossconv` (crash under loss: detection time AND false DEAD), `adapt`, `trace`; results in `results/phase2_results.jsonl`, summary in `results/summary_phase2.md`.
- `run_phase2.py` (new): staged runner with time estimate. `PHASE2.md` explains the design and how to read the tables.

## v2 — Phase 1.1 (scale)
- `eval_v2.py`: `--nodes N` (5 to 26). Node names, settle time (25 s + 1 s per extra node) and the detection deadline (35 s + 3 s per extra node) scale with N. Every result row records `nodes`; `analyze` groups by cluster size.
- Fixed: nodes were started 0.15 s apart, so at 20 nodes the last node bound its socket after the first node's 2 s warm-up (spacing now 0.05 s).
- Fixed: convergence scanned each node's whole log, so a false DEAD logged *before* the crash could be counted as a detection. Only text after the crash is read now.
- Fixed: `_count()` captured the node list at import time (`nodes=NAMES` default argument), so counts ignored `--nodes`.
- New: `run_scale.py`, `RUNBOOK.md`, `PHASE1.md`. Results env override `GOSSIPMESH_RESULTS`.
- `node.py` is unchanged from Phase 1 (byte-identical), so earlier results remain comparable.

## v2 — Phase 1 (this version)

### node.py
- **Idle back-off.** With zero churn for a full 8 s window the gossip interval ramps from 1.5 s to 3.0 s over 10 s. v1 could only speed up.
- **Fast attack / slow release.** Interval drops immediately on churn (and the sleeping loop is woken early); it grows back at most 25% per round. Fanout moves by at most ±1 per round (cooldown).
- **Overload guard** (the cascade fix). If ≥ max(3, 75% of live peers) distinct targets fail probes within 6 s, the node assumes the problem is local: it stops declaring SUSPECT, postpones pending suspicion timers (max 2 times), and freezes the controller at baseline. Engages for 5 s after the last trigger. `--no-guard` disables it for ablation.
- **Looser timeouts:** probe 0.4 → 1.0 s, indirect 0.6 → 1.5 s, suspect 2.0 → 3.5 s (env-overridable: `GOSSIPMESH_PROBE_TIMEOUT`, `_INDIRECT_TIMEOUT`, `_SUSPECT_TIMEOUT`). **v1 and v2 timings are not comparable.**
- **Counters:** messages, bytes, per-type counts, drops; `--stats-file` writes them as JSON every 0.5 s; status lines show `sent=` and `bytes=`.
- **Fault injection:** `--loss`, `--block-ports`, `--fault-after`, `--no-indirect`.
- **Four modes:** `adaptive`, `fixed-slow` (3.0 s, k=1), `fixed-base` (1.5 s, k=1), `fixed-fast` (0.5 s, k=3). `GOSSIPMESH_FIXED=1` still means `fixed-base`.

### eval_v2.py (new)
- Experiments: convergence (single/double crash), bandwidth (calm vs churn), loss (false verdicts, with ablations), partition (A↔B link cut, indirect on/off).
- Parallel trials on separate port blocks, interleaved modes, JSONL results, Markdown summary with mean/SD/p95/Welch p.

### other
- `run_demo.py`: wait after the crash raised 6 → 12 s for the looser timeouts.
- `eval_convergence.py` left as the v1 harness (legacy).

## v1 (previous zip)
- Fix: restarted nodes stayed DEAD (JOIN stored at incarnation 0). JOIN now carries the incarnation and merges by it.
- `GOSSIPMESH_FIXED=1` baseline; `eval_convergence.py` with single/double scenarios; 12 s settle.
- Preliminary v1 results (n=6): single 6.61 s vs 6.38 s (p=0.80); double 7.36 s vs 8.94 s (p=0.11).
