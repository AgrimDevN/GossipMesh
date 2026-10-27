# GossipMesh v2 - Phase 2

Everything here is built on what the 20-node run (Phase 1.1) showed:

| Phase 1.1 finding (20 nodes) | Phase 2 response |
|---|---|
| Adaptive = fixed-base on detection speed (12.18 vs 12.18 s double, p = 1.0), but about half the idle traffic | `--adapt` ablation: which half of the controller earns the saving? |
| 30% loss: ~480 false DEAD in every mode, guard never engaged (0.0) | Replace the guard trigger (`--guard-mode health`) and test it against crashes, not only against false alarms |
| Guard on/off made no difference (489 vs 503 false DEAD) | Same: the old trigger needed 15 distinct failed targets inside 6 s, impossible at 20 nodes |
| Adaptive had slightly more false SUSPECT (590 vs 354) | `trace` experiment shows whether the controller escalates under loss |

## What changed in `node.py`

Defaults are unchanged: `--guard-mode legacy --adapt both` behaves exactly as Phase 1.1 in the protocol path,
so the Phase 1.1 results stay comparable. Phase 2 behaviour is opt-in until the data says it should be the default.

**`--guard-mode off | legacy | health`** (`--no-guard` still means off)
- `legacy`: the Phase 1 guard, untouched.
- `health` (new). Signal: EWMA (weight 0.25) of *direct-probe* failures, plus one failure sample each time this
  node has to refute a suspicion about itself. Engages at 0.35, releases at 0.15 (hysteresis). Independent of cluster
  size. While degraded the node:
  1. retries the whole probe (direct + indirect) up to 2 more times before suspecting anyone;
  2. stretches the suspicion timers it arms by x3;
  3. holds the controller at its baseline interval/fanout (no escalation while its own view is bad).

  It never suppresses a verdict outright. A real crash is still detected, only later. That is the design
  difference from `legacy`, and `lossconv` below is the test of whether it works.
- Tunables (env): `GOSSIPMESH_HEALTH_ALPHA`, `_HEALTH_ENGAGE`, `_HEALTH_RELEASE`, `_HEALTH_RETRIES`, `_DEGRADED_SUSPECT_MULT`.

**`--adapt both | interval | fanout`**: restrict the controller to the interval half, the fanout half, or both.
Interval-only also keeps the early-wake on churn; fanout-only turns it off (it is an interval mechanism).

**Stats file** now also exports `health`, `degraded`, `churn`, `guard_mode`, `adapt`.

`_protocol_round` was split into `_probe_target` + the round itself. The probe logic is the same, same order of
random calls.

## The harness: `eval_p2.py` (does not touch `eval_v2.py`)

Results go to `results/phase2_results.jsonl`, never into `v2_results.jsonl`.

| command | question it answers |
|---|---|
| `lossconv` | With 30% loss AND a real crash: how long does detection take, and how many false DEAD hit the nodes that are alive? **Read both columns.** `off` is the reference. A guard that never suspects scores zero false DEAD in `loss` but fails here. |
| `loss` | False verdicts under loss only (comparable to the Phase 1.1 table), now per guard mode. |
| `adapt` | interval-only vs fanout-only vs both vs fixed-base: convergence and bandwidth. |
| `trace` | Loss switches on at 30 s. Per-second interval, fanout, health, traffic for every node saved to `trace_*.jsonl`; summary row has traffic before / peak / after. This is the feedback-loop (cascade) study. |

## How to run

```
python3 run_phase2.py --nodes 20 --estimate        # plan and time, nothing runs
python3 run_phase2.py --nodes 5 --quick            # 1 trial per cell, checks it works on your machine
python3 run_phase2.py --nodes 20                   # all four stages, about 2.4 h
python3 run_phase2.py --nodes 20 --stages lossconv # the single most important stage, about 36 min
```
Stop with Ctrl+C any time; re-running only appends. Send back `results/summary_phase2.md` and
`results/phase2_results.jsonl`. Keep the same rules as before: quiet machine, plugged in, do not edit `node.py`
between runs, do not mix files from different runs.

## Not done (still in PHASE2_PLAN.md)
Real network faults with `tc netem` (5), persisted incarnation (6), idle-detection fix (7), registry baseline (8).
Suspicion *confirmation* (a verdict needs more than one independent suspector) is not in the plan, and not built.
From reading the code it is probably what limits any local fix under uniform loss: a single false SUSPECT is
gossiped and every node that hears it starts its own timer. That is a hypothesis; the traces and `lossconv`
numbers will say whether it matters.
