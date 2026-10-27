# GossipMesh v2 (Phase 1) — Adaptive Gossip-Based Cluster Membership

Zero dependencies for the protocol (pure Python 3 stdlib, `asyncio` + UDP).
`scipy` is used by the analyzer for the Welch test if installed; there is a stdlib fallback.

## Files
- `node.py` — the protocol: SWIM-style membership + indirect probing + adaptive controller + self-healing rejoin, plus Phase 1 additions (idle back-off, overload guard, counters, fault injection, 4 modes).
- `eval_v2.py` — Phase 1 benchmark harness + analyzer (convergence, bandwidth, loss, partition). `--nodes N` sets the cluster size (5 to 26).
- `run_scale.py` — runs the whole suite at several cluster sizes (default 20, 10, 5) and rebuilds the summary.
- `RUNBOOK.md` — the step-by-step instructions for the overnight runs.
- `run_demo.py` — one-command demo: 5 nodes, kill D, watch detection, restart D, watch rejoin.
- `eval_convergence.py` — the v1 harness (kept for reproducing the old numbers; superseded by `eval_v2.py`).
- `PHASE1.md` — what changed and why, how to run it, how to read the results.
- `PHASE2_PLAN.md` — the advanced changes planned next.
- `CHANGES.md` — change log.
- `results/` — `smoke_5nodes.jsonl` and `smoke_20nodes.jsonl` are tiny smoke runs; your real runs append to `v2_results.jsonl` and `summary.md` is generated from it.

## Demo

```bash
python3 run_demo.py
```

Starts A–E on ports 9001–9005, kills D, shows SUSPECT → DEAD and the controller reacting, restarts D and shows it rejoining with no manual step. Note that after a long calm period the interval has backed off to 3 s, so the first detection is slower than in v1 (that is the price of the saving; see `PHASE1.md`).

Manual, separate terminals:

```bash
python3 node.py --id A --port 9001 --bootstrap A=9001,B=9002,C=9003,D=9004,E=9005
python3 node.py --id B --port 9002 --seed 127.0.0.1:9001
python3 node.py --id C --port 9003 --seed 127.0.0.1:9001      # ... and so on
```

Useful flags (all also available as `GOSSIPMESH_*` env vars):

| flag | meaning |
|---|---|
| `--mode adaptive\|fixed-slow\|fixed-base\|fixed-fast` | controller or one of three fixed baselines (3.0 s/k=1, 1.5 s/k=1, 0.5 s/k=3) |
| `--loss 0.3` | drop 30% of this node's outbound packets |
| `--block-ports 9002` | cut the link to the peer on port 9002 (both directions, at this node) |
| `--fault-after 15` | faults only start 15 s after launch |
| `--no-indirect` | disable PING-REQ indirect probing |
| `--no-guard` | disable the overload guard |
| `--stats-file f.json` | write message/byte counters every 0.5 s |

## Running the evaluation

```bash
python3 eval_v2.py suite --quick          # ~20 min smoke run of everything, small n
python3 eval_v2.py suite --trials 20      # the real run, 5 nodes (hours; use a quiet machine)
python3 eval_v2.py suite --trials 20 --nodes 20 --workers 1   # 20-node cluster (about 4.5 h)
python3 run_scale.py --sizes 20 10 5 --trials 20              # all sizes in sequence
python3 eval_v2.py convergence --scenario double --modes adaptive fixed-base fixed-fast --trials 20
python3 eval_v2.py bandwidth --trials 5
python3 eval_v2.py loss --loss 0.1 0.3 --ablate --trials 10
python3 eval_v2.py partition --trials 10
python3 eval_v2.py analyze                # rebuild results/summary.md from results/v2_results.jsonl
```

Keep `--workers` ≤ your CPU count, otherwise trials compete for CPU and the timings get noisy. At 10 or more nodes use `--workers 1`.

## Where to point during Q&A

- **Membership** (O1): `Node.members` — `state` + `incarnation` per node.
- **Gossip** (O2): `_pick_updates()` / `_merge_update()` — piggybacked, incarnation-ordered.
- **Indirect probing** (O3): `_protocol_round()`; the `--no-indirect` switch and the `partition` experiment test it.
- **Adaptive controller** (O4): `_recompute_adaptive_params()` — churn → interval/fanout with idle back-off, fast attack / slow release, fanout cooldown.
- **Overload guard**: `_note_probe_failure()` — stops the "everyone declares everyone dead" cascade.
- **Self-healing rejoin** (O5): `_join()` and the `JOIN` handler.
- **Evaluation** (O6): `eval_v2.py` with four modes, so the claim is "adaptive vs *both* a conservative and an aggressive fixed setting".

## Known limitations (say them first)
- Localhost only; losses and partitions are *injected*, not real network faults.
- Wall-clock incarnation is a shortcut (assumes loosely synced clocks); persisted counter is Phase 2.
- "Health" is still just state + incarnation; the overload guard is a simple local heuristic, not Lifeguard's health multiplier.
- The overload guard was designed for the Windows cascade but that cascade has **not been reproduced or re-tested on Windows yet**. Its trigger (3+ distinct failed targets in 6 s) is very hard to reach at normal probe rates and impossible at 20 nodes; see `PHASE1.md`.
- No registry (etcd/ZooKeeper) baseline; stated as future work, not claimed.
