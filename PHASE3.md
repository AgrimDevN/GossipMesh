# Phase 3 (opt-in; defaults reproduce Phase 2 exactly)

node.py (env flags, selected by `eval_p2.py --variant`):
- GOSSIPMESH_BUDDY=1      A1: SUSPECT is also sent to the suspect itself; a direct ACK from a node held SUSPECT cancels the timer and clears it locally.
- GOSSIPMESH_CONCURRENT=1 A4: probe rounds run as independent tasks on a fixed period (retries no longer stretch the period).
eval_p2.py:
- B1 metric fix: survivors that already held E DEAD before the kill are EXCLUDED (not scored t=0); `excluded_pre_dead` is reported.
- B3: Mann-Whitney permutation test replaces Welch; median column added.
- B6: `originated SUSPECT` and `distinct DEAD pairs` added next to the per-observer counts.
- `--variant base|buddy|conc|fix|st10` (st10 = guard off + constant SUSPECT_TIMEOUT 10.5, the missing control for the health guard).
  Rows carry `variant`; `analyze --variant X` writes results/summary_phase2_X.md. Old rows count as `base`.

Run (quiet machine, ~36 min per lossconv stage at 20 nodes, n=5; use --trials 10 for power):
  python3 eval_p2.py lossconv --nodes 20 --trials 10 --variant base  --guards off health
  python3 eval_p2.py lossconv --nodes 20 --trials 10 --variant st10  --guards off
  python3 eval_p2.py lossconv --nodes 20 --trials 10 --variant fix   --guards off health
  python3 eval_p2.py loss     --nodes 20 --trials 10 --variant fix   --guards off health
  python3 eval_p2.py lossconv --nodes 20 --trials 5 --modes fixed-slow fixed-base adaptive --guards off
Do NOT mix a base rerun with the old Phase 2 rows for detection time: the metric changed (old rows have no `excluded_pre_dead`).
Not built yet: A5 relay NACKs, A6 churn-signal filtering, A7 dynamic suspicion timeout, loss sweep, asymmetric-fault test.
