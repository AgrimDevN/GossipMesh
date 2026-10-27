# RUNBOOK — what to do, in order

You run these commands, then send me **`results/summary.md`** and **`results/v2_results.jsonl`**.
I turn them into the final tables in the report.

## Before you start (5 minutes)

1. **Use Linux, macOS or WSL2 if you can.** The Windows cascade you saw happened on a Windows host, and 20 nodes on a
   loaded Windows laptop can produce the same effect *for machine reasons*, which would contaminate the result.
   If you must use Windows, say so in the report.
2. **Close everything heavy** (browser tabs, IDE indexing, video calls). Plug the laptop in. Disable sleep.
3. **Python 3.9+**. Optional: `pip install scipy` (the analyzer has a fallback without it).
4. `cd gossipmesh`

## Step 1 — check that the harness works on your machine (about 6 minutes)

```bash
python3 eval_v2.py convergence --scenario single --modes adaptive fixed-base --trials 1 --nodes 20 --workers 1
```

You should see two lines like `'mode': 'adaptive', ..., 'nodes': 20, 'value': 9.4` and a table.
`failed` must be `0`. If a trial prints `'value': None`, the cluster did not converge: stop and send me the output.

While it runs, open Task Manager / `htop`. **CPU should stay below ~70%.** If it sits at 100%, the machine is the
bottleneck and the timings are not trustworthy; tell me and we use fewer trials or fewer nodes.

## Step 2 — the main run: 20 nodes first (about 4.5 hours, run it overnight)

```bash
python3 run_scale.py --sizes 20 --trials 20
```

It runs, for 4 modes (adaptive, fixed-slow, fixed-base, fixed-fast):
- crash convergence, single and double crash: 20 trials per cell
- bandwidth, calm and during churn: 5 trials per mode
- packet loss (30%) and the A<->B link cut: 10 trials each, with indirect-probing and overload-guard ablations

`results/summary.md` is rebuilt at the end. It is safe to stop with Ctrl+C: finished trials stay in
`results/v2_results.jsonl`, and re-running only adds more rows.

## Step 3 — the smaller clusters (about 6 more hours; only after step 2)

```bash
python3 run_scale.py --sizes 10 5 --trials 20
```

(Use `--trials 10` if you are short on time. 10 trials gives wide error bars but still shows the direction.)

## Step 4 — send me the results

Send: `results/summary.md` and `results/v2_results.jsonl` (both small text files), plus:
- your OS, CPU model, number of cores, RAM
- whether you saw CPU at 100% at any point

## If something goes wrong

| symptom | what it means | what to do |
|---|---|---|
| `value: None` in convergence | cluster never converged within the deadline | send the console output; do not rerun blindly |
| Huge `false DEAD` counts in calm runs | machine overloaded, not the protocol | close apps, use `--workers 1`, retry that size |
| `Address already in use` | leftover node processes from an aborted run | Linux/macOS: `pkill -f node.py`; Windows: end all `python.exe` |
| Run takes much longer than estimated | slow machine | reduce `--trials` and tell me the number you used |

## Do NOT

- run with `--workers` > 1 at 10 or 20 nodes;
- edit `node.py` between runs (results from different code are not comparable);
- mix the `results/smoke_*.jsonl` files into `v2_results.jsonl`: they are tiny smoke runs, kept only as evidence that the harness works.
