"""
Run the Phase 2 evaluation, one stage at a time or all of them.

    python3 run_phase2.py --nodes 20 --estimate          # print the plan and time estimate only
    python3 run_phase2.py --nodes 20                     # all four stages (about 2.5 h at 20 nodes)
    python3 run_phase2.py --nodes 20 --stages lossconv   # just one stage
    python3 run_phase2.py --nodes 5 --quick              # 1 trial per cell, to check it works (a few minutes)

Stages (in order of value for the report):
  lossconv  crash detection AND false DEAD under 30% loss, guard off / legacy / health
  loss      false verdicts under 30% loss, same guard modes
  adapt     controller ablation: interval-only vs fanout-only vs both vs fixed-base
  trace     feedback-loop traces (loss switches on at 30 s), saved for plotting

Results are appended to results/phase2_results.jsonl and results/summary_phase2.md is rebuilt after
every stage, so you can stop (Ctrl+C) at any time and re-run: it only appends. Always 1 worker.
"""
import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ALL = ["lossconv", "loss", "adapt", "trace"]

ap = argparse.ArgumentParser()
ap.add_argument("--nodes", type=int, default=20)
ap.add_argument("--stages", nargs="+", choices=ALL, default=ALL)
ap.add_argument("--loss", type=float, default=0.3)
ap.add_argument("--trials", type=int, default=5, help="trials per cell for lossconv and loss")
ap.add_argument("--adapt-trials", type=int, default=10, help="convergence trials per variant")
ap.add_argument("--bw-trials", type=int, default=3)
ap.add_argument("--trace-trials", type=int, default=2)
ap.add_argument("--quick", action="store_true", help="1 trial per cell: only checks that everything runs")
ap.add_argument("--estimate", action="store_true", help="print the plan and exit")
a = ap.parse_args()
if a.quick:
    a.trials = a.adapt_trials = a.bw_trials = a.trace_trials = 1

n = a.nodes
settle = 25.0 + max(0, n - 5)
deadline = 35.0 + 3.0 * max(0, n - 5)
# rough wall-clock seconds per trial (measured at 20 nodes: loss 62 s, convergence 52 s, bandwidth 92 s)
t_loss = 15 + 45 + 2
t_lossconv = settle + 0.35 * deadline + 5          # detection under loss is slower than clean: rough guess
t_conv = settle + 12 + 5
t_bw = settle + 30 + 20 + 6
t_trace = 100 + max(0, n - 5) + 2

plan = {
    "lossconv": ("lossconv", 2 * 3 * a.trials, t_lossconv, ["lossconv", "--loss", str(a.loss), "--trials", str(a.trials)]),
    "loss": ("loss", 2 * 3 * a.trials, t_loss, ["loss", "--loss", str(a.loss), "--trials", str(a.trials)]),
    "adapt": ("adapt", 4 * a.adapt_trials, t_conv, ["adapt", "--trials", str(a.adapt_trials),
                                                    "--bw-trials", str(a.bw_trials)]),
    "trace": ("trace", 2 * 3 * a.trace_trials, t_trace, ["trace", "--loss", str(a.loss), "--adapts", "both",
                                                      "--trials", str(a.trace_trials),
                                                      "--onset", str(30 + max(0, n - 5)),
                                                      "--duration", str(100 + max(0, n - 5))]),
}
# extra cost of the adapt stage's bandwidth trials and the trace stage's extra adaptive variants
extra = {"adapt": 4 * a.bw_trials * t_bw, "trace": 0}

total = 0.0
print(f"Phase 2 plan, {n} nodes, loss {a.loss:g}:")
for s in a.stages:
    name, cells, per, _ = plan[s]
    est = cells * per + extra.get(s, 0.0)
    total += est
    print(f"  {s:<9} ~{cells:>3} trials x ~{per:>4.0f} s  = ~{est / 60:>5.0f} min")
print(f"  total     ~{total / 3600:.1f} h  (estimate; the machine, not the CPU speed, sets this: most of it is waiting)")
if a.estimate:
    sys.exit(0)

t0 = time.time()
for s in a.stages:
    _, _, _, args = plan[s]
    print(f"\n===== stage {s} ({n} nodes) =====", flush=True)
    cmd = [sys.executable, os.path.join(HERE, "eval_p2.py")] + args + ["--nodes", str(n), "--workers", "1"]
    rc = subprocess.call(cmd)
    print(f"----- stage {s} finished (exit {rc}), elapsed {(time.time() - t0) / 3600:.2f} h", flush=True)
subprocess.call([sys.executable, os.path.join(HERE, "eval_p2.py"), "analyze"])
print("\nDone. Send me results/summary_phase2.md and results/phase2_results.jsonl "
      "(the trace_*.jsonl files only if you want the plots).")
