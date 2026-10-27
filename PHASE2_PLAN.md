# Phase 2 plan

Status: items 0, 2, 3 (EWMA + hysteresis part), 4 and the trace tooling for 3 are built, see PHASE2.md. Items 1, 5, 6, 7, 8 are not.
The loop-stability *analysis* in item 3 needs the trace data; the tooling exists, the analysis does not.

Order is by value for the report. Each item says what it settles.

0. **Replace the overload guard trigger (found at 20 nodes).** A node sees only 2 to 4 probes in the guard's 6 s window, so
   "3 distinct failed targets" almost never fires, and at 20 nodes the 75% rule (15 targets) cannot fire at all. Use a rate over
   the last K probes (for example failures among the last 8 probes) or Lifeguard's local health score, independent of cluster size.

1. **Statistical power.** 20+ trials per cell, report mean and p95, cluster sizes 5, 10, 20. Settles whether any Phase 1 trend is real. (Harness already supports it; needs `--trials 20` and a quiet machine.)
2. **Ablation: interval-only vs fanout-only adaptation.** Add `--adapt interval|fanout|both`. Settles which half of the controller does the work.
3. **Stability analysis of the cascade.** Replace the guard's hard threshold with an EWMA-smoothed churn signal plus hysteresis (separate engage/release thresholds), then study the controller as a feedback loop: when does "more churn → more traffic → more timeouts → more churn" diverge? This is the most original angle in the project.
4. **Lifeguard-style local health multiplier.** Churn says how hard the *cluster* should gossip; a local health score (missed ACKs, late ACKs) says whether *this node's timeouts* can be trusted. Replace the guard heuristic with it and compare.
5. **Real faults.** Run with `tc netem` (loss, delay, reorder) or network namespaces instead of in-process injection; reproduce and then fix the Windows cascade on the Windows machine.
6. **Persisted incarnation** instead of wall-clock (small file per node id), removing the clock-sync assumption.
7. **Idle detection fix.** Phase 1 shows idle back-off slows the *first* detection. Candidate: when idle, probe more targets per round at the long interval, or randomise back-off per node so the cluster isn't uniformly slow.
8. **Registry baseline** (etcd) or an explicit "future work" statement.
