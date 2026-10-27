"""
GossipMesh demo driver.

Launches a 5-node cluster on localhost, lets it settle, then:
  1. Kills node D (SIGKILL) to simulate a crash.
  2. Waits and shows the rest of the cluster detecting it via
     direct+indirect probing, marking it SUSPECT then DEAD, and the
     adaptive controller speeding up gossip while churn is happening.
  3. Restarts node D to demonstrate self-healing rejoin (no manual
     operator step — it just re-JOINs and the cluster re-integrates it).

Run:  python3 run_demo.py
Logs: gossipmesh/logs/<ID>.log  (tail -f them in separate terminals for
      the live viva demo, or just read the merged console output below)
"""

import subprocess
import sys
import time
import os

HERE = os.path.dirname(os.path.abspath(__file__))
LOGDIR = os.path.join(HERE, "logs")
os.makedirs(LOGDIR, exist_ok=True)

NODES = {"A": 9001, "B": 9002, "C": 9003, "D": 9004, "E": 9005}
BOOTSTRAP = ",".join(f"{nid}={port}" for nid, port in NODES.items())

procs = {}


def start(node_id, extra_seed=None):
    port = NODES[node_id]
    log_path = os.path.join(LOGDIR, f"{node_id}.log")
    log_f = open(log_path, "w")
    cmd = [sys.executable, "-u", os.path.join(HERE, "node.py"),
           "--id", node_id, "--port", str(port)]
    if node_id == "A":
        cmd += ["--bootstrap", BOOTSTRAP]
    else:
        cmd += ["--seed", f"127.0.0.1:{NODES['A']}"]
    if extra_seed:
        cmd += ["--seed", extra_seed]
    p = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT)
    procs[node_id] = (p, log_f)
    print(f"started {node_id} (pid={p.pid}) -> {log_path}")


def stop(node_id):
    # .kill() is cross-platform (SIGKILL on Unix, TerminateProcess on Windows) —
    # signal.SIGKILL itself doesn't exist on Windows, so don't reference it directly.
    p, log_f = procs[node_id]
    p.kill()
    print(f"killed {node_id} (pid={p.pid})")


def tail(node_id, n=6):
    path = os.path.join(LOGDIR, f"{node_id}.log")
    if not os.path.exists(path):
        return
    with open(path) as f:
        lines = f.readlines()[-n:]
    print(f"--- {node_id} (last {len(lines)} lines) ---")
    print("".join(lines))


def main():
    print("=== GossipMesh demo: bringing up 5-node cluster ===")
    start("A")
    time.sleep(0.3)
    for nid in ["B", "C", "D", "E"]:
        start(nid)
        time.sleep(0.2)

    print("\nletting the cluster settle for 6s...\n")
    time.sleep(6)
    for nid in NODES:
        tail(nid, 2)

    print("\n=== Simulating a crash: killing node D ===\n")
    stop("D")
    time.sleep(12)   # Phase 1 timeouts are looser (probe 1.0 + indirect 1.5 + suspect 3.5 s)
    print("\n--- state after crash (watch churn_rate / interval / fanout react) ---")
    for nid in ["A", "B", "C", "E"]:
        tail(nid, 6)

    print("\n=== Self-healing rejoin: restarting node D ===\n")
    start("D")
    time.sleep(5)
    print("\n--- state after rejoin (D should be ALIVE again everywhere) ---")
    for nid in NODES:
        tail(nid, 4)

    print("\n=== Demo complete. Ctrl+C to stop, or leave running and inspect logs/*.log ===")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for nid, (p, log_f) in procs.items():
            p.kill()
            log_f.close()


if __name__ == "__main__":
    main()
