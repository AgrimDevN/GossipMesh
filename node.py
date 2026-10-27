"""
GossipMesh v2 — Adaptive Gossip-Based Cluster Membership Protocol (Phase 2)
===========================================================================

A SWIM-style membership layer with these extensions:

1. Indirect probing (inherited from SWIM) to reduce false positives caused
   by asymmetric network reachability. Can be switched off for ablation.
2. Adaptive gossip controller (this project's contribution): the gossip
   interval and the number of indirect probers ("fanout") are recomputed
   every round from a measured churn-rate metric.

   Phase 1 changes to the controller:
     * IDLE BACK-OFF  - with zero churn the interval ramps from 1.5 s up to
       3.0 s, so a calm cluster really is quieter than the fixed baseline.
     * FAST ATTACK / SLOW RELEASE - the interval drops immediately when churn
       appears (and the sleeping loop is woken early), but climbs back
       gradually; the fanout moves by at most +/-1 per round (cooldown).
     * OVERLOAD GUARD - if a node sees most of its peers fail probes at the
       same time, it concludes the problem is probably local (overload or
       partition), stops declaring SUSPECT, and freezes the controller at its
       baseline instead of escalating. This is the fix for the "cascade"
       (every node declaring every other node dead) seen on Windows.
3. Looser default timeouts (probe 1.0 s / indirect 1.5 s / suspect 3.5 s).
4. Instrumentation: message and byte counters (JSON stats file).
5. Fault injection: random packet loss, link cuts (partition), and an
   indirect-probing on/off switch.
6. Four selectable modes: adaptive, and three fixed baselines
   (fixed-slow, fixed-base, fixed-fast).

Phase 2 additions (every default keeps the Phase 1.1 behaviour byte-for-byte
in the protocol path, so earlier results stay comparable):

7. --guard-mode off | legacy | health
     legacy  the Phase 1 guard ("N distinct failed targets in 6 s"). Default.
     health  local-health guard. Signal = EWMA of DIRECT-probe failures (plus a
             failure sample every time this node has to refute a suspicion
             about itself), with separate engage/release thresholds
             (hysteresis). Independent of cluster size. While degraded the node
             (a) retries the whole probe before suspecting anyone, (b) stretches
             the suspicion timers it arms, (c) freezes the controller at its
             baseline. It never suppresses a verdict outright, so a real crash
             is still detected, only later.
     off     no guard (same as --no-guard).
8. --adapt both | interval | fanout   ablation of the controller: adapt only the
   gossip interval, only the indirect fanout, or both (default).
9. Stats file also exports health, degraded, churn, guard_mode, adapt so the
   harness can trace the controller as a feedback loop.

Run one instance per process:
    python3 node.py --id A --port 9001 --bootstrap A=9001,B=9002,C=9003
    python3 node.py --id B --port 9002 --seed 127.0.0.1:9001 --mode fixed-base

Every node prints its membership table + current adaptive parameters every
3 s, so a live demo just means showing several terminals side by side and
killing/restarting one process.
"""

import asyncio
import argparse
import json
import random
import sys
import time
import math
import os
from collections import deque

# ---------------------------------------------------------------------
# Protocol constants (tunable bounds for the adaptive controller)
# ---------------------------------------------------------------------
ALIVE, SUSPECT, DEAD = "ALIVE", "SUSPECT", "DEAD"


def _envf(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


BASE_GOSSIP_INTERVAL = 1.5      # seconds, steady-state (low churn)
MIN_GOSSIP_INTERVAL = 0.3       # seconds, under heavy churn
MAX_GOSSIP_INTERVAL = 3.0       # seconds, fully idle cluster (idle back-off)

BASE_FANOUT = 1                 # indirect probers, steady-state
MAX_FANOUT = 4                  # indirect probers, under churn

# Phase 1: looser than v1 (0.4 / 0.6 / 2.0). Env vars allow sweeps.
PROBE_TIMEOUT = _envf("GOSSIPMESH_PROBE_TIMEOUT", 1.0)        # direct ACK
INDIRECT_TIMEOUT = _envf("GOSSIPMESH_INDIRECT_TIMEOUT", 1.5)  # relayed ACK
SUSPECT_TIMEOUT = _envf("GOSSIPMESH_SUSPECT_TIMEOUT", 3.5)    # SUSPECT -> DEAD

CHURN_WINDOW = 8.0              # sliding window (s) used for churn-rate calc
GOSSIP_PIGGYBACK_MAX = 6        # updates attached per message

# Idle back-off / slow release
IDLE_RAMP = 10.0                # s of zero churn (after the window) to reach MAX
RELEASE_FACTOR = 1.25           # interval may grow by at most 25% per round

# Overload guard
OVERLOAD_WINDOW = 6.0           # s over which distinct failed targets are counted
OVERLOAD_MIN_TARGETS = 3        # never trigger on fewer distinct failed targets
OVERLOAD_FRACTION = 0.75        # ...or on less than this share of live peers
OVERLOAD_HOLD = 5.0             # s the guard stays engaged after last trigger
MAX_SUSPECT_EXTENSIONS = 2      # times a suspicion timer may be postponed (legacy guard)

# Phase 2: local-health guard ("health" guard mode)
HEALTH_ALPHA = _envf("GOSSIPMESH_HEALTH_ALPHA", 0.25)       # EWMA weight of the newest probe sample
HEALTH_ENGAGE = _envf("GOSSIPMESH_HEALTH_ENGAGE", 0.35)     # degraded when EWMA >= this
HEALTH_RELEASE = _envf("GOSSIPMESH_HEALTH_RELEASE", 0.15)   # healthy again when EWMA <= this
HEALTH_RETRIES = int(_envf("GOSSIPMESH_HEALTH_RETRIES", 2)) # extra full probes before suspecting, while degraded
DEGRADED_SUSPECT_MULT = _envf("GOSSIPMESH_DEGRADED_SUSPECT_MULT", 3.0)  # suspicion-timer stretch while degraded
# Phase 3 (opt-in, defaults unchanged so Phase 2 results stay comparable)
BUDDY = _envf("GOSSIPMESH_BUDDY", 0) > 0            # tell the suspect directly; a direct ACK from a SUSPECT clears it locally
CONCURRENT = _envf("GOSSIPMESH_CONCURRENT", 0) > 0  # probe rounds run as independent tasks on a fixed period
GUARD_MODES = ["off", "legacy", "health"]
ADAPT_MODES = ["both", "interval", "fanout"]

# Fixed baselines: name -> (interval, fanout). "adaptive" is the controller.
FIXED_MODES = {
    "fixed-slow": (3.0, 1),     # conservative / quietest
    "fixed-base": (1.5, 1),     # = v1 baseline (old GOSSIPMESH_FIXED=1)
    "fixed-fast": (0.5, 3),     # aggressive
}
MODES = ["adaptive"] + list(FIXED_MODES)


def now():
    return time.time()


class Node:
    def __init__(self, node_id, host, port, seed_addr=None, bootstrap=None, log_fn=print,
                 mode="adaptive", loss=0.0, block_ports=(), indirect=True, guard=True,
                 stats_file=None, fault_after=0.0, guard_mode="legacy", adapt="both"):
        self.id = node_id
        self.addr = (host, port)
        self.log = log_fn

        if mode not in MODES:
            raise ValueError(f"unknown mode {mode!r}; choose from {MODES}")
        self.mode = mode
        self.indirect_enabled = indirect
        if guard_mode not in GUARD_MODES:
            raise ValueError(f"unknown guard mode {guard_mode!r}; choose from {GUARD_MODES}")
        if adapt not in ADAPT_MODES:
            raise ValueError(f"unknown adapt mode {adapt!r}; choose from {ADAPT_MODES}")
        self.guard_mode = guard_mode if guard else "off"   # guard=False (--no-guard) always means off
        self.guard_enabled = self.guard_mode != "off"
        self.adapt = adapt
        self.stats_file = stats_file

        # fault injection (applied in _send / datagram_received)
        self.loss = loss
        self.block_ports = set(block_ports)
        self.fault_after = fault_after   # faults only switch on this many s after start

        # incarnation is time-based so a restarted process always outranks
        # whatever the cluster last believed about this node (self-healing).
        self.incarnation = int(now())

        # membership table: id -> {addr, state, incarnation}
        self.members = {}
        if bootstrap:
            for bid, baddr in bootstrap.items():
                if bid == self.id:
                    continue
                self.members[bid] = {"addr": baddr, "state": ALIVE, "incarnation": 0}

        self.seed_addr = seed_addr

        # gossip piggyback queue: id -> times_sent (lower = higher priority)
        self.gossip_send_count = {}

        # churn tracking for the adaptive controller
        self.churn_events = deque()  # timestamps of state transitions
        self.last_churn = now()      # for idle back-off
        self._wake = asyncio.Event() # wakes a sleeping round early on churn

        # overload guard state
        self.fail_targets = {}       # target_id -> time of last failed probe
        self.overloaded_until = 0.0
        self._suspect_extensions = {}

        # local-health state (always tracked, only acted on in guard mode "health")
        self.health = 0.0            # EWMA of direct-probe failures, 0 (fine) .. 1 (nothing gets through)
        self.degraded = False        # hysteresis flag derived from self.health

        # bookkeeping for in-flight probes
        self._pending_acks = {}       # seq -> asyncio.Future
        self._seq = 0
        self._suspect_tasks = {}      # id -> asyncio.Task (suspicion timers)
        self._round_tasks = set()     # in-flight probe rounds (CONCURRENT mode)

        # instrumentation
        self.stats = {"sent": 0, "bytes": 0, "recv": 0, "dropped_out": 0,
                      "dropped_in": 0, "by_type": {}, "false_suspect": 0}
        self.started_at = now()

        self.transport = None
        if mode == "adaptive":
            self.gossip_interval, self.fanout = BASE_GOSSIP_INTERVAL, BASE_FANOUT
        else:
            self.gossip_interval, self.fanout = FIXED_MODES[mode]

    # ------------------------------------------------------------------
    # Networking plumbing
    # ------------------------------------------------------------------
    async def start(self):
        loop = asyncio.get_running_loop()
        self.transport, _ = await loop.create_datagram_endpoint(
            lambda: _UDP(self), local_addr=self.addr
        )
        self.log(f"[{self.id}] listening on {self.addr[0]}:{self.addr[1]} "
                 f"(incarnation={self.incarnation} mode={self.mode} "
                 f"loss={self.loss} block={sorted(self.block_ports)} "
                 f"indirect={self.indirect_enabled} guard={self.guard_mode} adapt={self.adapt})")

        self._queue_update(self.id)  # announce our (time-based) incarnation

        if self.seed_addr and self.seed_addr != self.addr:
            await self._join(self.seed_addr)

        asyncio.create_task(self._gossip_loop())
        asyncio.create_task(self._status_loop())
        if self.stats_file:
            asyncio.create_task(self._stats_loop())

    def _faults_on(self):
        return now() - self.started_at >= self.fault_after

    def _send(self, msg, addr):
        data = json.dumps(msg).encode()
        mtype = msg.get("type", "?")
        # count at the sender BEFORE fault injection: bandwidth = what we tried to send
        self.stats["sent"] += 1
        self.stats["bytes"] += len(data)
        self.stats["by_type"][mtype] = self.stats["by_type"].get(mtype, 0) + 1
        addr = tuple(addr)
        if self._faults_on() and (addr[1] in self.block_ports or
                                  (self.loss > 0 and random.random() < self.loss)):
            self.stats["dropped_out"] += 1
            return
        self.transport.sendto(data, addr)

    def _next_seq(self):
        self._seq += 1
        return self._seq

    # ------------------------------------------------------------------
    # Adaptive controller: churn-rate -> (gossip_interval, fanout)
    # ------------------------------------------------------------------
    def _record_churn(self):
        t = now()
        self.churn_events.append(t)
        self.last_churn = t
        if self.mode == "adaptive" and self.adapt != "fanout":
            self._wake.set()  # fast attack: don't sleep through a burst (interval adaptation)

    def _churn_rate(self):
        cutoff = now() - CHURN_WINDOW
        while self.churn_events and self.churn_events[0] < cutoff:
            self.churn_events.popleft()
        return len(self.churn_events) / CHURN_WINDOW  # events/sec

    def _legacy_overloaded(self):
        return self.guard_mode == "legacy" and now() < self.overloaded_until

    def _is_degraded(self):
        return self.guard_mode == "health" and self.degraded

    def _is_overloaded(self):
        """True while the guard (either flavour) says our own view is untrustworthy."""
        return self._legacy_overloaded() or self._is_degraded()

    def _suspect_mult(self):
        """Suspicion-timer stretch: only the health guard stretches (legacy postpones instead)."""
        return DEGRADED_SUSPECT_MULT if self._is_degraded() else 1.0

    def _recompute_adaptive_params(self):
        rate = self._churn_rate()
        if self.mode != "adaptive":
            # baseline for evaluation: adaptation disabled, constants only
            self.gossip_interval, self.fanout = FIXED_MODES[self.mode]
            return rate

        if self._is_overloaded():
            # do NOT escalate while we suspect the problem is local
            target_interval, target_fanout = BASE_GOSSIP_INTERVAL, BASE_FANOUT
        elif rate > 0:
            # more churn -> shorter interval, more fanout. Saturates at bounds.
            target_interval = max(MIN_GOSSIP_INTERVAL,
                                  BASE_GOSSIP_INTERVAL / (1.0 + 4.0 * rate))
            target_fanout = min(MAX_FANOUT, BASE_FANOUT + math.floor(rate * 6))
        else:
            # idle back-off: zero churn for a full window, then ramp to MAX
            quiet = now() - self.last_churn - CHURN_WINDOW
            frac = max(0.0, min(1.0, quiet / IDLE_RAMP))
            target_interval = BASE_GOSSIP_INTERVAL + frac * (MAX_GOSSIP_INTERVAL - BASE_GOSSIP_INTERVAL)
            target_fanout = BASE_FANOUT

        # Phase 2 ablation: restrict which half of the controller is allowed to move
        if self.adapt == "interval":
            target_fanout = BASE_FANOUT
        elif self.adapt == "fanout":
            target_interval = BASE_GOSSIP_INTERVAL

        # interval: fast attack (drop at once), slow release (grow <= 25%/round)
        if target_interval <= self.gossip_interval:
            self.gossip_interval = target_interval
        else:
            self.gossip_interval = min(target_interval, self.gossip_interval * RELEASE_FACTOR)
        self.gossip_interval = max(MIN_GOSSIP_INTERVAL, min(MAX_GOSSIP_INTERVAL, self.gossip_interval))

        # fanout: cooldown, moves by at most 1 per round in either direction
        if target_fanout > self.fanout:
            self.fanout += 1
        elif target_fanout < self.fanout:
            self.fanout -= 1
        self.fanout = max(BASE_FANOUT, min(MAX_FANOUT, self.fanout))
        return rate

    # ------------------------------------------------------------------
    # Overload guard: "legacy" (Phase 1 heuristic) and "health" (Phase 2)
    # ------------------------------------------------------------------
    def _health_sample(self, failed):
        """Feed one sample (1 = this node's own traffic failed) into the health EWMA.

        Samples: every first-attempt DIRECT probe (failed = no ACK in PROBE_TIMEOUT),
        and every time this node has to refute a suspicion about itself (that means
        peers could not hear it). Hysteresis: engage at HEALTH_ENGAGE, release at
        HEALTH_RELEASE, so the flag does not flap around a single threshold.
        """
        self.health = (1.0 - HEALTH_ALPHA) * self.health + HEALTH_ALPHA * (1.0 if failed else 0.0)
        if self.guard_mode != "health":
            return
        if not self.degraded and self.health >= HEALTH_ENGAGE:
            self.degraded = True
            self.log(f"[{self.id}] OVERLOAD GUARD engaged (health): direct-probe failure EWMA "
                     f"{self.health:.2f} >= {HEALTH_ENGAGE:g}; retrying probes before suspecting, "
                     f"suspicion timers x{DEGRADED_SUSPECT_MULT:g}, controller held at baseline")
        elif self.degraded and self.health <= HEALTH_RELEASE:
            self.degraded = False
            self.log(f"[{self.id}] OVERLOAD GUARD released (health): EWMA {self.health:.2f} "
                     f"<= {HEALTH_RELEASE:g}")

    def _note_probe_failure(self, target_id):
        """Record a failed probe; (legacy guard) engage if most live peers are failing."""
        t = now()
        self.fail_targets[target_id] = t
        cutoff = t - OVERLOAD_WINDOW
        self.fail_targets = {k: v for k, v in self.fail_targets.items() if v >= cutoff}
        peers = sum(1 for m in self.members.values() if m["state"] != DEAD)
        threshold = max(OVERLOAD_MIN_TARGETS, math.ceil(OVERLOAD_FRACTION * peers))
        if self.guard_mode == "legacy" and len(self.fail_targets) >= threshold:
            if not self._legacy_overloaded():
                self.log(f"[{self.id}] OVERLOAD GUARD engaged: {len(self.fail_targets)}/{peers} "
                         f"peers failed probes within {OVERLOAD_WINDOW:.0f}s -> assuming local "
                         f"problem, holding suspicions and controller at baseline")
            self.overloaded_until = t + OVERLOAD_HOLD

    def _note_probe_success(self, target_id):
        self.fail_targets.pop(target_id, None)

    # ------------------------------------------------------------------
    # Gossip payload helpers
    # ------------------------------------------------------------------
    def _queue_update(self, node_id):
        self.gossip_send_count[node_id] = 0

    def _pick_updates(self):
        limit = max(3, math.ceil(3 * math.log2(len(self.members) + 2)))
        items = sorted(self.gossip_send_count.items(), key=lambda kv: kv[1])[:GOSSIP_PIGGYBACK_MAX]
        payload = []
        for node_id, count in items:
            entry = self.members.get(node_id)
            if node_id == self.id:
                entry = {"addr": list(self.addr), "state": ALIVE, "incarnation": self.incarnation}
            if entry is None:
                continue
            payload.append([node_id, entry["state"], entry["incarnation"], list(entry["addr"])])
            self.gossip_send_count[node_id] += 1
            if self.gossip_send_count[node_id] >= limit:
                del self.gossip_send_count[node_id]
        return payload

    def _merge_update(self, node_id, state, incarnation, addr):
        if node_id == self.id:
            # someone thinks I'm suspect/dead -> refute by bumping incarnation
            if state in (SUSPECT, DEAD) and incarnation >= self.incarnation:
                self.incarnation += 1
                self.stats["false_suspect"] += 1
                self._health_sample(True)   # peers could not hear us: that is a local-health signal
                self._queue_update(self.id)
                self.log(f"[{self.id}] REFUTING false suspicion -> incarnation {self.incarnation}")
            return

        existing = self.members.get(node_id)
        precedence = {ALIVE: 0, SUSPECT: 1, DEAD: 2}

        if existing is None:
            self.members[node_id] = {"addr": addr, "state": state, "incarnation": incarnation}
            self._queue_update(node_id)
            self._record_churn()  # a join (or a rumour about an unknown node) is churn
            if state == ALIVE:
                self.log(f"[{self.id}] learned about new member {node_id} ({addr[0]}:{addr[1]})")
            if state == SUSPECT:
                self._arm_suspect_timer(node_id)
            return

        newer = incarnation > existing["incarnation"]
        same_inc_higher_state = (incarnation == existing["incarnation"] and
                                  precedence[state] > precedence[existing["state"]])

        if newer or same_inc_higher_state:
            changed_state = existing["state"] != state
            existing["state"], existing["incarnation"] = state, incarnation
            self._queue_update(node_id)
            if changed_state:
                self._record_churn()
                self.log(f"[{self.id}] {node_id} -> {state} (incarnation {incarnation})")
                if state == SUSPECT:
                    self._arm_suspect_timer(node_id)
                elif state == ALIVE and node_id in self._suspect_tasks:
                    self._suspect_tasks[node_id].cancel()
                    del self._suspect_tasks[node_id]

    def _arm_suspect_timer(self, node_id):
        if node_id in self._suspect_tasks:
            self._suspect_tasks[node_id].cancel()
        self._suspect_extensions[node_id] = 0
        self._suspect_tasks[node_id] = asyncio.create_task(self._suspect_timeout(node_id))

    async def _suspect_timeout(self, node_id):
        try:
            await asyncio.sleep(SUSPECT_TIMEOUT * self._suspect_mult())
            # legacy guard: while engaged our own view is not trustworthy,
            # postpone the verdict (bounded) instead of declaring DEAD.
            # (the health guard stretches the timer above instead)
            while (self._legacy_overloaded() and
                   self._suspect_extensions.get(node_id, 0) < MAX_SUSPECT_EXTENSIONS):
                self._suspect_extensions[node_id] = self._suspect_extensions.get(node_id, 0) + 1
                self.log(f"[{self.id}] {node_id} suspicion postponed (overload guard, "
                         f"{self._suspect_extensions[node_id]}/{MAX_SUSPECT_EXTENSIONS})")
                await asyncio.sleep(SUSPECT_TIMEOUT)
        except asyncio.CancelledError:
            return
        entry = self.members.get(node_id)
        if entry and entry["state"] == SUSPECT:
            entry["state"] = DEAD
            self._queue_update(node_id)
            self._record_churn()
            self.log(f"[{self.id}] {node_id} -> DEAD (suspicion timeout expired)")

    # ------------------------------------------------------------------
    # Join / bootstrap
    # ------------------------------------------------------------------
    async def _join(self, seed_addr):
        seq = self._next_seq()
        fut = asyncio.get_event_loop().create_future()
        self._pending_acks[seq] = fut
        self._send({"type": "JOIN", "id": self.id, "addr": list(self.addr), "seq": seq,
                    "incarnation": self.incarnation}, seed_addr)
        try:
            snapshot = await asyncio.wait_for(fut, timeout=2.0)
            for node_id, state, incarnation, addr in snapshot:
                self._merge_update(node_id, state, incarnation, addr)
            self.log(f"[{self.id}] joined cluster via {seed_addr}, "
                     f"{len(self.members)} known peers")
        except asyncio.TimeoutError:
            self.log(f"[{self.id}] JOIN to {seed_addr} timed out (seed unreachable at boot)")

    # ------------------------------------------------------------------
    # Main protocol round
    # ------------------------------------------------------------------
    async def _gossip_loop(self):
        # Warm-up grace period: give every node in the cluster time to bind
        # its socket and complete its JOIN handshake before anyone starts
        # probing. Without this, a node launched a fraction of a second
        # later than its peers can get falsely marked SUSPECT/DEAD simply
        # because it wasn't listening yet when the first round fired.
        await asyncio.sleep(2.0)
        while True:
            t0 = now()
            rate = self._recompute_adaptive_params()
            if CONCURRENT:
                # fixed protocol period: a slow/retrying round no longer delays the next probe
                task = asyncio.create_task(self._safe_round(rate))
                self._round_tasks.add(task)
                task.add_done_callback(self._round_tasks.discard)
            else:
                await self._protocol_round(rate)
            # sleep one interval, but wake early if churn shows up (adaptive only);
            # never run rounds closer together than MIN_GOSSIP_INTERVAL.
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.gossip_interval)
            except asyncio.TimeoutError:
                pass
            spare = MIN_GOSSIP_INTERVAL - (now() - t0)
            if spare > 0:
                await asyncio.sleep(spare)

    async def _safe_round(self, rate):
        try:
            await self._protocol_round(rate)
        except Exception as exc:   # a failed round must never kill the loop
            self.log(f"[{self.id}] protocol round error: {exc!r}")

    def _notice(self, target_id, entry):
        """Tell the suspect itself that it is suspected, so it can refute at once (buddy system)."""
        self._send({"type": "NOTICE", "id": self.id,
                    "updates": [[target_id, SUSPECT, entry["incarnation"], list(entry["addr"])]]},
                   tuple(entry["addr"]))

    def _clear_suspicion(self, target_id):
        """Direct ACK from a node we hold as SUSPECT: that is first-hand evidence it is alive."""
        entry = self.members.get(target_id)
        if not entry or entry["state"] != SUSPECT:
            return
        self._notice(target_id, entry)            # make it bump its incarnation so everyone else clears too
        task = self._suspect_tasks.pop(target_id, None)
        if task:
            task.cancel()
        entry["state"] = ALIVE
        self.log(f"[{self.id}] {target_id} -> ALIVE (direct ack while suspect)")

    async def _probe_target(self, target_id, target, candidates):
        """One full probe: direct ping, then indirect probing via `fanout` relays.

        Returns (reached, direct_ok). Identical to the Phase 1 logic, factored out
        so the health guard can repeat it.
        """
        direct_ok = await self._ping(target_id, target["addr"])
        if direct_ok:
            return True, True
        if self.indirect_enabled:
            relays = [nid for nid in candidates if nid != target_id]
            random.shuffle(relays)
            relays = relays[: self.fanout]
            if relays:
                results = await asyncio.gather(
                    *[self._ping_req(r, self.members[r]["addr"], target_id, target["addr"])
                      for r in relays]
                )
                if any(results):
                    return True, False
        return False, False

    async def _protocol_round(self, churn_rate):
        candidates = [nid for nid, m in self.members.items() if m["state"] != DEAD]
        if not candidates:
            return
        target_id = random.choice(candidates)
        target = self.members[target_id]

        reached, direct_ok = await self._probe_target(target_id, target, candidates)
        self._health_sample(not direct_ok)       # first attempt only: retries must not skew the signal
        if reached:
            self._note_probe_success(target_id)
            if BUDDY and direct_ok:
                self._clear_suspicion(target_id)
            return

        # health guard: while our own view is degraded, gather more evidence first.
        if self._is_degraded():
            for attempt in range(1, HEALTH_RETRIES + 1):
                reached, _ = await self._probe_target(target_id, target, candidates)
                if reached:
                    self._note_probe_success(target_id)
                    self.log(f"[{self.id}] {target_id} answered on retry {attempt} "
                             f"(health guard, EWMA {self.health:.2f}); not suspecting")
                    return
                if not self._is_degraded():       # recovered while retrying: stop spending probes
                    break

        # both direct and indirect probing failed
        self._note_probe_failure(target_id)
        if self._legacy_overloaded():
            self.log(f"[{self.id}] {target_id} unreachable but overload guard is engaged "
                     f"-> NOT suspecting (likely a local problem)")
            return

        # -> suspect, don't kill outright
        entry = self.members.get(target_id)
        if entry and entry["state"] == ALIVE:
            entry["state"] = SUSPECT
            self._queue_update(target_id)
            self._record_churn()
            self.log(f"[{self.id}] {target_id} unreachable (direct+indirect failed) -> SUSPECT "
                     f"[churn_rate={churn_rate:.2f}/s interval={self.gossip_interval:.2f}s "
                     f"fanout={self.fanout}]")
            self._arm_suspect_timer(target_id)
            if BUDDY:
                self._notice(target_id, entry)

    async def _ping(self, target_id, addr):
        seq = self._next_seq()
        fut = asyncio.get_event_loop().create_future()
        self._pending_acks[seq] = fut
        self._send({"type": "PING", "id": self.id, "seq": seq,
                     "updates": self._pick_updates()}, tuple(addr))
        try:
            await asyncio.wait_for(fut, timeout=PROBE_TIMEOUT)
            return True
        except asyncio.TimeoutError:
            self._pending_acks.pop(seq, None)
            return False

    async def _ping_req(self, relay_id, relay_addr, target_id, target_addr):
        seq = self._next_seq()
        fut = asyncio.get_event_loop().create_future()
        self._pending_acks[seq] = fut
        self._send({"type": "PING_REQ", "id": self.id, "seq": seq,
                     "target_id": target_id, "target_addr": list(target_addr)},
                    tuple(relay_addr))
        try:
            await asyncio.wait_for(fut, timeout=INDIRECT_TIMEOUT)
            return True
        except asyncio.TimeoutError:
            self._pending_acks.pop(seq, None)
            return False

    # ------------------------------------------------------------------
    # Inbound message handling (called from the UDP protocol class)
    # ------------------------------------------------------------------
    def on_message(self, msg, from_addr):
        mtype = msg.get("type")

        for node_id, state, incarnation, addr in msg.get("updates", []):
            self._merge_update(node_id, state, incarnation, addr)

        if mtype == "PING":
            self._send({"type": "ACK", "seq": msg["seq"], "id": self.id,
                         "updates": self._pick_updates()}, from_addr)

        elif mtype == "ACK":
            fut = self._pending_acks.pop(msg["seq"], None)
            if fut and not fut.done():
                fut.set_result(True)

        elif mtype == "PING_REQ":
            target_addr = tuple(msg["target_addr"])
            asyncio.create_task(self._relay_ping(msg["seq"], from_addr, target_addr))

        elif mtype == "PING_REQ_ACK":
            fut = self._pending_acks.pop(msg["seq"], None)
            if fut and not fut.done():
                fut.set_result(True)

        elif mtype == "JOIN":
            new_id, new_addr = msg["id"], tuple(msg["addr"])
            new_inc = msg.get("incarnation", 0)
            # merge by incarnation: a restarted node (higher incarnation) outranks
            # stale SUSPECT/DEAD beliefs; a stale JOIN cannot resurrect anything.
            self._merge_update(new_id, ALIVE, new_inc, new_addr)
            if new_id in self.members:
                self.members[new_id]["addr"] = new_addr
            self._record_churn()
            self.log(f"[{self.id}] {new_id} JOINED at {new_addr[0]}:{new_addr[1]} "
                     f"(incarnation {new_inc})")
            snapshot = [[nid, m["state"], m["incarnation"], list(m["addr"])]
                        for nid, m in self.members.items() if nid != new_id]
            snapshot.append([self.id, ALIVE, self.incarnation, list(self.addr)])
            self._send({"type": "JOIN_ACK", "seq": msg["seq"], "snapshot": snapshot}, from_addr)

        elif mtype == "JOIN_ACK":
            fut = self._pending_acks.pop(msg["seq"], None)
            if fut and not fut.done():
                fut.set_result(msg["snapshot"])

    async def _relay_ping(self, orig_seq, requester_addr, target_addr):
        seq = self._next_seq()
        fut = asyncio.get_event_loop().create_future()
        self._pending_acks[seq] = fut
        self._send({"type": "PING", "id": self.id, "seq": seq, "updates": []}, target_addr)
        try:
            await asyncio.wait_for(fut, timeout=INDIRECT_TIMEOUT * 0.8)
            self._send({"type": "PING_REQ_ACK", "seq": orig_seq}, requester_addr)
        except asyncio.TimeoutError:
            self._pending_acks.pop(seq, None)  # no ack forwarded -> requester times out naturally

    # ------------------------------------------------------------------
    # Status printout (used live during the demo) and stats export
    # ------------------------------------------------------------------
    async def _status_loop(self):
        while True:
            await asyncio.sleep(3)
            table = ", ".join(f"{nid}:{m['state']}" for nid, m in sorted(self.members.items()))
            guard = " GUARD" if self._is_overloaded() else ""
            self.log(f"[{self.id}] view=[{table}]  "
                     f"interval={self.gossip_interval:.2f}s fanout={self.fanout} "
                     f"churn={self._churn_rate():.2f}/s health={self.health:.2f} "
                     f"sent={self.stats['sent']} bytes={self.stats['bytes']}{guard}")

    def snapshot_stats(self):
        s = dict(self.stats)
        s.update({"t": now(), "id": self.id, "mode": self.mode,
                  "interval": self.gossip_interval, "fanout": self.fanout,
                  "overloaded": self._is_overloaded(), "health": self.health,
                  "degraded": self._is_degraded(), "churn": self._churn_rate(),
                  "guard_mode": self.guard_mode, "adapt": self.adapt})
        return s

    async def _stats_loop(self):
        tmp = self.stats_file + ".tmp"
        while True:
            try:
                with open(tmp, "w") as f:
                    json.dump(self.snapshot_stats(), f)
                os.replace(tmp, self.stats_file)  # atomic: harness never reads a half file
            except OSError:
                pass
            await asyncio.sleep(0.5)


class _UDP(asyncio.DatagramProtocol):
    def __init__(self, node: Node):
        self.node = node

    def datagram_received(self, data, addr):
        if addr[1] in self.node.block_ports and self.node._faults_on():  # injected link cut (inbound)
            self.node.stats["dropped_in"] += 1
            return
        try:
            msg = json.loads(data.decode())
        except Exception:
            return
        self.node.stats["recv"] += 1
        self.node.on_message(msg, addr)


def parse_bootstrap(s):
    out = {}
    if not s:
        return out
    for part in s.split(","):
        nid, port = part.split("=")
        out[nid] = ("127.0.0.1", int(port))
    return out


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--seed", default=None, help="host:port of an existing member")
    ap.add_argument("--bootstrap", default=None,
                     help="ID=port,ID=port,... static initial view (first launch of the cluster)")
    ap.add_argument("--mode", default=None, choices=MODES,
                     help="adaptive | fixed-slow (3.0s,k=1) | fixed-base (1.5s,k=1) | "
                          "fixed-fast (0.5s,k=3). Env GOSSIPMESH_MODE also works; "
                          "legacy GOSSIPMESH_FIXED=1 means fixed-base.")
    ap.add_argument("--loss", type=float, default=None,
                     help="probability [0..1] of dropping each outbound packet "
                          "(env GOSSIPMESH_LOSS)")
    ap.add_argument("--block-ports", default=None,
                     help="comma-separated peer UDP ports whose link is cut in both "
                          "directions at this node (env GOSSIPMESH_BLOCK_PORTS)")
    ap.add_argument("--no-indirect", action="store_true",
                     help="disable PING-REQ indirect probing (env GOSSIPMESH_NO_INDIRECT=1)")
    ap.add_argument("--no-guard", action="store_true",
                     help="disable the overload guard (env GOSSIPMESH_NO_GUARD=1)")
    ap.add_argument("--guard-mode", default=None, choices=GUARD_MODES,
                     help="off | legacy (Phase 1 heuristic, default) | health (Phase 2 local-health "
                          "guard). Env GOSSIPMESH_GUARD_MODE. --no-guard always wins and means off.")
    ap.add_argument("--adapt", default=None, choices=ADAPT_MODES,
                     help="both (default) | interval | fanout: which half of the controller may "
                          "move (ablation). Env GOSSIPMESH_ADAPT.")
    ap.add_argument("--fault-after", type=float, default=None,
                     help="seconds after start before --loss/--block-ports take effect "
                          "(env GOSSIPMESH_FAULT_AFTER); lets the cluster settle first")
    ap.add_argument("--stats-file", default=None,
                     help="write counters as JSON here every 0.5 s (env GOSSIPMESH_STATS_FILE)")
    args = ap.parse_args()

    # Belt-and-braces: force unbuffered stdout even if this script is ever
    # launched without the `-u` flag (e.g. a different orchestrator), so
    # log output is visible immediately instead of sitting in a buffer
    # until the process exits.
    sys.stdout.reconfigure(line_buffering=True)

    env = os.environ
    mode = args.mode or env.get("GOSSIPMESH_MODE") or ("fixed-base" if env.get("GOSSIPMESH_FIXED") else "adaptive")
    loss = args.loss if args.loss is not None else _envf("GOSSIPMESH_LOSS", 0.0)
    bp = args.block_ports if args.block_ports is not None else env.get("GOSSIPMESH_BLOCK_PORTS", "")
    block_ports = [int(x) for x in bp.split(",") if x.strip()]
    indirect = not (args.no_indirect or env.get("GOSSIPMESH_NO_INDIRECT"))
    guard = not (args.no_guard or env.get("GOSSIPMESH_NO_GUARD"))
    guard_mode = args.guard_mode or env.get("GOSSIPMESH_GUARD_MODE") or "legacy"
    adapt = args.adapt or env.get("GOSSIPMESH_ADAPT") or "both"
    if guard_mode not in GUARD_MODES or adapt not in ADAPT_MODES:
        ap.error(f"bad GOSSIPMESH_GUARD_MODE={guard_mode!r} or GOSSIPMESH_ADAPT={adapt!r}")
    stats_file = args.stats_file or env.get("GOSSIPMESH_STATS_FILE")
    fault_after = args.fault_after if args.fault_after is not None else _envf("GOSSIPMESH_FAULT_AFTER", 0.0)

    seed_addr = None
    if args.seed:
        h, p = args.seed.split(":")
        seed_addr = (h, int(p))

    bootstrap = parse_bootstrap(args.bootstrap)

    node = Node(args.id, args.host, args.port, seed_addr=seed_addr, bootstrap=bootstrap,
                mode=mode, loss=loss, block_ports=block_ports, indirect=indirect,
                guard=guard, stats_file=stats_file, fault_after=fault_after,
                guard_mode=guard_mode, adapt=adapt)
    await node.start()
    await asyncio.Event().wait()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
