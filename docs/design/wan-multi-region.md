# cw-wan — Multi-region (WAN) deployment characterization

beads: cw-cql (20ms sensitivity) · cw-sx9 (deep 0/50/100ms sweep) · branch: feat/multi-shard

## The knob

Real cross-region RTT can't be exercised on a single host, so crabscheme's cs-net
TCP transport gained a software WAN-RTT knob:

```
CW_NET_DELAY_MS=<one-way-ms>   # per-node env var; 0 = off (default)
```

It delays *inbound* frame delivery by `one-way-ms` via a `sleep_until` queue in the
reader task, preserving bandwidth (delay is applied at enqueue, not by stalling the
socket) and per-channel ordering. Set it identically on every node to emulate a
symmetric `2 × one-way` RTT between peers. Implemented in crabscheme
`crates/cs-net/src/lib.rs` (commit `5c4edf2` on `fix/cw-6m8-server-leaks`).

Reproduce: `bench/cluster-3node.sh` with `CW_NET_DELAY_MS` exported, or the sweep
harness used for the numbers below.

## Findings (conc=512, 3-node N=3 sharded; mean_lat = conc/tput·1000)

| RTT    | write tput   | write lat | read-lin tput/lat | read-ser-follower tput/lat |
|--------|--------------|-----------|-------------------|----------------------------|
| 0ms    | ~7.8–9.6k w/s| 53–65 ms  | ~15.7k / 32–33 ms | ~15.1k / 32–34 ms          |
| 50ms   | ~4.8k (−42%) | 106–113 ms| ~15.6k / 33 ms    | ~16k / 31 ms               |
| 100ms  | ~3.1k (−61%) | 157–168 ms| ~15.8k / 32 ms    | ~16k / 31 ms               |

(Two independent runs; write LAN figure is the run-to-run band. 0% fail, clean
elections at every RTT.)

## Conclusions

1. **The deployment is valid across cross-region RTT** — predictable, stable, no
   instability or lost elections at any delay. LAN write throughput reproduces at
   ~8–9.6k w/s, still beating etcd's ~7562 on the same host (confirms cw-mul
   scale-out is real, not a fluke).

2. **Write fall-off is smooth and linear, not a cliff.** Each ~50ms RTT step adds
   ≈1 RTT to mean write latency and roughly halves throughput. Group-commit
   batching + the 512 in-flight pipeline amortize but cannot hide the Raft
   quorum-RTT cost. At 100ms RTT writes hold ~39% of LAN throughput — usable but
   quorum-bound.

3. **Reads are RTT-immune.** Both linearizable (ReadIndex) and serializable-follower
   stay ~15–16k r/s and ~31–34 ms at *every* RTT — reads never pay the inter-node
   delay (ReadIndex batches many reads per heartbeat; serializable never leaves the
   local node). A read-heavy multi-region workload scales out essentially for free.

4. **Negative result — serializable does NOT win on latency under saturating load.**
   Hypothesis was that serializable-follower reads would beat linearizable under WAN;
   they did not (lin equal-or-slightly-better at every RTT). In a closed-loop batched
   harness ReadIndex amortizes the confirmation RTT so cheaply that the theoretical
   serializable edge never materializes. Its real win would only surface in a
   single-shot (conc=1) read-latency test, not under load.

## Where the write ceiling goes next

The multi-region write ceiling **is** the Raft quorum RTT (a linear cost). Batching
is already saturated, so the next lever must cut the quorum-RTT *count* — flexible /
leaderless quorums or a follower-read-friendly write path — which is a consensus-core
(`raft.scm`) change and out of scope here.
