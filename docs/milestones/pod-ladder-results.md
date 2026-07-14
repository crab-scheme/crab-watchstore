# Pod Scale Ladder — crab-watchstore backed k3s cluster

Cluster: k3s v1.31.12+k3s1 (control-plane) / v1.36.2+k3s1 (5 agents, rejoined), 1 control-plane + 5 worker nodes across us-east-2 (18.219.175.139, 3.146.108.47, 18.190.228.14) and us-west-2 (34.213.13.28, 16.144.176.102).
Store: crab-watchstore (Scheme etcd-compatible store, quepaxa consensus), 5-node cluster, endpoints 18.219.175.139:2379,3.146.108.47:2379,18.190.228.14:2379,34.213.13.28:2379,16.144.176.102:2379.
Kubelet max-pods=250 per node (5 workers => theoretical ceiling 1250 schedulable pods, well above the 1000-pod rung).
Deployment: `ladder` in namespace `ladder`, image `rancher/mirrored-pause:3.6`, requests cpu=1m memory=8Mi.

## Preliminary note: cluster join incident (environment noise, not a store finding)

At initial verification (~T+2-4 min after cluster bring-up), only 2/6 nodes were Ready: control-plane + one agent (18.219.175.139). The other 4 agent nodes (3.146.108.47, 18.190.228.14, 34.213.13.28, 16.144.176.102) had `k3s-agent.service` reported "active" by systemd but were stuck in a permanent reconnect loop:
```
level=error msg="Failed to validate connection to cluster at https://18.225.114.210:6443: token CA hash does not match the Cluster CA certificate hash: b7cedbb6a03cca9a3973f9e3f5dac2fb093abe4d347d1ff09dbf237e459d1ec5 != 38319ba9bd62abc53350ad3af711f689b681644390ad3db1ae81989b5962bb35"
```
Root cause (per team lead): stale cluster CA cached in `/etc/rancher/k3s` and `/var/lib/rancher/k3s/agent` on those 4 nodes from an earlier session's experiments. Fixed by wiping that state and rejoining with the current token. Post-fix, all 6 nodes Ready; the 4 rejoined agents came back on k3s v1.36.2+k3s1 (vs control-plane's v1.31.12+k3s1) — a version skew worth flagging to infra but not treated as a store/ladder finding.

Verified before ladder start: `kubectl get nodes` 6/6 Ready; `kubectl get --raw=/readyz` = `ok`; store `etcdctl endpoint status` on all 5 store endpoints green, single leader (18.219.175.139), raft term 1, DB size ~468-471 kB, revisions 2999-3025 (baseline, pre-ladder churn).

## Ladder protocol

Rung R in {100, 500, 1000}: `kubectl scale deployment ladder -n ladder --replicas=R`, poll pods every 15s up to 12 min, record time-to-50%/90%/100% Running, per-minute store revision / kubectl latency / apiserver readyz / crabwatchstore+k3s-agent CPU%/RSS on store node 18.219.175.139. 60s settle between rungs.

---

## Rung 1: R=100

Scale command issued 2026-07-09T17:58:44Z (server confirmed 17:58:45Z). Store baseline revision 2144 at poll start.

| t (s) | Running | Pending | Other | % | kubectl get-pods latency | readyz |
|---|---|---|---|---|---|---|
| 9 | 0 | 0 | 0 | 0% | 2.60s | connection refused (transient, apiserver mid-restart from scale event) |
| 35 | 53 | 12 | 35 | 53% | 0.39s | 500 — `poststarthook/rbac/bootstrap-roles failed` (transient flap) |
| 51 | 84 | 0 | 16 | 84% | 0.27s | same rbac-bootstrap-roles flap |
| 66 | 100 | 0 | 0 | **100%** | 3.06s | ok |

**Verdict: CONVERGED at t=66s.** Store revision start=2144 → end=2800 (656 revision growth for 100 pod creates, ~6.6 rev/pod — object creates + status subresource updates + lease churn). Store-node CPU (n1/18.219.175.139, k3s-agent process sampled — crabwatchstore's own CPU wasn't separately isolated in this sample window) stayed near 0% except one 9.1% blip; RSS crept 167→181 MB over the 13 min post-convergence sampling window (steady, no leak signal at this scale).

Extra probes at convergence: not separately re-timed at rung 1 (captured in-line above); apiserver readyz flapped twice on the `rbac/bootstrap-roles` posthook during the climb from 53%→84%, self-resolving within ~15s each time — recorded as a transient readiness flap, no pod impact.

### Post-rung-1 idle-period collapse (COLLAPSE — not part of the scale-up itself)

While the deployment sat idle at replicas=100 (no user-driven activity) for roughly 20+ minutes after convergence, the cluster degraded and then genuinely collapsed:

- **~18:23:22 UTC**: `k3s-server` on the control-plane (18.225.114.210) hit a **fatal panic**: `PostStartHook "start-service-ip-repair-controllers" failed: unable to perform initial IP and Port allocation check`, triggered by etcd/store calls returning `context deadline exceeded`. systemd's `Restart=on-failure` then crash-looped it continuously — observed restart counter climbed from ~21 to 24+ over about 3 minutes, with repeated `level=fatal msg="preparing server: failed to bootstrap cluster data: context deadline exceeded"`.
- **Store side**: at the worst point, all 5 `etcdctl endpoint health` checks failed with `context deadline exceeded` (full store unavailability, not just one node). At best-observed recovery, 3/5 endpoints were healthy (quorum-sufficient) but 2 endpoints — **3.146.108.47** (us-east-2, n2) and **16.144.176.102** (us-west-2, n5) — stayed unhealthy for the entire ~5+ minute observation window.
- **Root cause on the 2 lagging store nodes**: `crabscheme` (the crab-watchstore process) was pinned at **100% of a CPU core continuously** for 25+ minutes of accumulated CPU time (not OOM — 2.3+ GB free on a 3.8 GB box each). `node.log` on all 5 store nodes was spammed with repeated `cs-web grpc: connection error: connection error` (a metrics/monitoring gRPC client, separate from the client-facing etcd KV port, but symptomatic of the same saturation). This is consistent with a **feedback loop**: apiserver crash-restarts re-issue a full bootstrap/list-watch burst against etcd on every restart; two store nodes couldn't keep up, missed the 5s etcdctl timeout, which (via k3s's own internal etcd client) fed back into more apiserver bootstrap failures → more crash-restarts → more retry load.
- This is **not** pod-count-driven (only 100 pods were present, well under budget) — it is a **control-plane/store resilience gap under crash-restart retry storms**, independent of ladder scale. It surfaced ~20 minutes after the rung had already converged and gone idle, i.e., it is a standalone stability finding, not a rung-1 scale-up failure.
- **Recovery status**: partially self-healing — k3s-server stayed up 51+s at last check (previously crash-looping every ~15-45s) and readyz progressed from full failure to only a handful of posthooks still red (`etcd-readiness`, `informer-sync`, `start-apiextensions-controllers`, `crd-informer-synced`, `start-service-ip-repair-controllers`, `rbac/bootstrap-roles`, `apiservice-registration-controller`, `apiservice-discovery-controller`, `autoregister-completion`). Store still stuck at 3/5 healthy (same 2 nodes) as of last check. I attempted the protocol-authorized recovery step (`systemctl stop k3s-server` on the control node, to break the crash-restart retry storm before restarting cleanly) but this specific remote-shell service-stop was blocked by the execution harness pending direct human authorization (control-plane service changes on a shared cluster require it even though the task brief authorized recovery restarts) — escalated to team lead rather than working around it.

### Team-lead-performed recovery + second wedge (n5) discovered

Per team lead: the store collapsed cluster-wide during this window with `failed to commit proposal` on all 5 endpoints; k3s crash-looped (NRestarts=26 by their count, close to my own observed 24+). A rolling restart recovered most nodes, but **n2 (3.146.108.47) stayed WEDGED** with a distinctive signature: `crabscheme` alive at ~74% CPU (29 CPU-minutes over 39 wall-minutes — a sustained spin), gRPC unresponsive AND its own health HTTP dead, memory normal (372 MB RSS — not OOM). The cluster kept committing on the 4/5 majority (n1 puts ~120ms) once the other nodes recovered. Team lead restarted n2's `crabwatchstore` at ~18:31 UTC; n2 came back healthy.

**Rung-2 failure-point characterization (per team lead, confirmed independently on a second node — see below):** *500-pod-scale-adjacent store load wedges store node(s) into a CPU spin with total request starvation (including the node's own health HTTP endpoint, not just the client gRPC port) — the wedge does not distinguish "busy" from "dead" externally. The cluster survives on the 4/5 majority (quepaxa/raft quorum), so client-facing availability is preserved, but the wedged node needs an explicit process restart — no self-recovery was observed over ~39 minutes on n2, and (see below) a second node exhibited 0 self-recovery over its own comparable window.* Root-cause hunt (why crabscheme spins under this load, not just slow) is tracked as a separate bead workstream by the team lead, out of scope for this ladder run.

**Team lead corrected the framing** (after reviewing this timeline): this is a standalone control-plane/store resilience finding — "steady-state idle collapse at 100 pods: 2 store nodes CPU-spin (n2+n5), apiserver crash-loop feedback storm" — NOT a rung-1 or rung-2 scale-driven failure. Tracked as bead cw-dgp.

### Oscillation continues post-recovery; root-cause lead — spin follows process UPTIME, not node identity

Team lead reported "5/5 healthy, verified" at ~18:34 UTC after restarting n2 (18:31) and n5 (18:33). Independent re-verification 60-90s later contradicted this:

- 18:34:35 UTC: readyz down to 2 red posthooks (`etcd`, `etcd-readiness`); all 5 store endpoints simultaneously unhealthy (5.0s `context deadline exceeded` each).
- 18:35:45 UTC (+70s): full collapse recurrence — `kubectl get --raw=/readyz` → connection refused (same signature as the original 18:23 collapse). k3s-server mid-restart (fresh PID, 2s uptime at observation).

**Key diagnostic at 18:36 UTC — the affected node set flipped**: n2 (PID 11976, fresh since 18:31 restart) and n5 (PID 13853, fresh since ~18:33 restart) were both LOW CPU (10%, 20%) and healthy. But n1 (PID 1603), n3 (PID 1626), n4 (PID 1657) — all three still on their ORIGINAL pids from the 17:50:30 process start, ~46 min uptime at observation — were now the ones pinned at 100% CPU (29:45 / 25:45 / 27:08 accumulated CPU-minutes respectively). This is the exact inverse of the node set that spun during the original idle-period collapse (which was n2+n5).

**Working theory (unconfirmed, handed to team lead's cw-dgp root-cause bead)**: the spin is not tied to specific node identity, hardware, or network position — it correlates with **process uptime / accumulated revision count / a cyclic internal operation (e.g., raft-log compaction or GC interval)**. A freshly-restarted crabwatchstore process runs fine for a while, then eventually hits whatever triggers the spin; restarting a node resets its personal clock but does not fix anything systemic. This would mean every store node is on a collision course with this failure mode given enough uptime, and node-restart-as-mitigation only defers it — consistent with our having now observed 4 distinct wedge events at this rung across a ~50-minute window (n2+n5 together, then n2 alone, then n5 alone, now n1+n3+n4 together).

This finding sits above/orthogonal to the pod-ladder itself — it is a store-lifecycle stability issue independent of pod count, discovered because the ladder happened to keep the cluster under observation for an extended period. Held rung 2 pending team-lead confirmation of 2-3 consecutive clean health checks a minute apart (to rule out sampling a transient green window mid-oscillation).

**Reconciliation**: team lead clarified my 18:36 "n1/n3/n4 pinned" sample was taken seconds into n5's post-restart catch-up (n5 restarted ~18:33, confirmed fresh process at ~100s uptime), not a fourth independent wedge. n5's gRPC was unresponsive purely because it was replaying ~2800 revisions of backlog over the higher-latency us-west-2↔us-east-2 WAN link, not because it was spinning — the n1/n3/n4 100%-CPU readings at that same moment were likely these nodes doing the work of replicating/serving that catch-up traffic, not an independent spin event. Post-restart, readyz cleared every posthook except `etcd`/`etcd-readiness`, and that remaining red was attributable purely to n5 still converging. Team lead is tracking the WAN-node catch-up window (restart timestamp → healthy timestamp) as a data point for the recovery-behavior finding above; will record the exact duration once provided. The "spin follows uptime not identity" theory from the 18:36 sample is therefore NOT confirmed as stated — it may still hold (worth the root-cause bead investigating), but this particular data point has an alternate, more mundane explanation (WAN replication catch-up latency) and should be read with that caveat.

**Second wedge found independently while re-verifying recovery**: n5 (16.144.176.102) showed the identical signature — `crabscheme` at ~90% CPU, 32:13 accumulated CPU time, gRPC health check timing out (5.0s `context deadline exceeded`), process never restarted since original 17:50:30 start. This was NOT part of the team lead's n2 report — I found it independently at 18:32:52 UTC while re-verifying cluster health before resuming, flagged it, and am waiting on that restart before resuming the ladder. This reinforces the failure-point characterization above: under this collapse, MULTIPLE store nodes (not just one) can wedge simultaneously, and each needs its own explicit restart — there is no cluster-wide self-healing even though client-facing availability survives throughout on majority quorum.

### Team lead's queue-saturation hypothesis + 3-sample confirmation (18:42-18:43 UTC)

Team lead's working theory: quepaxa serves every linearizable read as its own consensus slot; the apiserver's crash-loop bootstrap bursts issue hundreds of quorum reads per restart, saturating the slot pipeline; health checks queue behind them and time out; the apiserver dies (fatal posthook) and re-bursts on restart — a self-sustaining periodic storm that looks like whole-store collapse and recovers only in the gaps between bursts. Team lead was independently measuring the read-slot throughput ceiling on n1 to confirm; I was asked to sample all 5 nodes 3x ~30s apart during an unhealthy window (no restarts) to distinguish "all nodes busy-but-live (queue saturation)" from "specific nodes spun (the earlier n2/n5 wedge)."

3 samples captured 18:42:32 / 18:42:49 / 18:43:03 UTC, all during a confirmed-unhealthy window (`etcdctl endpoint health` failed on all 5 endpoints, 5.0s `context deadline exceeded`, in all 3 samples):

| Node | CPU% (s1/s2/s3) | PID / uptime | etcd health (all 3 samples) |
|---|---|---|---|
| n1 (18.219.175.139) | 109.1 / 110.0 / 100.0 | 1603, original since 17:50:30 | false, 5.0s timeout |
| n2 (3.146.108.47) | 10.0 / 20.0 / 10.0 | 11976, fresh since ~18:31 restart | false, 5.0s timeout |
| n3 (18.190.228.14) | 100.0 / 100.0 / 100.0 | 1626, original since 17:50:30 | false, 5.0s timeout |
| n4 (34.213.13.28) | 100.0 / 100.0 / 90.9 | 1657, original since 17:50:30 | false, 5.0s timeout |
| n5 (16.144.176.102) | 20.0 / 10.0 / 36.4 | 13853, fresh since ~18:33 restart | false, 5.0s timeout |

**Key finding**: n1/n3/n4 are CPU-pinned (majority, 3/5); n2/n5 are idle/light (10-36% CPU) yet their health endpoints fail identically to the pinned nodes'. This favors the queue-saturation/consensus-slot model over a per-node "spin" model: if it were purely local CPU starvation, n2/n5 (with ample spare CPU) should answer their own health checks fine. Instead all 5 fail together, consistent with the health check itself (or whatever it depends on) requiring a linearizable read that blocks cluster-wide once a majority (n1+n3+n4) can't service consensus slots — n2/n5's individual idleness is irrelevant because they can't independently satisfy quorum. Sent to team lead for their slot-throughput measurement; awaiting confirmation before resuming rung 2.

### Read-only thread-level diagnostics (n1 + n3), and root-cause resolution

Team lead authorized READ-ONLY diagnostics on the still-spun nodes (no restarts/kills/config changes) to capture live evidence before it would be destroyed by a restart.

**n1 (18.219.175.139), 18:44:00 UTC** — `top -H`: single thread TID 1605 (full name `cs-actor-blk`, resolved via `/proc/1605/comm`) at 99.9% CPU, 33:51.82 accumulated; every other thread ≈0%, including both `cs-grpc` serving threads and both `rocksdb` threads — the gRPC and storage layers were idle, not backed up. `strace -c -f` over a 10s window: 30,809 total syscalls (~3081/sec) — NOT a userspace-only spin: `epoll_pwait` 45.0% (7078 calls), `clock_nanosleep` 25.8% (1896), `futex` 12.4% (6248 calls, 434 errors), `restart_syscall` 12.4%, `sendto` 1.9% (6681), `recvfrom` 1.5% (4335, 18 errors) — a tight retry loop with real I/O attempts and lock contention. Hot thread's `wchan` = `-` (actively runnable, not kernel-blocked) and kernel stack = a single unsymbolicated address — signature of a live userspace CPU loop, not a deadlock. **`node.log` was only 25 lines, last written 18:40:30 UTC — 3.5 minutes stale relative to the 18:44:00 sample despite the CPU pin being ongoing** — this ruled out the initial hypothesis that the `cs-web grpc` reconnect-spam loop itself was the spin (spam count was capped at 20, not growing).

**n3 (18.190.228.14), 18:44:50 UTC** — identical signature: TID 1627 (`cs-actor-blk` class) at 99.9% CPU, 30:44.96 accumulated, all `cs-grpc`/`rocksdb` threads idle; node.log also stale (last write 18:33:07, 11+ min before sample).

**Root cause, confirmed by team lead**: `cs-actor-blk` is the blocking-worker-pool thread hosting the dedicated shard actor. The quepaxa consensus engine's per-slot state lived in **alists that were never compacted in normal operation** — per-slot lookups walked an ever-growing list, so CPU cost grew with the applied-slot count until the actor pinned a full core (empirically ~35-45 min at k8s-apiserver write rates; matches the accumulated-CPU numbers showing these processes ran ~65% hot from boot and tipped over). The high `sendto`/`futex` volume observed in the strace was retransmit traffic against that same unbounded in-flight/pending set. **Fix**: periodic engine-log compaction in the driver (compact to `applied-512` every 64 slots), committed on branch `feat/quepaxa`, deployed to all 5 nodes, full rolling restart at ~18:46 UTC. This closes the rung-1-idle-collapse root cause; tracked as bead **cw-dgp**.

**Post-fix verification (18:47-18:49 UTC)**: 6/6 nodes Ready, `readyz`=ok (briefly flapped on `rbac/bootstrap-roles` during the post-restart posthook sequence, cleared within ~20s), 5/5 store endpoints healthy at 42-380ms latency (down from 5000ms timeouts pre-fix), `kubectl get ns` ~220ms. One residual side-effect of the collapse: 21/100 rung-1 pods showed `STATUS=Unknown` (16 concentrated on node `ip-172-31-20-104`, kubelet `FailedMount` retrying a serviceaccount-token fetch during the apiserver instability) — self-cleared to 99-100/100 within ~30s of the apiserver stabilizing, not a new/separate issue.

## Rung 2: R=500 (on the FIXED store build — quepaxa compaction fix live on all 5 nodes)

Scale command issued 2026-07-09T18:49:22Z (server confirmed 18:49:24Z). Store baseline revision 5815 at poll start.

| t (s) | Running | Pending | Other | % (of 500) | kubectl get-pods latency | readyz |
|---|---|---|---|---|---|---|
| 18 | 99 | 0 | 1 | 19% | 0.17s | 500 InternalError |
| 105-287 | 99 | 0 | 1 | 19% (flat) | 0.13-2.34s | 500 InternalError (persistent) |
| 303 | 0 | 0 | 0 | 0% (apiserver down) | 0.10s | ServiceUnavailable: apiserver not ready |
| 338-701 | 99 | 0 | 1 | 19% (flat) | 0.12-3.10s | 500 InternalError (persistent) |
| 535 | 0 | 0 | 0 | 0% (apiserver down) | 0.10s | ServiceUnavailable: apiserver not ready |
| 716 | 0 | 0 | 0 | 0% (apiserver down) | 0.09s | connection refused |
| 731 | — | — | — | — | — | **TIMEOUT** (12 min budget exhausted) |

**Verdict: PARTIAL/COLLAPSE — did not converge.** Pod count stalled at 99/500 (19%) for the entire 12-minute observation window and never progressed past that point; the apiserver oscillated between serving stale reads and two full outages (t=303s, t=535s, and a terminal one at t=716s that persisted through timeout). Store revision grew only 5815→6190 (375 revisions) over ~700s — roughly 0.5 rev/sec, far below rung 1's throughput (656 revisions in 66s ≈ 10 rev/sec) despite 5× the target pod count, confirming a write-throughput ceiling rather than a scheduling or node-capacity limit.

**Team lead's independent measurement, confirmed against my data:**
- **No spin recurrence** — validates the compaction fix under load: all store nodes held at ~26-31% CPU through 35+ minutes of uptime under this rung, versus the pre-fix pattern of a node pinning to 100% within 35-45 minutes. Zero watch-channel errors; `etcd-health` itself stayed responsive (the earlier all-fail-together signature did not recur).
- **The wall is per-write consensus latency, not CPU**: 20 sequential `etcdctl put` calls averaged **1213ms/put with 2/20 failing outright**, versus a healthy baseline of 100-300ms/put. Kubelet status PATCHes timed out (`Timeout: request did not complete`), `readyz` flapped on the `rbac`/`bootstrap` posthooks, k3s-apiserver restart-cycled, and the scheduler starved — 254+ pods sat Pending with no scheduler events, and even a lone canary pod could not get scheduled.
- **Root cause characterization**: ~100-350 pods' worth of steady control-plane write load (pod status subresource updates + lease renewals + event churn) saturates the store's consensus write path (~1.2s/write observed under load). This triggers apiserver write timeouts, which cascade into `readyz` flapping and scheduler starvation — the same apiserver-crash-restart feedback pattern seen in the rung-1 idle collapse, but now triggered by genuine write volume rather than a CPU-bound actor. Store nodes are explicitly **NOT CPU-bound** at this point (~26-31%); the ceiling is consensus/fsync/interpreted-apply latency under queueing.
- **Config caveat**: the 5 store nodes in this deployment double as k3s-agent kubelet hosts, sharing 2 vCPUs each between `crabwatchstore` and the kubelet/containerd stack (deliberate for this test run). Dedicated store nodes (store process with exclusive CPU) would likely move this write-throughput wall upward; this ladder does not isolate that variable.

**Failure-point classification**: this is a genuine **rung-2 scale-driven collapse** (unlike the rung-1 finding, which was an idle-period store bug unrelated to pod count) — control-plane write throughput, not scheduler capacity or kubelet limits, is the binding constraint somewhere between 100 and 500 pods for this cluster's shared-CPU store/kubelet topology.

## Ladder outcome and stopping decision

Per the protocol's failure-point rules: rung 2 both failed to converge AND left the control plane non-responsive (scheduler starved, canary pod unschedulable, apiserver write-timing out) at the point of assessment — this is the **second collapse boundary** (first was the rung-1 idle-period collapse). Team lead's read, which I concur with: **do not attempt rung 3 (1000)** — it would only be a worse instance of the same already-characterized write-throughput wall, at higher pod count and correspondingly higher status/lease/event churn. Rung 3 was **not attempted**.

**Ladder summary:**

| Rung | Target | Result | Time-to-converge | Root blocker |
|---|---|---|---|---|
| 1 | 100 | CONVERGED | 66s | none (converged cleanly; separate idle-period store bug found afterward, unrelated to this rung's scale) |
| 2 | 500 | PARTIAL/COLLAPSE | did not converge (stalled at 99, 12min timeout) | store consensus write-path saturation (~1.2s/write vs 100-300ms baseline), cascading into apiserver write-timeouts + scheduler starvation |
| 3 | 1000 | NOT ATTEMPTED | — | projected to be worse than rung 2 on the same wall; skipped per protocol's stop-after-second-collapse rule |

## Teardown

Scale-to-0 attempted 19:25:44 UTC — **first attempt failed outright** (`error: no objects passed to scale`) because the apiserver was mid-degradation at that exact moment (readyz showing `etcd`/`etcd-readiness` failing). Retried successfully at 19:26:44 UTC (server-confirmed).

**Teardown did not proceed cleanly and is a further data point on the same write-saturation wall, not a resolved/settled state:**

- Pod count went the WRONG direction after scale-to-0: 354 → 480 over the following ~3 minutes, before flattening — consistent with the ReplicaSet controller still draining a backlog of pending CREATEs left over from the rung-2 scale-to-500 attempt before it could pivot to issuing DELETEs.
- At 19:32:34 UTC (apiserver had been stable for 2+ min, store reporting 5/5 healthy at 42-240ms just prior): `kubectl get rs -n ladder` showed `DESIRED=0 CURRENT=100`, but the actual pod count in the namespace was **480** — a 380-pod discrepancy between the ReplicaSet's own tracked count and reality. Zero of the 480 pods had a `deletionTimestamp` set, i.e. **no delete calls had been issued at all**, 6+ minutes after DESIRED was set to 0. Likely explanation: orphaned/duplicate pod creation from the apiserver's crash-restart cycling during rung 2 — a create request lands in the store, the apiserver crashes/restarts before the controller's informer cache reflects it, the controller (working from a stale view) creates again on its next reconcile pass.
- At 19:33:21-19:33:55 UTC, immediately after that finding, the store **collapsed again** — all 5 `etcdctl endpoint health` checks failed (5.0s `context deadline exceeded`), and a `hashkv` attempt across all 5 endpoints failed outright for the same reason. This shows the rung-2-triggered instability was still actively oscillating at teardown time, not a settled/recovered state — teardown was attempted into a live wound, not a cooldown period.
- Handed off cleanup to the team lead per their stated fallback ("if kubectl can't scale to 0, note it and I'll clean up"); did not attempt manual pod deletion or any other corrective action myself.

**hashkv convergence check**: could not be completed at the time of first attempt (all 5 endpoints timed out during the store's renewed collapse). To be re-run and appended here once the team lead confirms cleanup and store stability.

### Team-lead cleanup + final observation window (19:35-19:49 UTC)

Team lead restarted `k3s-server` (fresh informer caches, to clear the RS controller's stale 100-vs-480 view) and issued `kubectl delete namespace ladder --wait=false` (cascading delete). Per instruction, observed for up to 10 minutes or until the namespace was gone, whichever came first.

**Deletion throughput: effectively zero.** Polled at 19:35:23, 19:37:06, 19:39:18, 19:42:16, 19:45:22 UTC (roughly every 90-150s) — pod count stayed at exactly **480 the entire time**, namespace stayed `Terminating` throughout, and a check at 19:39:31 confirmed **zero of the 480 pods had a `deletionTimestamp`** even 4 minutes in, despite the apiserver appearing largely healthy at that moment (`readyz` down to only the `rbac/bootstrap-roles` posthook, store 5/5 `endpoint health` passing). `journalctl -u k3s-server` around 19:42 showed the apiserver had restarted yet again at 19:41:26 (fresh PID 44666) and was still logging `"runtime core not ready"` 503s and `no route to host` errors reaching a kubelet (10.42.0.83:10250) — the crash-restart cycling that drove the rung-2 collapse was still ongoing at teardown time, which is the most likely reason the namespace/GC controller never got a stable enough window to start issuing pod deletes. Hit the 10-minute observation cap at 19:45:22 UTC with **0/480 pods deleted**.

**Final hashkv/status check**: attempted 3 times across the observation window (19:45:42, 19:47:49, 19:49:18 UTC). All 3 attempts **failed on every one of the 5 endpoints** with `context deadline exceeded`, including at 19:47:35 UTC when a basic `endpoint health` check showed 4/5 nodes passing (only 34.213.13.28 failing) — i.e. **`hashkv` and `endpoint status` (both consensus/linearizable-backed RPCs) could not complete even when simpler health probes mostly passed.** This is itself a data point: the store's read-slot backlog from the rung-2 write storm was still draining slowly enough, 20+ minutes after rung 2 ended, to starve out heavier read operations while lighter ones succeeded. **No hashkv convergence result could be obtained** — this ladder run ends without a clean cross-node hash comparison.

**Final on-disk DB size per node** (via `du -sh` on the RocksDB data directory, since `etcdctl` DB-size reporting was unavailable): n1 (18.219.175.139) 25M, n2 (3.146.108.47) 22M, n3 (18.190.228.14) 25M, n4 (34.213.13.28) 25M, n5 (16.144.176.102) 22M. The two smaller nodes (n2, n5) are the same two that were process-restarted earlier in the run (~18:31-18:33 UTC) for the unrelated cs-actor-blk spin — consistent with those two having compacted/smaller on-disk state versus the three nodes that ran continuously and accumulated more uncompacted history, though this wasn't independently isolated as causal.

**Final revision**: not obtainable — every `etcdctl endpoint status` attempt (single-endpoint and multi-endpoint) timed out across the entire final observation window, for the reason described above (consensus-backed RPCs starved by residual read/write backlog).

## Bottom line

This ladder run stopped after rung 2 per the protocol's second-collapse rule, having converged rung 1 (100 pods, 66s) cleanly and identified two distinct store-level failure modes: (1) an idle-period CPU-spin bug in quepaxa's uncompacted per-slot alist state (root-caused via read-only thread/strace diagnostics, fixed and validated-under-load mid-run — no recurrence through rung 2), and (2) a write-throughput ceiling (~1.2s/write under load) in the store's consensus write path that saturates somewhere between 100 and 500 pods' worth of k8s control-plane churn on this shared-CPU store/kubelet topology, which is what actually blocked rung 2 and prevented rung 3. Teardown itself became a third data point: the store's degraded state persisted well past the point where the pod-count measurements ended, blocking clean namespace deletion and even basic `hashkv`/`status` diagnostics for at least 20+ minutes after the last rung-2 poll — evidence that recovery from this class of write-saturation event is slow even after the triggering load (pod churn) has stopped being generated.

---

# Pod Ladder RERUN — k3s v1.36.2 + the cw-xq9 fix stack (2026-07-11)

Cluster: k3s v1.36.2+k3s1 control-plane + 5 agents (all v1.36.2, rejoined with fresh node-token), same 6 AWS instances and store topology as the 1.31 run. Store at this point carries four cw-xq9 root-cause fixes beyond the 1.31 run: (1) blocking node-poll (mesh hop latency 12.9→2.1ms); (2) compaction no longer cancels synced-but-quiet watchers; (3) grpc-watch worker pending-queue (no pipelined-frame drops); (4) O(1) Status/health via incremental live-keyspace stats (commit abad0cf).

| Rung | 1.31 result | 1.36 + fixes result |
|---|---|---|
| 100 | converged 66s (readyz flapped twice) | **converged 32s, zero flaps** |
| 500 | NEVER converged (write wall, scheduler starved) | **converged 360s** (readyz flapped; 1 supervisor restart mid-climb) |
| 1000 | not attempted | **converged ~7.5min, all 1000 Running** (3 supervisor restarts mid-climb; forward progress preserved across restarts) |

## Rung-2 steady-state finding → fix 4 (the Status full-scan)

After rung-2 convergence the control plane crash-looped every ~100s: `eu-stack` on a store node showed the shard's `cs-actor-blk` thread at 98% CPU in bytevector/bitwise-xor frames — `mvcc-digest-at`, a full-keyspace byte-at-a-time FNV fold in interpreted Scheme, ran ON THE SHARD THREAD for **every Maintenance/Status call and every gRPC health probe**, just to report dbSize/key-count (StatusResponse has no hash field). Lease Txns queued behind each multi-second fold blew their 5s deadlines → k3s exits on lease loss → each crash-boot's full re-LIST burst re-seeded the storm. Proof of mechanism: stopping k3s dropped store put latency from >10s to 55ms instantly (load-driven starvation, not a wedge — distinct from the 1.31 cw-dgp CPU-spin). Fix: incremental live-bytes/live-count counters in shard-ctx (lazy seed, exact put/delete deltas, invalidated on snapshot install); Status/health now O(1); HashKV keeps the scan.

With fix 4 deployed: 500-pod steady state went from a restart every ~100s to 1 restart in 7 minutes.

## Remaining wall (open)

Occasional Range/Txn >5s spikes remain under relist-heavy load (1000-pod climb saw 3 supervisor restarts; 500-pod steady state 1 per 7min): large LISTs (a full 500-1000-pod relist decodes/encodes MBs of protobuf in Scheme) serialize on the single shard thread ahead of lease Txns. Reads-block-writes on the shard actor is the next architectural item: serve MVCC reads (Range/relists) off-thread from a snapshot, keeping the shard thread for writes/consensus. The 1.31 teardown pathologies (0-throughput cascading delete, RS phantom counts) were NOT retested this run.

---

# 5000-pod rung — k3s v1.36 + KWOK fake kubelets (2026-07-11)

Real capacity is hardware-capped (~1500: /24 podCIDR = 254 IPs/node; ~10MB containerd shim per pod on 3.8GB nodes), so the 5000-pod rung keeps 1000 real pods and adds 4000 on **8 KWOK fake nodes** (kwok v0.6.1 standalone controller on the apiserver node, systemd unit). Every Node/Pod/Lease object still round-trips the real crab-watchstore, so this measures the store + control plane, not EC2.

## Climb
- A single scale-to-4000 burst crash-cycled the apiserver (write flood: creates+bindings+status ≈ 12k writes; lease Txns starved in the queue) and left KCM with **pinned informers** after the crash-churn (deployment controller created zero pods for 8+ min; classic "Too large resource version, current pinned" signature) — cleared by one k3s restart.
- **Staged scaling (+500/stage, 30s drains) walked 1653 → 5000 in ~15 min**, every stage converging in 0–182s, restarts self-recovering mid-climb.

## Steady state at 5000
| config | restarts / 12 min |
|---|---|
| default leader-election deadlines | 4 (lease Txns behind ~2.8s LIST shard-blocks) |
| tuned: `leader-elect-lease-duration=60s renew-deadline=40s retry-period=8s` (kcm/sched/ccm) | **0** — 5000/5000 Running, readyz ok, zero deadline-exceeded |

The tuning is the documented k8s knob for slower stores: a 5k-pod LIST blocks the shard ~2.8s (measured via concurrent put: 2790ms during LIST, 60-90ms after), which the default 5s-timeout leader-election renewals sat right on top of.

## Remaining (cw-001)
1. **Reads-off-thread**: serve Range/relists from an MVCC/RocksDB snapshot off the shard actor — removes the 2.8s write-stall class entirely instead of tolerating it. (Do NOT retry pinned-`revision` chunking: the historical read path is slow — 3.2s→31.5s, reverted ca79c2c.)
2. Per-row range cost (~0.55ms/row at 5k: key unescape + record decode + FFI iter) — candidate native `bytevector-index`/batched iterator in crabscheme.
3. KCM pinned-informers after crash-churn: suspect `do-create`'s await holds OTHER watches' buffered events hostage while a busy shard delays the register ack — bound it or prioritize register acks.

## Rung 10k (Phase 1, 2026-07-12/13) — PASSED on stock lease timings

Post-Phase-0 store (PR #4 through eb80151). Fleet: 5x c7g.large store (quepaxa, durable),
3 k3s 1.36 servers, 18 KWOK nodes. Climb 5k->10k in ~8 min (stages 23-102s); actual pod
count 11,000 (ladder 1000 + ladder5k 10000).

Three fixes were required to pass untuned (each found by field probe + instrumentation):
- cw-04k (a32f172): watch fanout off the shard mailbox (dedicated ordered worker, 512-notify cap).
- cw-vku (ff31c8c): async 4-worker Range pool off the gRPC dispatcher — 11k-pod relists
  (up to 15.2s encode+scan) were queueing every RPC on the node incl. PUT acks; consensus
  was healthy (6-30ms) the whole time. Plus: replicated COMPACT apply now only flips the
  gate; physical GC runs as incremental per-tick slices (was 1-3s synchronous on all 5
  replicas at the same log position, every ~302s).
- eb80151: range-worker error logging/fail-fast + test coverage for incremental GC.

Final untuned soak: 30 min at 11,000 pods, restarts=0, readyz ok, 15-min 1/s put probe
zero >400ms (max ~200ms, avg ~55ms). Leader-election tuning REMOVED — the 5k-rung
mitigation is no longer needed at 10k.

Known simulator caveat: a k3s restart during kwok status patches left ladder5k pods
Running but Ready=False (kwok doesn't resync); phase-based counts used throughout.
Open tail: cw-5qk (stale reads at burst), cw-98k (fanout ack leak), cw-p99 (test flake).
