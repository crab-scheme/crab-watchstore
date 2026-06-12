# Operations guide (cw-24e.3)

Day-2 operations for a crab-watchstore cluster, driven entirely by the official
`etcdctl`. Every command below was executed against a live local 3-node cluster
(the `examples/cluster-3` configs) while writing this guide; outputs are real.
Launcher + config-file reference: `docs/cluster-launch.md`.

## Bootstrap

One process per member; the shared `cluster` spec lists every member as
`name:host:raftport:clientport`:

```sh
bin/crab-watchstore --config examples/cluster-3/a.conf &
bin/crab-watchstore --config examples/cluster-3/b.conf &
bin/crab-watchstore --config examples/cluster-3/c.conf &
```

Wait for each log to print `etcd KV gRPC serving on ...`, then:

```text
$ etcdctl --endpoints=127.0.0.1:23790,127.0.0.1:23791,127.0.0.1:23792 endpoint status -w table
# every row shows the SAME leader id; revision/raftTerm agree across members
```

Reads and writes are leader-gated (ReadIndex): a follower endpoint answers
status/health locally but redirects KV traffic with `etcdserver: not leader` —
give clients the full endpoint list and they fail over to the leader.

## Health

Each member serves etcd's HTTP probe endpoints on `clientport + 10000`
(override: `--metrics-port`): `/health`, `/version`, `/metrics`.

```text
$ curl -s http://127.0.0.1:33790/health
{"health":"true","reason":""}          # 503 + "NO LEADER" when the member has no leader
```

## KV smoke

```text
$ etcdctl --endpoints=<leader> put app/config v1
OK
$ etcdctl --endpoints=<leader> get app/config --print-value-only
v1
```

## Membership (live, joint consensus)

```text
$ etcdctl --endpoints=<leader> member list -w table
+----------+---------+------+------------------------+--------------+------------+
|    ID    | STATUS  | NAME |       PEER ADDRS       | CLIENT ADDRS | IS LEARNER |
+----------+---------+------+------------------------+--------------+------------+
| 313bcb2d | started |    a | http://127.0.0.1:21790 |              |      false |
| 343bcfe6 | started |    b | http://127.0.0.1:21791 |              |      false |
| 333bce53 | started |    c | http://127.0.0.1:21792 |              |      false |
+----------+---------+------+------------------------+--------------+------------+
```

**Adding a real member** (order matters — same as etcd):

1. Start the new process with `--join yes` and the FULL cluster spec including
   itself. It comes up as a non-voter, dials the existing members, and waits.
2. On the leader: `etcdctl member add d --peer-urls=http://<host>:<raftport>`
   (voter, one joint-consensus round) — or `member add d --learner` followed by
   `etcdctl member promote <ID>` once it has caught up.
3. The leader replicates the log to it; when the configuration change commits
   the member is a full voter.

```text
$ etcdctl --endpoints=<leader> member add d --peer-urls=http://127.0.0.1:21793
Member          8a3d11f added to cluster         22b282ab
```

**Removing a member**: `etcdctl member remove <ID>` (the ID column from
`member list`). Removing the LEADER is legal — it transfers out via the same
joint-consensus machinery. One membership change at a time (a second `member
add/remove` while one is in flight is rejected, like etcd).

Caveat from the live run: adding a voter that is NOT actually running leaves a
dead voter in the config — quorum math now includes it (3 live of 4 voters
still has quorum, but you are one failure from stall). Add real processes, or
remove the entry promptly. The Jepsen membership nemesis exercises 3 full
remove→re-add cycles under load (docs/jepsen-validation.md).

## Snapshot

```text
$ etcdctl --endpoints=<leader> snapshot save /tmp/backup.db
Snapshot saved at /tmp/backup.db
```

The stream is crab-watchstore's logical keyspace (512-aligned + sha256 trailer,
so etcdctl downloads and verifies it). **`etcdctl snapshot restore` does NOT
apply** — it parses bbolt, which this store does not use. Restore paths:

- **Member loss**: start a fresh member with `--join` + `member add`; Raft log
  replication (snapshot-backed below the compaction floor) catches it up. This
  is the normal recovery path and is what Jepsen's kill/membership runs prove.
- **Whole-cluster restore**: each member's RocksDB data dir (`<db>-shard0`) is
  the durable state — file-copy it (or a `store-checkpoint`) while the process
  is stopped, and start the cluster from the copied dirs.

## Leadership transfer (MoveLeader)

```text
$ etcdctl --endpoints=<leader> move-leader 313bcb2d
Leadership transferred from 343bcfe6 to 313bcb2d
```

The target must be a CAUGHT-UP VOTER; otherwise the request is refused with
`etcdserver: bad leader transferee` (observed live against a lagging target —
the guard, not a fault). Implementation: TimeoutNow (Raft §3.10), so the
transferee immediately campaigns at the next term, winning without waiting an
election timeout.

## Maintenance odds and ends

- `etcdctl endpoint hashkv` on every member must return the SAME hash — the
  quick cross-member consistency check (`test/etcd-maintenance-grpc.sh` asserts
  it; the docker acceptance test does too).
- `etcdctl defrag` maps to an advisory RocksDB flush/compact; always succeeds.
- `etcdctl alarm list / disarm` — alarms are Raft-replicated (survive failover).
- Maintenance/Status fields (`raftIndex`, `raftTerm`, `raftAppliedIndex`,
  `dbSize`) are live values from the shard, suitable for dashboards.

## Where the proofs live

`test/etcd-cluster-grpc.sh` (membership), `test/etcd-maintenance-grpc.sh`
(status/hashkv/alarm/defrag/snapshot/move-leader), `test/etcd-moveleader-grpc.sh`,
`test/cluster-config-launch.sh` (bootstrap), `test/docker-cluster.sh` (compose),
and `docs/jepsen-validation.md` (fault tolerance under partition/kill/membership).

## Multi-region operation (cw-lkq)

A cluster spans regions with one (or more) members per region; writes stay one
linearizable Raft group; reads/watches are served region-locally
(docs/specs/multi-region.md). Everything below was executed against the WAN
compose overlay (`deploy/docker/wan.override.yml`, 150 ms simulated RTT —
`test/wan-soak.sh` is the repeatable gate).

### Configuration

```text
# member config — one per region
locality us-east/1a          # region[/zone]; or spec field name:host:raft:client:REGION
tick-ms 250                  # WAN raft profile: heartbeat interval (etcd --heartbeat-interval)
election-ticks 8             # election base; timeout ~= tick-ms * (8 + stagger)
leader-region us-east        # leaders auto-transfer back into this region
serializable-max-lag 5000    # optional: redirect serializable reads when > N entries behind
```

- **Leader placement**: an out-of-region leader hands off (TimeoutNow) to a
  caught-up voter in `leader-region`, rate-limited; a down preferred region is
  a no-op (availability wins). Verified live: east leader auto-transferred west.
- **Reads**: `--consistency=s` (serializable) is served by the LOCAL member, no
  WAN hop. Linearizable reads AND writes sent to a follower are FORWARDED to
  the leader transparently (etcd parity) — so kube-apiserver can list every
  member in `--etcd-servers`. Forwarded calls expire (~3 s) to `tryagain` if
  the leader dies mid-flight; clients retry.
- **Watches**: served by the LOCAL member (replay + live, in that member's
  revision order). Learners (non-voting members added with `member add
  --learner`) serve serializable reads + watches without affecting quorum —
  the read-replica pattern for far regions.

### Client routing (kube-apiserver)

Per region, list the LOCAL members first; the gRPC client health-checks and
fails over. Writes from a non-leader region cost one WAN RTT (~1.5x RTT for a
persistent client; see test/wan-soak.sh's measured budgets).

### Bootstrap order

Start all members of the QUORUM regions first (the cluster spec is identical
everywhere); learner regions join afterwards (`--join` + `member add
--learner`). With a 2+2+1 voter layout the cluster survives any single region
loss; with 3 voters in one region + learners elsewhere, the voter region is
the failure domain (choose deliberately).

### Region-loss runbook

1. A quorum-retaining loss (e.g. one region of a 2+2+1 layout): nothing to do —
   writes continue after one election (terms move by exactly 1; verified at
   150 ms RTT under load with zero lost acks). If the lost region was
   `leader-region`, leadership stays out-of-region until it returns, then
   auto-transfers back.
2. A learner region loss: zero quorum impact; its clients fail over to the
   next-nearest region's endpoints.
3. Recovery: restart members with their data dirs (they catch up by
   replication); a destroyed member rejoins via the wipe + `--join` +
   `member add` flow (see Membership above).
