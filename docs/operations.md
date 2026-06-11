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
