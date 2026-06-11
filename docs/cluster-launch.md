# Running a 3-node cluster (cw-24e.1)

The supported entrypoint is `bin/crab-watchstore` — one process per member.
Options come from `--config FILE` (plain `key value` lines, `#` comments),
with any CLI flag overriding the config. Full option set = node-cluster.scm's
flags without the leading `--`: `node`, `db`, `durable`, `cluster`, `join`,
`tls-cert`, `tls-key`, `tls-ca`, `tls-require-client-cert`, `config`.

## Bootstrap (3 nodes, one host)

```sh
# the checked-in example configs use ports 21790-2 (raft) / 23790-2 (client)
bin/crab-watchstore --config examples/cluster-3/a.conf &
bin/crab-watchstore --config examples/cluster-3/b.conf &
bin/crab-watchstore --config examples/cluster-3/c.conf &

etcdctl --endpoints=127.0.0.1:23790,127.0.0.1:23791,127.0.0.1:23792 endpoint status -w table
etcdctl --endpoints=127.0.0.1:23790 put k v
```

A config file:

```
node     a
db       /var/lib/crab-watchstore/a
durable  yes
cluster  a:10.0.0.1:7001:2379,b:10.0.0.2:7001:2379,c:10.0.0.3:7001:2379
```

`cluster` lists EVERY member as `name:host:raftport:clientport`; each member
finds its own row by `node`. The same spec must be identical on all members.
`durable yes` = fsync-gated acks (group-committed); `no` = relaxed.

For TLS/mTLS set `tls-cert`/`tls-key` (+ `tls-ca` for mutual). To grow or
shrink a running cluster see the membership flow (`--join` + `etcdctl member
add/promote/remove`) — full operations guide lands as cw-24e.3.

Acceptance test: `test/cluster-config-launch.sh`.
