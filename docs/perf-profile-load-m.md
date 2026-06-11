# Profile: gRPC write path under `check perf --load m` (cw-b5w.1)

Date: 2026-06-11 · single node, relaxed (`--durable no`), darwin arm64, release crabscheme
(`--features stdlib-store,grpc`). Harness: `bench/profile-load-m.sh` — starts the server,
drives `etcdctl check perf --load m` (200 clients, ~1000 writes/s target, 60s), and takes
three 10s `sample` profiles mid-load (1ms interval). Raw output: `/tmp/cws-profile-m/`.

## Headline: load=m now PASSES

```
PASS: Throughput is 1000 writes/s
PASS: Slowest request took 0.081538s
PASS: Stddev is 0.004527s
PASS
```

This supersedes the `cw-u4a.36` result in `docs/perf-vs-etcd.md` ("did not sustain load=m"),
measured before the `cw-u4a.38` O(log n) `store-seek` point-read rewrite. The single-node
sustain ceiling is now ≥1000 w/s with an 82ms worst-case and 4.5ms stddev (etcd at load=m:
177ms slowest). Remaining gap to etcd is load=l (~8000 w/s).

One artifact: the post-test cleanup `DeleteRange` of all 60k `check perf` keys exceeded the
client's deadline (`FAIL: Cleanup failed during key deletion`). The perf verdict itself is
PASS; bulk-delete latency is a separate (minor) issue.

## Ranked hotspots (top-of-stack leaf samples, stable across all 3 samples)

Kernel wait states (`__psynch_cvwait` 172k, `kevent` 15k, `swtch_pri` 11k, `__accept` 8k)
dominate raw counts — those are parked threads/green-actor workers, i.e. the server is NOT
CPU-saturated at 1000 w/s. Among on-CPU samples:

| rank | leaf | samples (s2) | what it is |
|---|---|---|---|
| 1 | `cs_vm::vm::run_dispatch` | 2422 | VM bytecode interpreter loop — **the** cost center |
| 2 | malloc/free family (`_xzm_*`, `_free`, memset/bzero) | ~2000 | allocator churn |
| 3 | `Env::get_nb` + `Bindings::insert_nb` | ~1150 | VM env variable lookup/insert |
| 4 | `vm_value_drop_gc` + `vm_value_clone_gc` + `Value::clone` + pair drops | ~1900 | refcount/clone/drop traffic on boxed Values |
| 5 | `ValueStack::slice_as_values` + `NanboxValue::from/to_value` | ~1250 | nanbox↔boxed conversion at builtin-call boundaries |
| 6 | `cs_runtime::eval::eval` (walker) | ~420 | residual walker fallback (higher-order builtins) |
| 7 | `proc_table::alloc/peek` | ~430 | closure allocation per call |
| 8 | sip hashing (`Hasher::write`, `hash_one`) | ~170 | hashtable ops (env/global/table lookups) |

RocksDB frames appear in the call tree but are nowhere near the leaf top — storage is not
the bottleneck. No `raft`/`mvcc`-named native frames (those layers are Scheme, accounted
inside run_dispatch).

Top builtins by tree appearances: `b_cons`, `b_make_bytevector`, `b_reverse`, `b_append`,
`b_car/cadr/cdr`, `b_bytevector_u8_set/ref`, `b_quotient/remainder` — the signature of
**proto.scm's hand-rolled varint/protobuf encode/decode in interpreted Scheme** (byte-at-a-
time bytevector ops + list building with reverse/append).

## What this means for the epic

1. **The bottleneck is interpreted-Scheme CPU per request, exactly as hypothesized** —
   VM dispatch + boxing/alloc churn, not storage, not Raft, not the network stack.
2. **cw-b5w.2 (native protobuf fast-path) attacks the right code**: the builtin mix shows
   per-byte bytevector + list churn from proto.scm on every request. Moving
   PutRequest/RangeRequest decode + response encode to native FFI removes rank 1/2/5
   work wholesale for the hot RPCs.
3. **cw-b5w.3 (proposal batching)** is about tail/ceiling, not CPU: the server is idle-heavy
   at 1000 w/s, so the serialization cost is round-trips through the shard actor, which
   batching amortizes. Bulk DeleteRange timing out is a hint that per-op apply cost compounds.
4. **cw-b5w.4 (multi-shard)** adds parallelism we don't yet need at load=m (not CPU-bound)
   but will need for load=l.
5. **Acceptance for cw-b5w.5 moves up**: load=m already passes, so the re-run target is
   "sustain load=m comfortably + report best sustainable point toward load=l (~8000 w/s)".
