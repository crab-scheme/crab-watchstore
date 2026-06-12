# Federation pattern: per-region clusters + a global discovery prefix (cw-lkq.11)

The stretched multi-region cluster (docs/operations.md) gives one linearizable
keyspace — and non-leader regions pay one WAN RTT per write. When regions must
WRITE locally (CRDB's "regional table" locality), invert the topology:

```
region us-east                 region eu-west
┌─────────────────────┐        ┌─────────────────────┐
│ k8s cluster A        │        │ k8s cluster B        │
│  └ apiserver         │        │  └ apiserver         │
│     └ LOCAL store    │        │     └ LOCAL store    │
│       (3 voters,     │        │       (3 voters,     │
│        LAN raft)     │        │        LAN raft)     │
└──────────┬──────────┘        └──────────┬──────────┘
           │      stretched GLOBAL store (3-5 voters     │
           └──── across regions; /global/** prefix ──────┘
                 service discovery, failover routing)
```

- Each Kubernetes cluster runs on its OWN crab-watchstore (LAN Raft: local
  write latency, the k8s cluster is the partition unit — kubernetes-the-API has
  no cross-cluster objects anyway).
- A SEPARATE stretched cluster (WAN profile from docs/operations.md) holds only
  the small, slow-changing global state under an agreed prefix: service
  registry entries, cluster directory, failover policy. Writes there are rare,
  so the WAN RTT is irrelevant; reads/watches are region-local (serializable +
  local watch serving).
- Controllers in each region watch `/global/**` on the stretched store and
  reconcile into their local cluster (the standard multi-cluster
  service-discovery shape: ExternalName/EndpointSlice projection).

When to choose which:
| | stretched (operations.md) | federated (this doc) |
|---|---|---|
| write latency in every region | 1 WAN RTT (non-leader regions) | LAN |
| single logical k8s control plane | yes | no (N clusters + projection) |
| survives a region loss | yes (quorum layout) | per-cluster (global store survives) |
| consistency across regions | linearizable everywhere | linearizable only under /global |

Example assets: `deploy/docker/docker-compose.yml` (a regional cluster),
`deploy/docker/wan.override.yml` (the stretched global store profile).
