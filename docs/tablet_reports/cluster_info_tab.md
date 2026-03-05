# Cluster Info Tab

The **Cluster Info** tab provides reference information about the cluster topology captured in the tablet report. It lists every TServer and Master node along with their locations and status. Use this tab to verify the cluster configuration and correlate node identities with the data shown in other tabs.

[← Back to Tablet Report Guide](README.md)

---

## Version Information

At the top of the tab, a row of metadata shows details about the tablet report itself:

| Field | Description |
|-------|-------------|
| **Parser** | The name of the program that generated the tablet report SQLite database (typically `tablet_report_parser.py`). |
| **Version** | The version of the parser tool used. |
| **Run on** | The date and time when the report was generated. This is the point-in-time snapshot date. |
| **Host** | The hostname of the machine where the parser was run. |

This information is useful for:

- Confirming **when** the snapshot was taken (all data in the report reflects that specific moment)
- Verifying which **version** of the parser was used, in case of known issues or feature differences
- Cross-referencing with support tickets or internal records

---

## Tables

### TServers

A table listing every tablet server (TServer) node in the cluster. TServers are the worker nodes that host tablet replicas and serve read/write requests.

| Column | Description |
|--------|-------------|
| **IP** | The IP address of the TServer. Use this to correlate with node-level charts in the [Nodes Tab](nodes_tab.md) and [Balance Analysis Tab](balance_analysis_tab.md). |
| **UUID** | The unique identifier assigned to this TServer instance. UUIDs are stable across restarts and are used internally by YugabyteDB to track node identity. |
| **Region** | The cloud region where this TServer is deployed (e.g., `us-east-1`, `europe-west1`). |
| **Zone** | The availability zone within the region (e.g., `us-east-1a`, `europe-west1-b`). Zone placement affects replica distribution for fault tolerance. |
| **Port** | The RPC port the TServer listens on (typically `9100`). |
| **Uptime** | How long the TServer has been running since its last restart. A dash (`–`) indicates the uptime was not available in the support bundle. |

**How to Interpret:**

- **Verify the expected TServer count** — if you expect 9 TServers but only see 8, a node may be down.
- **Check Region/Zone distribution** — ensure nodes are spread across zones as intended by your deployment topology. Uneven zone placement can affect data distribution and fault tolerance.
- **Review Uptime** — recently restarted nodes (short uptime) may still be rebalancing tablets. This can cause temporary skew visible in the [Nodes Tab](nodes_tab.md) and [Balance Analysis Tab](balance_analysis_tab.md).
- **Node UUIDs** are useful when correlating with YugabyteDB logs, `yb-admin` commands, or internal diagnostics where nodes are referenced by UUID rather than IP.

---

### Masters

A table listing every Master node in the cluster. Masters manage cluster metadata, coordinate tablet placement, handle DDL operations, and manage the tablet load balancer.

| Column | Description |
|--------|-------------|
| **IP** | The IP address of the Master node. |
| **UUID** | The unique identifier for this Master instance. |
| **Region** | The cloud region where this Master is deployed. |
| **Zone** | The availability zone within the region. |
| **Port** | The RPC port the Master listens on (typically `7100`). |
| **Role** | The Raft consensus role of this Master. Possible values: **LEADER** (the active master handling metadata operations), **FOLLOWER** (a standby that replicates metadata), or a dash (`–`) if the role was not captured. |

**How to Interpret:**

- **Exactly one Master should be LEADER** — this is the active master that handles schema changes, tablet placement decisions, and load balancing coordination.
- **Other Masters should be FOLLOWER** — they replicate metadata and can become leader if the current leader fails.
- **Masters should be in different zones** for fault tolerance. If all masters are in the same zone, a zone outage could lose the master quorum.
- **Master count** is typically 3 or 5 for production clusters (odd number for Raft consensus quorum). If you see fewer masters than expected, investigate.

---

## Common Scenarios and What They Mean

| Observation | Likely Cause | Recommended Action |
|-------------|-------------|-------------------|
| Fewer TServers than expected | A node is down or excluded from the support bundle | Check cluster status via `yb-admin list_all_tablet_servers` |
| A TServer has very short uptime | Recently restarted (manually or due to crash) | Data may be rebalancing — check [Nodes Tab](nodes_tab.md) for temporary skew |
| No Master has the LEADER role | Master election in progress, or masters are unhealthy | Check master logs and ensure master quorum is available |
| All nodes are in the same zone | Single-zone deployment | Acceptable for dev/test; not recommended for production (no zone-level fault tolerance) |
| TServers span many regions | Multi-region deployment | Expect higher latency for cross-region replication. Verify geo-partitioning is configured correctly. |

---

[← Previous: Balance Analysis Tab](balance_analysis_tab.md) | [Back to Tablet Report Guide →](README.md)
