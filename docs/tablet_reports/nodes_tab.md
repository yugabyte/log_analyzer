# Nodes Tab

The **Nodes** tab provides a detailed view of how data and tablets are distributed across individual TServer nodes. Use this tab to identify nodes that are carrying disproportionate storage load or tablet counts.

[← Back to Tablet Report Guide](README.md)

---

## Charts

### Data Size per Node

**Type:** Stacked Bar Chart

This chart shows the total on-disk footprint of each TServer node, with SST and WAL stacked to show their relative contribution.

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | Node labels in the format `IP Address` + `Zone` |
| Y-axis | Size in bytes (auto-scaled) |
| Blue segment (bottom) | SST size on that node |
| Orange segment (top) | WAL size on that node |

**Hover Information:** Displays the node identifier and exact SST or WAL size in human-readable format.

**How to Interpret:**

- **Even bar heights** across all nodes indicates a healthy, balanced cluster.
- **Small differences** (within 10-15%) are normal due to compaction timing and leader placement. Compaction is an asynchronous process and nodes may temporarily hold different amounts of SST data.
- **Persistent large gaps (>20%)** may indicate:
  - Tables that are not spread across all nodes (e.g., tables with fewer tablets than nodes)
  - Uneven tablet distribution after recent topology changes
  - Ongoing tablet splits or load balancing
- **Disproportionately large WAL** on a specific node may indicate heavy write activity directed to that node, or delayed compaction.
- Use the [Balance Analysis Tab](balance_analysis_tab.md) → Waterfall chart to understand which tables contribute to the size difference between nodes.

---

### Tablets per Node

**Type:** Bar Chart

This chart shows how many tablet replicas are hosted on each node. An even distribution is desirable because YugabyteDB's load balancer aims to distribute tablets equally across nodes.

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | Node IP addresses |
| Y-axis | Number of tablets |
| Teal bars | Total tablet count (leaders + followers) per node |
| Indigo bars | Leader-only count (when "Leader Only" is enabled) |

**Controls:**

- **Leader Only checkbox** — Toggle to show only leader tablets per node. This is useful for understanding which nodes are handling the write workload, since leaders coordinate all writes for their tablet.

**Hover Information:** Displays the node IP and the tablet count (or leader count if toggled).

**How to Interpret:**

- **Even bar heights** means the load balancer is working as expected.
- **Minor variance** is normal during topology changes, tablet splits, or when some tables have fewer tablets than there are nodes.
- **Persistent imbalance** (one node has significantly more tablets) warrants investigation:
  - Check if a node recently joined or restarted — the load balancer takes time to redistribute.
  - Check if there are placement constraints that prevent even distribution.
- **Leader-only view** reveals if leader tablets are unevenly distributed. Even when total tablet counts are balanced, an uneven leader distribution can cause I/O hotspots since leaders handle all reads and coordinate all writes.

---

## Table

### Node Details

A comprehensive table showing key metrics for every TServer node in the cluster.

| Column | Description |
|--------|-------------|
| **IP** | The IP address of the TServer node. |
| **Region** | The cloud region where the node is deployed (e.g., `us-east-1`, `us-west-2`). |
| **Zone** | The availability zone within the region (e.g., `us-east-1a`). |
| **Tablets** | Total number of tablet replicas (leaders + followers) hosted on this node. |
| **Leaders** | Number of leader tablets on this node. Leaders serve reads and coordinate writes. |
| **SST** | Total SST (compacted data) size on this node, in human-readable format (e.g., "125.3 GB"). |
| **WAL** | Total WAL (write-ahead log) size on this node, in human-readable format. |
| **Total** | Combined SST + WAL size — the total disk footprint on this node. |

**How to Interpret:**

- Compare the **Total** column across nodes. In a healthy cluster, all nodes should have roughly similar totals.
- Compare **Leaders** across nodes. An even leader distribution ensures balanced I/O workload. YugabyteDB's load balancer distributes leaders by count, not data size.
- A node with significantly more **Tablets** than others may be in the process of rebalancing, or may indicate a configuration issue.
- Nodes in different **Regions/Zones** should ideally carry similar data loads, assuming your tables have placement policies that span all zones.

---

## Common Scenarios and What They Mean

| Observation | Likely Cause | Recommended Action |
|-------------|-------------|-------------------|
| One node has 2x the Total size | Large tables not spread to all nodes, or uneven tablet sizes | Check [Balance Analysis](balance_analysis_tab.md) → Waterfall chart and Tables Not on All Nodes |
| Leader counts are uneven | Load balancer in progress, or recent leader failover | Usually resolves automatically. If persistent (>10 minutes), check TServer logs. |
| WAL is very large on one node | Heavy write workload or delayed compaction | Check if compaction is running. Large WAL may be transient. |
| A node has 0 leaders | Node may be unhealthy or recently restarted | Check TServer status and logs. Leaders should redistribute automatically. |

---

[← Previous: Overview Tab](overview_tab.md) | [Next: Tables Tab →](tables_tab.md)
