# Overview Tab

The **Overview** tab is your starting point for understanding the health and composition of your YugabyteDB cluster. It provides at-a-glance summary metrics and high-level visualizations of how data is distributed across zones, namespaces, and tables.

[← Back to Tablet Report Guide](README.md)

---

## Summary Cards

At the top of the Overview tab, eight summary cards provide key cluster-wide metrics. Each card can be hovered for additional context.

| Card | Description | What to Look For |
|------|-------------|------------------|
| **TServers** | Number of active tablet server nodes in the cluster at the time the report was captured. | Confirm this matches your expected node count. A missing TServer could indicate a node failure. |
| **Tables** | Total number of distinct YSQL/YCQL tables across all namespaces. This includes indexes, which are sharded like tables in YugabyteDB. | Useful as a reference for cluster complexity. Very high table counts increase metadata overhead. |
| **Unique Tablets** | The number of unique tablet partitions before replication. Each tablet adds Raft consensus and memory overhead per replica. | High counts (thousands+) increase CPU and memory usage. Monitor this if you are using auto-split aggressively. |
| **Total SST** | Total SST (compacted data) across **all replicas** on all nodes. To estimate logical data size, divide this by the replication factor (e.g., divide by 3 for RF=3). | This is total disk footprint for SST, not logical data. Compare with Total WAL to understand the SST-to-WAL ratio. |
| **Total WAL** | Total write-ahead log size across all replicas. WAL is periodically flushed to SST files during compaction. | Large WAL relative to SST may indicate heavy write throughput or pending compactions — this is not necessarily a problem on its own. |
| **Total Size** | Combined SST + WAL across all replicas. This represents the total disk footprint of your cluster. | This card is highlighted to draw attention to overall cluster storage consumption. |
| **Leaderless** | Number of tablets without an active leader at the time of capture. | Brief leaderless states during leader elections are normal. **Persistent leaderless tablets (>30 seconds) cause unavailability** for the affected partitions and need immediate investigation. This card turns **red** if the count is greater than 0. |
| **Node Skew** | A measure of data distribution imbalance across nodes, calculated as `(max_node_size - min_node_size) / avg_node_size × 100%`. | **Less than 15%** is typical for healthy clusters. Temporary spikes are normal during compaction or splits. **Greater than 20%** warrants investigation — see the [Balance Analysis Tab](balance_analysis_tab.md) for root cause details. This card turns **red** when skew exceeds 20%. |

---

## Charts

### Size by Zone

**Type:** Grouped Bar Chart

This chart compares SST and WAL storage consumption across availability zones (region/zone pairs). It helps you verify that data is evenly distributed across your zones, which is important for fault tolerance and performance.

**Data Points:**

| Axis | Data |
|------|------|
| X-axis | Zone labels in the format `region / zone` |
| Y-axis | Size in bytes (auto-scaled with human-readable labels) |
| Blue bars | SST size per zone |
| Orange bars | WAL size per zone |

**Hover Information:** Shows the zone name and the exact SST or WAL size in human-readable format (e.g., "42.5 GB").

**How to Interpret:**

- **Even bars across zones** means data is well distributed geographically — this is the healthy state.
- **One zone significantly larger** could indicate uneven table placement, a zone that recently absorbed data from a failed zone, or uneven tablet distribution.
- **Disproportionately large WAL** relative to SST in a zone may indicate heavy write activity or delayed compaction on nodes in that zone. This is informational, not necessarily a problem.

---

### Size by Namespace

**Type:** Horizontal Bar Chart

This chart shows the top 15 namespaces (databases/keyspaces) ranked by leader SST size. It reveals which databases own the most data in your cluster.

**Data Points:**

| Axis | Data |
|------|------|
| X-axis | SST size in bytes |
| Y-axis | Namespace names |
| Blue bars | Leader SST per namespace |

**Hover Information:** Displays the namespace name and exact SST size in human-readable format.

**How to Interpret:**

- This shows **data volume ownership**, not total disk usage. Multiply by the replication factor (RF) for the full replica footprint.
- A dominant namespace is normal if your workload is concentrated in one database.
- Use this to identify which databases are consuming the most storage and prioritize investigation accordingly.

---

### Storage Distribution

**Type:** Treemap (default) / Sunburst / Icicle — switchable via toggle buttons

This interactive visualization shows every table in the cluster, sized proportionally by its leader SST. Tables are grouped by namespace. It gives you an intuitive view of where your data lives.

**Visualization Modes:**

| Mode | Description |
|------|-------------|
| **Treemap** | Nested rectangles where area represents data size. Namespaces are top-level rectangles containing their tables. This is the default and most intuitive view. |
| **Sunburst** | Concentric rings — inner ring shows namespaces, outer ring shows tables. Area of each segment represents data size. |
| **Icicle** | Vertical partitioned layout — namespaces on top, tables below. Width represents data size. |

**Data Points:**

| Element | Data |
|---------|------|
| Each rectangle/segment | One table |
| Size/area | Proportional to leader SST size |
| Color | Varies by namespace — darker shades indicate larger tables within that namespace |
| Grouping | Tables are grouped under their parent namespace |

**Hover Information:** Displays the table name, namespace, SST size in human-readable format, tablet count, and percentage share of total cluster SST.

**Interactions:**

- **Click a namespace** to zoom into it and see only tables within that namespace.
- **Click "Reset Zoom"** (appears after zooming) to return to the full view.
- Use the **toggle buttons** above the chart to switch between Treemap, Sunburst, and Icicle views.

**How to Interpret:**

- **Very large rectangles** represent tables that dominate your cluster's storage. These are good candidates for reviewing tablet distribution in the [Balance Analysis Tab](balance_analysis_tab.md).
- **Very small tables** may not be visible in the Treemap. Switch to Sunburst view for better visibility of small tables.
- This reflects **all replicas** — single-replica size is available in the [Tables Tab](tables_tab.md).
- A cluster dominated by a few large tables is common and not inherently problematic, as long as those tables have enough tablets for parallelism.

---

## How to Use the Overview Tab for Analysis

1. **Start with the summary cards** — check TServer count, leaderless tablets, and node skew for any immediate red flags.
2. **Review Size by Zone** — confirm data is balanced across availability zones. Significant imbalance may affect fault tolerance.
3. **Check Size by Namespace** — identify which databases hold the most data. This helps prioritize investigation.
4. **Explore the Storage Distribution** — zoom into large namespaces to understand which tables dominate storage. Click through to the [Balance Analysis Tab](balance_analysis_tab.md) for deeper investigation of any concerning tables.

---

[Next: Nodes Tab →](nodes_tab.md)
