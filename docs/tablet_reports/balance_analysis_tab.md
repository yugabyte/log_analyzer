# Balance Analysis Tab

The **Balance Analysis** tab is the most detailed and powerful section of the tablet report. It provides deep diagnostics for understanding data skew, identifying imbalanced tables, evaluating split opportunities, and drilling down to individual tablet sizes. If you suspect a data distribution issue, this is where you should spend most of your time.

[← Back to Tablet Report Guide](README.md)

---

## Balance Summary Banner

At the top of this tab, a colored banner provides an instant health assessment of your cluster's data balance.

| Status | Color | Condition | Meaning |
|--------|-------|-----------|---------|
| **Cluster is well balanced** | Green | Node skew ≤ 15% | Data is evenly distributed. No action needed. |
| **Moderate data skew detected** | Yellow/Amber | Node skew between 15% and 30% | Some imbalance exists. Review the charts below to understand the cause. May be transient. |
| **Significant data skew detected** | Red | Node skew > 30% | Significant imbalance. Investigate the root cause using the tools below. |

The banner also displays:

- **Node range:** The smallest and largest node sizes (SST + WAL)
- **Average:** The mean node size
- **Skew percentage:** The `(max - min) / avg × 100` metric
- **Leaderless count:** (If any) Highlighted in red

---

## Charts and Sections

### Auto-Split Phase

**Type:** Information Panel

This section shows the current automatic tablet splitting phase for the cluster. YugabyteDB uses a multi-phase auto-split algorithm that adjusts splitting aggressiveness based on the number of shards per node.

| Phase | Condition | Split Threshold | Description |
|-------|-----------|----------------|-------------|
| **Low** (Green) | < 1 shard/node | 128 MiB | Aggressive splitting. The cluster has very few tablets per node, so the system splits early to improve parallelism. This phase completes once the table reaches approximately 8 tablets per node. |
| **High** (Orange) | 1–24 shards/node | 10 GiB | Moderate splitting. Distribution is already reasonable. The system avoids over-splitting to limit Raft consensus overhead. Continues until approximately 24 shards per node. |
| **Final** (Red) | ≥ 24 shards/node | 100 GiB | Conservative splitting. Only very large tablets (>100 GiB) auto-split. To split tablets below this threshold, use manual splitting (`yb-admin split_tablet`) or lower the `tablet_force_split_threshold_bytes` flag. |

**Displayed Metrics:**

- **Shards per node:** Total tablet replicas (leaders + followers) divided by TServer count
- **Current split threshold:** The SST size above which tablets auto-split
- **Leader tablets above threshold:** How many leader tablets currently exceed the auto-split threshold
- **Tablets in 10–100 GB range (Final phase only):** Warning count of tablets that are large but won't auto-split at current settings

**How to Interpret:**

- If you're in the **Final phase** and see tablets in the 10–100 GB range that aren't splitting, consider whether:
  - The current threshold is appropriate for your workload
  - Manual splitting would be beneficial
  - The `tablet_force_split_threshold_bytes` flag should be lowered
- Auto-split phases are **per table** in practice. The phase shown here is based on the cluster-wide average.

---

### Leader Data Weight per Node

**Type:** Bar Chart

This chart shows the total SST held by **leader tablets only** on each node. This is a critical metric because YugabyteDB's load balancer distributes leaders by **count**, not by **data size**. So even when each node has the same number of leaders, the actual I/O workload may differ dramatically if some leaders hold much more data than others.

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | Node IP addresses |
| Y-axis | Total leader SST in bytes |
| Bar color | **Blue** = within 15% of average; **Red** = more than 15% above average; **Green** = more than 15% below average |
| Bar labels | Human-readable size displayed above each bar |

**Hover Information:** Displays the node IP, exact leader SST size in human-readable format, and the number of leader tablets on that node.

**How to Interpret:**

- **Even bars (all blue)** indicate balanced leader data weight — ideal.
- **Red bars** (above average) indicate nodes carrying a heavier read/write workload. These nodes may experience higher CPU, I/O, and memory pressure.
- **Green bars** (below average) are underutilized relative to others.
- **Imbalance with equal leader counts** means some leaders hold much larger tablets than others. This is a hidden performance issue that isn't visible from leader count metrics alone.
- To address leader weight imbalance, investigate which tables contribute the most — use the Waterfall chart and Heatmap below.

---

### Why Is One Node Bigger? (Waterfall Chart)

**Type:** Waterfall Chart

This is one of the most useful charts in the entire report. It answers the question: "Why does Node A have more data than Node B?" by showing the table-by-table SST contributions to the size gap.

**Controls:**

- **Node A dropdown:** Select the first node to compare
- **Node B dropdown:** Select the second node to compare
- **Swap button:** Quickly swap the two selected nodes
- By default, Node A is set to the heaviest node and Node B to the lightest

**Data Points:**

| Element | Data |
|---------|------|
| X-axis labels | Table names (top 25 contributors to the gap) |
| Y-axis | SST size difference (`Node A SST - Node B SST`) for each table |
| Red bars (increasing) | Tables where Node A has more data than Node B |
| Green bars (decreasing) | Tables where Node B has more data than Node A |
| Blue bar ("Net Gap") | The total cumulative gap between the two nodes |
| Bar labels | Human-readable size of each difference |

**Hover Information:** Displays the full table name, the difference in human-readable format, the SST size on Node A, and the SST size on Node B.

**How to Interpret:**

- **Red bars at the left** show the biggest contributors to why Node A is larger. Focus investigation on these tables first.
- **Green bars** partially offset the gap — these are tables where Node B has more data.
- **The "Net Gap" bar** at the right shows the total difference.
- **Common causes of table-level gaps:**
  - The table has fewer tablets than nodes, so not all nodes have replicas
  - Uneven tablet sizes within the table (skew)
  - Recent tablet splits that haven't fully rebalanced
  - Ongoing compaction on one node that temporarily reduces its SST
- Select **different node pairs** to investigate other relationships. The gap may be caused by different tables for different node combinations.

---

### Table × Node SST (Heatmap Table)

**Type:** Data Table with Color-Coded Cells

This table shows the SST size of each major table on each node. Cells are color-coded using a yellow-orange-red intensity scale — darker cells indicate more data on that node.

**Controls:**

- **Sort by:** Dropdown to sort rows by Total Size, Spread (max-min), or Node Count
- **Leader Only checkbox:** Toggle to show only leader tablet SST (hides follower data)
- **Column header clicks:** Click "Total", "Spread", or "Nodes" column headers to sort

| Column | Description |
|--------|-------------|
| **Table** | The fully qualified table name (`namespace.table_name`) |
| **Total** | Total SST across all nodes for this table |
| **Spread** | The difference between the maximum and minimum SST across nodes for this table. High spread indicates uneven distribution. |
| **Nodes** | How many nodes have replicas of this table, shown as `X/Y` where Y is total TServers. A warning icon appears if the table is not on all nodes. |
| **Node columns (one per IP)** | SST size of this table on each specific node. Dash (`–`) means no replicas on that node. |

**Cell Colors:**

- **Light/white:** Small or zero SST on that node
- **Yellow:** Moderate SST
- **Orange:** Significant SST
- **Dark red:** Very high SST (relative to the global maximum across all cells)

**How to Interpret:**

- **Even shading across a row** means the table is well distributed across nodes.
- **Uneven shading within a row** means some nodes hold more data for that table — investigate tablet-level skew.
- **Dash cells (`–`)** mean no replicas of that table exist on that node. This is expected when `tablet_count × RF < node_count` (e.g., a table with 3 tablets at RF=3 on a 9-node cluster). It becomes a concern if a large table is concentrated on only a few nodes.
- **Sort by Spread** to find tables with the most uneven distribution.
- **Sort by Node Count** to find tables that aren't spread across all nodes.

---

### Tables Not on All Nodes

**Type:** Data Table

This section appears only when there are tables with replicas on fewer nodes than the total number of TServers. Only tables larger than 1 GB are shown.

| Column | Description |
|--------|-------------|
| **Table** | The fully qualified table name |
| **Total SST** | Total SST across all replicas |
| **Nodes Present** | Number of nodes that have replicas, shown in red as `X / Y` |
| **Absent From** | Comma-separated list of node IPs that do not have any replicas of this table |

**How to Interpret:**

- **Expected when `tablet_count × RF < node_count`:** For example, a table with 3 tablets at RF=3 on a 9-node cluster has 9 total replicas, but each node may only host 1 replica, meaning all nodes are covered. However, a table with 2 tablets at RF=3 has only 6 replicas, so 3 nodes will have no data for this table.
- **Becomes a concern** when a large table is concentrated on a subset of nodes, contributing to disk imbalance.
- **Possible actions:**
  - Increase the tablet count for the table (via split or manual pre-splitting)
  - Adjust placement policies if using geo-partitioned deployments

---

### Tablet Split Recommendations

**Type:** Data Table with Warning Banner

This section provides **advisory estimates** for tables that may benefit from additional tablet splits. The recommendations target approximately 10 GB per tablet (RF1), which is a general guideline.

**Important Warning (displayed above the table):**

Before acting on these recommendations:

- **Tablet merging is not supported** — splits are permanent and irreversible
- Each additional tablet adds Raft consensus and memory overhead per replica
- Splitting triggers post-split compaction, temporarily increasing CPU and disk I/O
- Check if auto-split is enabled (`enable_automatic_tablet_splitting`) and whether lowering the threshold flag is a better approach
- The recommended count targets ~10 GB/tablet — your workload may have different optimal sizes

| Column | Description |
|--------|-------------|
| **Namespace** | Database/keyspace name |
| **Table** | Table name |
| **SST (RF1)** | Estimated single-replica SST size |
| **Avg Tablet Size** | Current average tablet size (total SST / tablet count / RF) |
| **Current Tablets** | Number of unique tablets currently |
| **Suggested** | Recommended number of tablets (targeting ~10 GB each). Color-coded: **Green** (≤2x current), **Orange** (2-5x), **Red** (>5x). |
| **Multiplier** | Ratio of suggested to current tablets. Color-coded same as Suggested. |
| **RF** | Replication factor of the table |

**How to Interpret:**

- **Green multiplier (≤2x):** Modest increase recommended. Low risk.
- **Orange multiplier (2-5x):** Significant increase. Evaluate the overhead cost carefully.
- **Red multiplier (>5x):** Major increase. Strongly consider whether auto-split with adjusted thresholds would be more appropriate.
- These are **starting points**, not prescriptions. Always consider your specific workload, query patterns, and the overhead of additional tablets before splitting.

---

### Node Size Imbalance

**Type:** Data Table with Visual Bars

This table shows how each node's SST size deviates from the cluster average, with visual indicators.

| Column | Description |
|--------|-------------|
| **Node** | IP address of the TServer. Tagged with **MAX** (highest SST) or **MIN** (lowest SST) badges. |
| **Zone** | Availability zone |
| **Tablets** | Total tablet count with percentage deviation from average in parentheses |
| **Leaders** | Leader tablet count with percentage deviation from average in parentheses |
| **SST Size** | Total SST in human-readable format |
| **Deviation** | Percentage deviation from average SST. **Red** for positive deviation (above average), **Green** for negative (below average), **Gray** for within ±5%. |
| **vs Average** | A visual bar chart showing the node's SST relative to the average (1.0x). A vertical line marks the average. Bar color: **Red** (above average), **Green** (below average), **Blue** (near average). The ratio (e.g., "1.15x") is displayed alongside. |

**Footer Row:** Shows the cluster average for each metric.

**How to Interpret:**

- **Deviation within ±5%** is healthy and expected.
- **Deviation between 5-15%** is common during compaction cycles or after recent tablet operations.
- **Deviation >20%** is significant and warrants investigation using the Waterfall chart and Heatmap above.
- The **Tablets** and **Leaders** deviation percentages help identify whether the imbalance is due to tablet distribution or data volume per tablet:
  - If Tablets deviation matches SST deviation → the issue is tablet count distribution
  - If SST deviation is high but Tablets deviation is low → some tablets are much larger than others

---

### Top Skewed Tables

**Type:** Horizontal Bar Chart (Clickable)

This chart ranks tables by their intra-table skew — the ratio of the largest leader tablet's SST to the smallest. High skew means some tablets within the table hold much more data than others.

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | Skew ratio (`max leader tablet SST / min leader tablet SST`) |
| Y-axis | Table names (top 30, truncated to 40 characters) |
| Bar colors | **Green** (skew ≤ 3x), **Orange** (skew 3-10x), **Red** (skew > 10x) |

**Hover Information:** Displays the full table name, skew ratio, minimum tablet SST, maximum tablet SST, and average tablet SST.

**Interactions:**

- **Click a bar** to drill down into that table's individual tablet sizes (see Drilldown section below).

**How to Interpret:**

- **Skew ratio 1-3x:** Normal variation. No action needed.
- **Skew ratio 3-10x:** Noticeable imbalance. Common with range-sharded tables or non-uniform key distributions.
- **Skew ratio >10x:** Significant hotspot risk. Some tablets may be handling disproportionate I/O. Click the bar to investigate per-tablet sizes.
- **Common causes of skew:**
  - Range-sharded tables with non-uniform key distribution
  - Recent tablet splits (new tablets start small)
  - Sequential writes to a subset of key ranges
  - Hash-sharded tables with a skewed hash function (rare)

---

## Table Drilldown Section

When you click a bar in the Top Skewed Tables chart or use the table search dropdown, a detailed drilldown section appears. This section provides four views of the selected table's tablet-level data.

### Table Search Dropdown

A searchable dropdown that lists all tables in the cluster. Type to filter, and select a table to drill into it. Tables from the skew analysis are listed first with their skew ratio.

---

### Tablet Count by Size Range

**Type:** Bar Chart

Shows leader tablets of the selected table grouped into dynamically calculated size buckets.

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | Dynamically computed size ranges (e.g., "0 – 100 MB", "100 MB – 500 MB", "500 MB – 1 GB", etc.) |
| Y-axis | Number of leader tablets in each range |
| Blue bars | Count of tablets per bucket |
| Bar labels | Exact count displayed above each bar |

**How to Interpret:**

- **Concentrated in one bucket:** Tablets are uniformly sized — ideal.
- **Spread across many buckets:** Some tablets are much larger or smaller than others — indicates intra-table skew.
- Buckets are computed dynamically based on the actual data range, so they adapt to the specific table being analyzed.

---

### Tablet Storage Treemap

**Type:** Treemap (default) / Sunburst / Icicle — switchable via toggle buttons

An interactive visualization showing every tablet replica grouped by node. Each shape represents a single tablet replica, sized by its SST.

**Controls:**

- **Treemap / Sunburst / Icicle toggle:** Switch between visualization modes
- **Leader Only checkbox:** Show only leader tablet replicas
- **Reset Zoom button:** Return to full view after clicking into a node

**Data Points:**

| Element | Data |
|---------|------|
| Top-level groups | Nodes (labeled by IP and zone) |
| Individual shapes | Tablet replicas |
| Shape size | Proportional to SST size |
| Shape color | **Blue shades** for leader tablets, **Orange shades** for follower tablets. Darker shades indicate larger tablets within a node. |

**Hover Information:** Displays the tablet UUID (first 12 characters), the node label, exact SST size, and whether the tablet is a leader or follower.

**How to Interpret:**

- **Even shape sizes within each node group** means tablets are uniformly distributed.
- **One large shape dominating a node** indicates a large tablet that could be a hotspot.
- **Leader Only mode** helps you focus on the tablets that handle reads and writes.
- Compare the total area across nodes — larger areas mean that node holds more data for this table.

---

### Per-Node Breakdown

**Type:** Stacked Bar Chart

Shows the SST contribution of the selected table on each node, split into leader and follower components.

**Controls:**

- **Leader Only checkbox:** Show only leader tablet SST per node

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | Node IP addresses |
| Y-axis | SST size in bytes |
| Blue segments | Leader tablet SST on each node |
| Orange segments | Follower tablet SST on each node |

**Hover Information:** Displays the node IP, SST size, and tablet count for leaders/followers separately.

**How to Interpret:**

- **Even bars** mean the table's data is well distributed across nodes.
- **Missing bars** (a node with no data) means the table doesn't have replicas on that node — check if the table has enough tablets to cover all nodes.
- **Significant leader-follower imbalance** on a single node could indicate that node is serving a disproportionate number of leader tablets for this table.

---

### Individual Tablet Sizes

**Type:** Bar Chart

Displays every tablet replica as an individual bar, sorted from largest to smallest. This is the most granular view available.

**Controls:**

- **Leader Only checkbox:** Show only leader replicas

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | Tablet number (sequential index, sorted by size descending) |
| Y-axis | SST size in bytes |
| Blue bars | Leader tablets |
| Orange bars | Follower tablets |

**Hover Information:** Displays the tablet UUID (truncated), exact SST size, leader/follower status, and which node hosts the replica.

**How to Interpret:**

- **Flat horizontal line** of bars means all tablets are similar in size — ideal.
- **Sharp drop-off** (a few tall bars followed by much shorter ones) indicates significant skew — the tall bars are potential hotspots.
- **Step pattern** (groups of bars at the same height) may indicate tablets that serve similar key ranges with similar data volumes.
- Up to **200 replicas** are shown. For tables with more replicas, the smallest ones are truncated.

---

### Region/Zone Distribution

**Type:** Data Table

This table at the bottom of the Balance Analysis tab shows how tablet replicas are distributed across regions and zones.

| Column | Description |
|--------|-------------|
| **region** | Cloud region name |
| **zone** | Availability zone within the region |
| **tservers** | Number of TServers in this zone |
| **missing_replicas** | Number of tablets that should have a replica in this zone but don't |
| **1_replicas, 2_replicas, ...** | Count of tablets with exactly 1, 2, etc. replicas in this zone |
| **balanced** | Whether the zone's replica distribution is considered balanced |

**How to Interpret:**

- **missing_replicas > 0** indicates under-replication in that zone, possibly due to a failed TServer.
- **balanced = Yes** for all zones is the healthy state.
- Unbalanced zones may be recovering from recent node failures or topology changes.

---

## Analytical Workflow for Balance Analysis

Here is a recommended workflow for investigating data balance issues:

1. **Check the Balance Summary Banner** — if green, your cluster is healthy. If yellow or red, proceed below.
2. **Review Leader Data Weight** — are some nodes carrying significantly more leader data?
3. **Use the Waterfall Chart** — select the heaviest and lightest nodes to see which tables contribute to the gap.
4. **Check the Heatmap Table** — sort by Spread to find tables with uneven node distribution.
5. **Review Tables Not on All Nodes** — are large tables concentrated on a subset of nodes?
6. **Check Top Skewed Tables** — click into tables with high skew to see per-tablet details.
7. **Review Split Recommendations** — but treat them as advisory. Consider auto-split settings first.
8. **Check Node Size Imbalance** — correlate SST deviation with tablet count deviation to understand root cause.

---

[← Previous: Tablets Tab](tablets_tab.md) | [Next: Cluster Info Tab →](cluster_info_tab.md)
