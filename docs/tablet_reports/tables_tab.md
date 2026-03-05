# Tables Tab

The **Tables** tab gives you a table-level view of storage consumption in your cluster. Use it to identify the largest tables and understand their storage characteristics.

[← Back to Tablet Report Guide](README.md)

---

## Charts

### Top 30 Tables by SST

**Type:** Horizontal Bar Chart

This chart shows the 30 largest tables in your cluster, ranked by total SST size across all replicas.

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | SST size in bytes (auto-scaled) |
| Y-axis | Table names (truncated to 40 characters if needed) |
| Blue bars | Total SST across all replicas for each table |

**Hover Information:** Displays the full table name, the namespace it belongs to, exact SST size in human-readable format, and the number of unique tablets.

**How to Interpret:**

- **A dominant table** at the top is not necessarily a problem. What matters is whether its tablets are evenly distributed across nodes. Use the [Balance Analysis Tab](balance_analysis_tab.md) to check distribution.
- **Index tables** may appear alongside their parent tables. In YugabyteDB, indexes are sharded like tables and can consume significant storage.
- **System tables** (from `system` or `system_schema` namespaces) typically appear small. If a system table is unexpectedly large, it may warrant investigation.
- Before deciding to split a large table, check the Balance Analysis tab — more tablets means more Raft consensus overhead per node.

---

## Table

### All Tables

A searchable, comprehensive table listing every table in your cluster with storage details.

**Filter:**

A text input at the top allows you to filter the table by namespace or table name. Simply type any part of the name to narrow down the list.

| Column | Description |
|--------|-------------|
| **Namespace** | The database (YSQL) or keyspace (YCQL) that the table belongs to. |
| **Table** | The name of the table. This includes user tables, indexes, and system tables. |
| **Tablets** | The number of unique tablet partitions for this table (before replication). Each unique tablet is replicated RF times. |
| **SST** | Total SST size across **all replicas** in human-readable format (e.g., "45.2 GB"). This is the total disk space consumed by this table's compacted data files across the entire cluster. |
| **SST (RF1)** | Estimated single-replica SST size, calculated as `total SST / replication factor`. This represents the approximate logical data size of the table. |
| **WAL** | Total WAL size across all replicas. This is the write-ahead log data that hasn't been compacted yet. |
| **Total** | Combined SST + WAL across all replicas — the full disk footprint of this table in the cluster. |

**How to Interpret:**

- **SST vs. SST (RF1):** The SST column shows total storage across all replicas. SST (RF1) shows the estimated single-replica size. For a table with RF=3, `SST ≈ SST(RF1) × 3`. Use SST (RF1) when you want to understand the actual data volume.
- **Large WAL relative to SST:** If WAL is disproportionately large for a table, it may be experiencing heavy write activity or pending compaction.
- **Tablet count:** Tables with very few tablets (e.g., 1-3) on a large cluster may not be evenly distributed across all nodes. Tables with too many tablets increase Raft and memory overhead.
- **Sorting:** Click column headers to sort the table by any column. This helps identify outliers quickly.
- **Filtering:** Use the search box to find specific tables. This is especially useful in clusters with hundreds of tables.

---

## Common Scenarios and What They Mean

| Observation | Likely Cause | Recommended Action |
|-------------|-------------|-------------------|
| A table's SST is much larger than others | Normal for tables with more data or indexes | Check its tablet distribution in [Balance Analysis](balance_analysis_tab.md) |
| SST (RF1) doesn't match expected data size | May include tombstones, or compaction hasn't caught up | Check recent DELETE operations or compaction status |
| A table has very few tablets (1-2) | Default tablet count or pre-split configuration | Consider splitting if the table is large (see [Balance Analysis](balance_analysis_tab.md) → Split Recommendations) |
| WAL is large compared to SST for a table | Heavy write activity or delayed compaction | Usually resolves after compaction completes |
| An index table is very large | Dense index on a large table | Normal behavior — indexes are full tables in YugabyteDB |

---

[← Previous: Nodes Tab](nodes_tab.md) | [Next: Tablets Tab →](tablets_tab.md)
