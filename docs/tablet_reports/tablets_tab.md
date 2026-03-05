# Tablets Tab

The **Tablets** tab focuses on individual tablets — the fundamental data distribution unit in YugabyteDB. Use this tab to understand how tablet sizes are distributed, whether replication is healthy, and if there are oversized tablets that might benefit from splitting.

[← Back to Tablet Report Guide](README.md)

---

## Charts

### Tablet Size Distribution

**Type:** Bar Chart

This chart groups **leader tablets** into predefined size ranges (buckets) to show the overall distribution of tablet sizes across the cluster. Only non-empty buckets are shown.

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | Size buckets: `<1 GB`, `1-2 GB`, `2-5 GB`, `5-10 GB`, `10-20 GB`, `20-50 GB`, `>50 GB` |
| Y-axis | Number of leader tablets in each bucket |
| Blue bars | Count of leader tablets falling into each size range |

**Hover Information:** Displays the size range label and exact tablet count.

**How to Interpret:**

- **Concentrated in a narrow range** (e.g., most tablets in the "1-2 GB" bucket) indicates uniform tablet sizes — this is the ideal state.
- **Wide spread across many buckets** is normal when your cluster has tables with very different data volumes. A small configuration table may have tablets under 1 GB, while a large fact table may have tablets in the 10-20 GB range. Focus on **intra-table skew** (variation within a single table) rather than cross-table variation.
- **Tablets in the >50 GB bucket** are exceptionally large and may be candidates for manual splitting. Check if auto-split is enabled and whether the split thresholds are appropriate.
- **Many tablets in the <1 GB bucket** is common for newly created tables, tables with little data, or tables that have been recently split.

---

### Replication Factor

**Type:** Donut (Pie) Chart

This chart shows the distribution of replication factors across all unique tablets in the cluster. It answers the question: "How many copies of each tablet exist?"

**Data Points:**

| Element | Data |
|---------|------|
| Segments | Each segment represents a distinct replication factor (e.g., RF 1, RF 2, RF 3) |
| Segment size | Proportional to the number of unique tablets with that replication factor |
| Labels | Show the RF value, tablet count, and percentage |

**Hover Information:** Displays the replication factor label, exact count of tablets, and percentage of total.

**How to Interpret:**

- **In a healthy RF=3 cluster**, you should see nearly all tablets at RF 3. This means every tablet has three copies spread across different nodes.
- **Tablets below the expected RF are under-replicated** and at risk of data loss if another node fails. For example, in an RF=3 cluster, tablets showing RF 2 have lost one replica.
- **Under-replication usually resolves automatically** once a failed node returns or a new one joins the cluster. The YugabyteDB master server detects under-replication and schedules new replicas.
- **Persistent under-replication** (lasting more than a few minutes) needs investigation:
  - Check if a TServer is permanently down.
  - Check if there's insufficient disk space to create new replicas.
  - Review master logs for replication errors.
- **RF 1 tablets** are particularly risky — a single node failure will cause data loss for those tablets.

---

### SST Size Histogram

**Type:** Histogram

A fine-grained histogram showing the distribution of individual leader tablet SST sizes with 50 bins. This provides more detail than the Tablet Size Distribution chart above.

**Data Points:**

| Axis / Element | Data |
|----------------|------|
| X-axis | SST size in gigabytes (GB) |
| Y-axis | Number of leader tablets in each bin |
| Blue bars | Frequency count — how many leader tablets fall into each size bin |
| Bin count | 50 evenly-spaced bins across the range of tablet sizes |

**Hover Information:** Displays the size value in GB (to 2 decimal places) and the count of tablets in that bin.

**How to Interpret:**

- **A tight bell curve** centered around a single value indicates uniform tablet sizes — ideal for balanced performance.
- **A long right tail** (a few bars extending far to the right) indicates a small number of oversized tablets. These may belong to tables that need more shards, or they may be tablets that haven't been split yet despite reaching the auto-split threshold.
- **Zero-size tablets** are normal for empty or newly-created tables and are excluded from this chart (only tablets with SST > 0 are shown).
- **Bimodal distribution** (two peaks) may indicate two groups of tables with different data volumes or compaction states.
- To investigate oversized tablets, use the [Balance Analysis Tab](balance_analysis_tab.md) → Top Skewed Tables chart and drill down into specific tables.

---

## Common Scenarios and What They Mean

| Observation | Likely Cause | Recommended Action |
|-------------|-------------|-------------------|
| Most tablets are under 1 GB | Small dataset or heavily pre-split tables | Normal. Avoid unnecessary splits as each tablet adds overhead. |
| A few tablets exceed 50 GB | Auto-split threshold may be too high, or auto-split is disabled | Check auto-split configuration. Consider manual splitting if needed. See [Balance Analysis](balance_analysis_tab.md) → Split Recommendations. |
| Some tablets are at RF 2 instead of RF 3 | A TServer is down or recently restarted | Under-replicated tablets should heal automatically. If persistent, check TServer health. |
| Histogram shows a long right tail | A few tables have disproportionately large tablets | Drill into those tables via [Balance Analysis](balance_analysis_tab.md) → Top Skewed Tables to identify candidates for splitting. |
| All tablets are RF 1 | Cluster configured with RF=1 (development mode) | Expected for development. Not recommended for production — no fault tolerance. |

---

[← Previous: Tables Tab](tables_tab.md) | [Next: Balance Analysis Tab →](balance_analysis_tab.md)
