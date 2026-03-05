# Tablet Report — User Guide

The **Tablet Report** is a powerful diagnostic tool in the YugabyteDB Support Bundle Analyzer that helps you understand how data is distributed across your YugabyteDB cluster at the tablet level. It provides visual insights into storage consumption, data balance, replication health, and potential hotspots — all derived from a point-in-time snapshot of your cluster.

---

## What Is a Tablet Report?

In YugabyteDB, every table is automatically split into **tablets** (also called shards). Each tablet holds a portion of a table's data and is replicated across multiple nodes for fault tolerance. The tablet report captures a snapshot of every tablet in your cluster — its size, location, leader status, and replication state — and presents it through interactive charts and tables.

Understanding tablet distribution is critical for:

- **Diagnosing uneven disk usage** across nodes
- **Identifying skewed tables** where some tablets are much larger than others
- **Evaluating replication health** (under-replicated or leaderless tablets)
- **Planning tablet splits** to improve parallelism
- **Investigating why one node is larger** than others

---

## How to Use This Feature

### Step 1: Generate the Tablet Report Database

Before you can visualize a tablet report, you need a SQLite database file generated from a YugabyteDB support bundle. This file is created by the `tablet_report_parser.pl` script that processes support bundle data and produces a `.sqlite` file.

### Step 2: Open the Tablet Report

There are two ways to open a tablet report:

**Option A — From the Home Page:**

1. Navigate to the YugabyteDB Support Bundle Analyzer home page.
2. Click **"Analyze Tablet Report"**.
3. In the dialog that appears, enter the full file path to your `.sqlite` database file (e.g., `/path/to/tablet-report.sqlite`).
4. Click **Load Report**.

**Option B — Direct URL:**

Navigate directly to:

```
http://linoln:5001/tablet-report?db=/path/to/tablet-report.sqlite
```

### Step 3: Explore the Tabs

Once loaded, the report displays six tabs. Each tab focuses on a different aspect of your cluster's tablet distribution:

| Tab | Purpose |
|-----|---------|
| [**Overview**](overview_tab.md) | High-level summary cards and storage distribution visualizations |
| [**Nodes**](nodes_tab.md) | Per-node data sizes, tablet counts, and a detailed node table |
| [**Tables**](tables_tab.md) | Top tables by size and a searchable table listing all tables |
| [**Tablets**](tablets_tab.md) | Tablet size distribution, replication factor breakdown, and histogram |
| [**Balance Analysis**](balance_analysis_tab.md) | Deep-dive into data skew, node imbalance, heatmaps, split recommendations, and per-table drilldowns |
| [**Cluster Info**](cluster_info_tab.md) | TServer and Master node details, version information |

---

## Key Concepts

Before diving into the tabs, it helps to understand a few terms used throughout the report:

| Term | Meaning |
|------|---------|
| **SST (Sorted String Table)** | Compacted, persisted data files on disk. This represents the actual data size after compaction. |
| **WAL (Write-Ahead Log)** | A log of recent writes that haven't yet been flushed and compacted into SST files. Large WAL relative to SST may indicate heavy write throughput or pending compactions. |
| **Leader Tablet** | The replica that serves reads and coordinates writes for a given tablet. Each tablet has exactly one leader at any time. |
| **Follower Tablet** | A replica that receives updates from the leader via Raft consensus. Followers provide fault tolerance and can become leaders if the current leader fails. |
| **Replication Factor (RF)** | The number of copies of each tablet maintained across the cluster. RF=3 means three copies exist. |
| **Skew** | The ratio between the largest and smallest tablet within a table. High skew means uneven data distribution within that table. |
| **Node Skew** | The percentage difference between the heaviest and lightest nodes relative to the average. Formula: `(max - min) / avg × 100`. |
| **Namespace** | A database (in YSQL) or keyspace (in YCQL) that contains tables. |

---

## Quick Troubleshooting Guide

| Symptom | Where to Look | What to Check |
|---------|---------------|---------------|
| One node has significantly more data | [Balance Analysis](balance_analysis_tab.md) → Waterfall chart | Identify which tables contribute most to the gap |
| High node skew (>20%) | [Balance Analysis](balance_analysis_tab.md) → Node Size Imbalance | Check if tables are spread across all nodes |
| Leaderless tablets detected | [Overview](overview_tab.md) → Summary cards | Brief leaderless states are normal; persistent ones need investigation |
| Some tablets are very large | [Tablets](tablets_tab.md) → SST Size Histogram | Long right tail indicates split candidates |
| Uneven tablet sizes within a table | [Balance Analysis](balance_analysis_tab.md) → Top Skewed Tables | Click a bar to see per-tablet details |
| Under-replicated tablets | [Tablets](tablets_tab.md) → Replication Factor chart | Tablets below expected RF need investigation |

---

## Tab Documentation

For detailed documentation on each tab, see:

1. [Overview Tab](overview_tab.md)
2. [Nodes Tab](nodes_tab.md)
3. [Tables Tab](tables_tab.md)
4. [Tablets Tab](tablets_tab.md)
5. [Balance Analysis Tab](balance_analysis_tab.md)
6. [Cluster Info Tab](cluster_info_tab.md)
