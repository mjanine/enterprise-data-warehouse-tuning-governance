# Enterprise Data Warehouse Performance Tuning, Governance & Data Mart Isolation

## 📌 Overview
An enterprise-grade data engineering project focused on structural performance tuning, multi-engine data governance enforcement, zero-downtime database maintenance, and isolated departmental data mart creation. Applied directly to a production Streaming Media Analytics Warehouse (`warehouse.db`), this architecture achieved up to 81.26% latency reductions through strategic B-tree indexing and completely decoupled high-throughput analytics from core transactional workloads.

## 🛠 Tech Stack
* **Relational Database & Tuning:** SQLite 3 (WAL Mode, In-Memory Caching, B-Tree Indexing, Native Backup API)
* **NoSQL Governance:** MongoDB (`pymongo`) with `$jsonSchema` document validation
* **Data Engineering & Mart ETL:** Python 3.x, pandas, SQL Common Table Expressions (CTEs)
* **Performance Benchmarking:** `EXPLAIN QUERY PLAN`, Microsecond Execution Timers

## ⚡ Core Architecture & Engineering Highlights

### 1. Performance Tuning & Zero-Downtime Infrastructure (`lab_optimize.py`)
* **PRAGMA Optimizations:** Enforced Write-Ahead Logging (`PRAGMA journal_mode = WAL;`) for non-blocking concurrent reads and writes, allocated a 64MB dedicated RAM cache (`PRAGMA cache_size = -64000;`), directed temp sorting buffers to memory, and enabled foreign keys.
* **Online Backup Automation:** Utilized SQLite's native incremental online backup API (`sqlite3.Connection.backup`) to perform live database snapshots (`warehouse_backup.db`) in **8.42 ms** with zero table locking or operational downtime.
* **B-Tree Indexing Strategy:** Implemented 12 custom single-column and compound B-tree indexes (`idx_watch_logs_user_date`, `idx_catalog_type_year`, covering indexes), achieving 100% elimination of full table scans (`SCAN TABLE`) across all analytical benchmark queries.

### 2. Query Execution & Latency Benchmarks (`EXPLAIN QUERY PLAN`)
* **Temporal Range & Completion Filtering (Q3):** Reduced query execution latency by **81.26%** (1.443 ms ➔ 0.270 ms) using `idx_watch_logs_watch_date`.
* **Multi-Attribute Catalog Queries (Q4):** Reduced query execution latency by **80.76%** (0.535 ms ➔ 0.103 ms) using `idx_catalog_type_year`.
* **OLAP Viewing Streaks (Q1):** Replaced full table scans and temporary sorting with an optimized covering index scan (`SCAN USING COVERING INDEX`).

### 3. Isolated Departmental Data Mart (`viewer_watchtime_mart.db`)
Extracted and pre-aggregated specialized analytical reporting tables for Content Acquisition and Audience Engagement teams to prevent compute contention on the main warehouse while masking sensitive subscriber billing and financial data:
* `monthly_genre_performance` (574 rows): Monthly viewing volumes, unique streamers, total watch hours, and genre ROI metrics.
* `content_engagement_summary` (2,545 rows): Title-level engagement classification tiers (*Blockbuster Tier*, *High Engagement*, *Niche*).
* `subscriber_viewing_habits` (500 rows): Viewer intensity segmentation (*Power Streamer*, *Casual*) and churn risk indicators.
* `daily_platform_watchtime` (450 rows): Operational time-series metrics including platform Daily Active Viewers (DAV) and total streaming duration.

### 4. Multi-Engine Schema Governance (`lab_governance.py`)
* **Relational SQLite Constraints:** Codified strict DDL `CHECK` constraints (positive subscription fees, non-negative watch durations, bounded completion ratios `0.0–1.0`, valid enums, and foreign key rules) verified through automated unit tests.
* **NoSQL MongoDB Schema Enforcement:** Configured strict `$jsonSchema` validators operating on `validationAction='error'` to actively reject out-of-bounds telemetry payloads, invalid star ratings, and unauthorized device types.

## 🚀 How to Run

1. **Clone repository and install dependencies:**
   ```bash
   pip install pandas pymongo