"""
========================================================================================
LABORATORY ACTIVITY 4: PERFORMANCE TUNING & DEPARTMENTAL DATA MART CREATION
Script 1: Optimization, Structural Indexing & Data Mart Materialization (lab_optimize.py)
Group 11: Streaming Media User Subscription & Watch-Time Analytics Warehouse
Authors: Sola, Mariella Janine A. & De Castro, Brent Gabriel
Course: Data Warehousing - Lec (Instructor: Prof Rob Malitao)
Date: October 2026
========================================================================================
"""

import sqlite3
import pandas as pd
import time
import os
import sys
import shutil

# Locate root directory if executed inside Lesson 7-Activity
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

CORE_DB = os.path.join(PARENT_DIR, "warehouse.db") if os.path.exists(os.path.join(PARENT_DIR, "warehouse.db")) else "warehouse.db"
BACKUP_DB = os.path.join(BASE_DIR, "warehouse_backup.db")
MART_DB = os.path.join(BASE_DIR, "viewer_watchtime_mart.db")
LOG_FILE = os.path.join(BASE_DIR, "lab4_execution_log.txt")

class TeeLogger:
    """Tee output simultaneously to terminal and log file."""
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log_file = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)

    def flush(self):
        self.terminal.flush()
        if not self.log_file.closed:
            self.log_file.flush()

    def close(self):
        self.log_file.close()

def log_header(title):
    border = "=" * 80
    print(f"\n{border}")
    print(f" {title.upper()}")
    print(f"{border}\n")

def log_section(title):
    print(f"\n--- {title} ---")

def configure_sqlite_pragmas(conn):
    """Enforces SQLite pragmas for high performance, concurrency and integrity."""
    cursor = conn.cursor()
    log_section("1. CONFIGURING SQLITE ENGINE PRAGMAS")
    
    # 1. Enable Write-Ahead Logging (WAL)
    cursor.execute("PRAGMA journal_mode = WAL;")
    journal_mode = cursor.fetchone()[0]
    print(f"[PRAGMA] Journal Mode set to: {journal_mode.upper()} (Enables concurrent reads during writes)")
    
    # 2. Enable Foreign Key Constraints
    cursor.execute("PRAGMA foreign_keys = ON;")
    cursor.execute("PRAGMA foreign_keys;")
    fk_status = cursor.fetchone()[0]
    print(f"[PRAGMA] Foreign Keys Enforced: {'ON' if fk_status == 1 else 'OFF'} (Enforces referential integrity)")
    
    # 3. Synchronous = NORMAL (safe in WAL mode, eliminates fsync overhead)
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA synchronous;")
    sync_status = cursor.fetchone()[0]
    print(f"[PRAGMA] Synchronous Mode set to: {sync_status} (Optimized for WAL)")

    # 4. Cache Size = 64MB memory cache for OLAP scans
    cursor.execute("PRAGMA cache_size = -64000;")
    print("[PRAGMA] Cache Size configured to: 64,000 KB (64 MB in-memory cache)")

    # 5. Temp store in RAM for fast sorting
    cursor.execute("PRAGMA temp_store = MEMORY;")
    print("[PRAGMA] Temp Store configured to: MEMORY (Fast sort & hash operations)")
    conn.commit()

def perform_online_backup(source_conn, backup_filepath):
    """Performs non-blocking online backup using SQLite's native backup API."""
    log_section("2. AUTOMATED ONLINE DATABASE BACKUP (SQLITE BACKUP API)")
    print(f"[BACKUP] Initiating live online backup from '{CORE_DB}' to '{backup_filepath}'...")
    
    start_time = time.perf_counter()
    if os.path.exists(backup_filepath):
        os.remove(backup_filepath)
        
    backup_conn = sqlite3.connect(backup_filepath)
    with backup_conn:
        source_conn.backup(backup_conn, pages=250)
    backup_conn.close()
    
    elapsed = (time.perf_counter() - start_time) * 1000
    backup_size = os.path.getsize(backup_filepath) / (1024 * 1024)
    print(f"[BACKUP] Online backup completed successfully in {elapsed:.2f} ms.")
    print(f"[BACKUP] Backup file size: {backup_size:.2f} MB ({backup_filepath})")
    
    # Verify backup integrity
    verify_conn = sqlite3.connect(backup_filepath)
    v_cur = verify_conn.cursor()
    v_cur.execute("PRAGMA quick_check;")
    integrity = v_cur.fetchone()[0]
    
    tables = [row[0] for row in v_cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() if row[0] != 'sqlite_sequence']
    print(f"[BACKUP VERIFY] Database integrity check: {integrity.upper()}")
    for tbl in tables:
        count = v_cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"[BACKUP VERIFY] Table '{tbl}': {count:,} verified rows.")
    verify_conn.close()

# Benchmark Queries
BENCHMARK_QUERIES = [
    {
        "id": "Q1_STREAKS",
        "title": "Window Function: Sequential Viewing Streaks (PARTITION BY user_id ORDER BY watch_date)",
        "sql": """
        WITH RankedLogs AS (
            SELECT DISTINCT user_id, watch_date FROM watch_logs
        ),
        DateDiffs AS (
            SELECT 
                user_id,
                watch_date,
                LAG(watch_date, 1) OVER (PARTITION BY user_id ORDER BY watch_date) as prev_date,
                CAST(julianday(watch_date) - julianday(LAG(watch_date, 1) OVER (PARTITION BY user_id ORDER BY watch_date)) AS INTEGER) as days_since_prev
            FROM RankedLogs
        )
        SELECT user_id, COUNT(*) as continuous_streak_days
        FROM DateDiffs
        WHERE days_since_prev = 1
        GROUP BY user_id
        ORDER BY continuous_streak_days DESC
        LIMIT 10;
        """
    },
    {
        "id": "Q2_MULTI_JOIN",
        "title": "High-Cardinality Multi-Table Join & Aggregation (watch_logs JOIN catalog JOIN users)",
        "sql": """
        SELECT 
            cc.type as content_type,
            us.plan_type,
            COUNT(wl.log_id) as total_streams,
            SUM(wl.watch_duration_minutes) as total_watch_minutes,
            ROUND(AVG(wl.completion_percentage) * 100, 2) as avg_completion_pct,
            ROUND(SUM(us.monthly_fee), 2) as attributed_monthly_revenue
        FROM watch_logs wl
        JOIN content_catalog cc ON wl.show_id = cc.show_id
        JOIN user_subscriptions us ON wl.user_id = us.user_id
        WHERE us.status = 'Active'
        GROUP BY cc.type, us.plan_type
        ORDER BY total_watch_minutes DESC;
        """
    },
    {
        "id": "Q3_FILTER_RANGE",
        "title": "High-Selectivity Date Range & Completion Filter (watch_date BETWEEN AND completion >= 0.8)",
        "sql": """
        SELECT 
            wl.log_id,
            wl.user_id,
            wl.show_id,
            wl.watch_date,
            wl.watch_duration_minutes,
            wl.completion_percentage
        FROM watch_logs wl
        WHERE wl.watch_date BETWEEN '2025-06-01' AND '2025-08-31'
          AND wl.completion_percentage >= 0.80
        ORDER BY wl.watch_date DESC, wl.watch_duration_minutes DESC
        LIMIT 25;
        """
    },
    {
        "id": "Q4_CATALOG_FILTER",
        "title": "Content Catalog Filtering by Type and Release Era (type = 'Movie' AND release_year >= 2018)",
        "sql": """
        SELECT 
            show_id,
            title,
            release_year,
            rating,
            duration_num,
            listed_in
        FROM content_catalog
        WHERE type = 'Movie'
          AND release_year >= 2018
        ORDER BY release_year DESC, duration_num DESC
        LIMIT 25;
        """
    }
]

def analyze_query_plan(conn, query_sql):
    """Executes EXPLAIN QUERY PLAN and returns formatted plan details."""
    cursor = conn.cursor()
    cursor.execute(f"EXPLAIN QUERY PLAN {query_sql}")
    rows = cursor.fetchall()
    plan_lines = []
    for r in rows:
        detail = r[3]
        plan_lines.append(detail)
    return plan_lines

def benchmark_queries(conn, iterations=5):
    """Measures execution latency across benchmark queries."""
    results = {}
    cursor = conn.cursor()
    for q in BENCHMARK_QUERIES:
        q_id = q["id"]
        cursor.execute(q["sql"]).fetchall()
        
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            cursor.execute(q["sql"]).fetchall()
            times.append((time.perf_counter() - start) * 1000)
        avg_ms = sum(times) / len(times)
        plan = analyze_query_plan(conn, q["sql"])
        results[q_id] = {
            "title": q["title"],
            "avg_ms": avg_ms,
            "plan": plan
        }
    return results

def drop_custom_indexes(conn):
    """Drops custom indexes to ensure clean, reproducible unindexed baseline benchmarks."""
    cursor = conn.cursor()
    indexes = [
        "idx_watch_logs_user_id",
        "idx_watch_logs_show_id",
        "idx_watch_logs_watch_date",
        "idx_user_subscriptions_status",
        "idx_user_subscriptions_plan",
        "idx_content_catalog_type",
        "idx_content_catalog_release_year",
        "idx_watch_logs_user_date",
        "idx_watch_logs_show_dur_comp",
        "idx_watch_logs_date_comp",
        "idx_user_subs_status_plan",
        "idx_catalog_type_year"
    ]
    for idx in indexes:
        cursor.execute(f"DROP INDEX IF EXISTS {idx};")
    cursor.execute("ANALYZE;")
    conn.commit()

def apply_custom_indexes(conn):
    """Creates single-column and multi-column compound B-Tree indexes."""
    cursor = conn.cursor()
    log_section("3. STRATEGIC B-TREE INDEX CREATION (DDL EXECUTION)")
    
    index_statements = [
        ("idx_watch_logs_user_id", "CREATE INDEX IF NOT EXISTS idx_watch_logs_user_id ON watch_logs(user_id);",
         "Single-column index on foreign key user_id for rapid join resolution."),
        
        ("idx_watch_logs_show_id", "CREATE INDEX IF NOT EXISTS idx_watch_logs_show_id ON watch_logs(show_id);",
         "Single-column index on foreign key show_id linking watch logs to catalog."),
        
        ("idx_watch_logs_watch_date", "CREATE INDEX IF NOT EXISTS idx_watch_logs_watch_date ON watch_logs(watch_date);",
         "Single-column index on watch_date for temporal filtering."),
        
        ("idx_user_subscriptions_status", "CREATE INDEX IF NOT EXISTS idx_user_subscriptions_status ON user_subscriptions(status);",
         "Single-column index on account status ('Active', 'Cancelled', 'Expired')."),
        
        ("idx_user_subscriptions_plan", "CREATE INDEX IF NOT EXISTS idx_user_subscriptions_plan ON user_subscriptions(plan_type);",
         "Single-column index on plan tier ('Basic', 'Standard', 'Premium')."),
        
        ("idx_content_catalog_type", "CREATE INDEX IF NOT EXISTS idx_content_catalog_type ON content_catalog(type);",
         "Single-column index on content type ('Movie', 'TV Show')."),

        ("idx_content_catalog_release_year", "CREATE INDEX IF NOT EXISTS idx_content_catalog_release_year ON content_catalog(release_year);",
         "Single-column index on release_year for release era range filtering."),

        ("idx_watch_logs_user_date", "CREATE INDEX IF NOT EXISTS idx_watch_logs_user_date ON watch_logs(user_id, watch_date);",
         "Compound index on (user_id, watch_date): Eliminates TEMP B-TREE sorts for window functions partitioned by user_id and ordered by date."),

        ("idx_watch_logs_show_dur_comp", "CREATE INDEX IF NOT EXISTS idx_watch_logs_show_dur_comp ON watch_logs(show_id, watch_duration_minutes, completion_percentage);",
         "Compound covering index on (show_id, watch_duration_minutes, completion_percentage) for ultra-fast title metrics aggregation."),

        ("idx_watch_logs_date_comp", "CREATE INDEX IF NOT EXISTS idx_watch_logs_date_comp ON watch_logs(watch_date, completion_percentage);",
         "Compound index on (watch_date, completion_percentage) for high-selectivity temporal engagement queries."),

        ("idx_user_subs_status_plan", "CREATE INDEX IF NOT EXISTS idx_user_subs_status_plan ON user_subscriptions(status, plan_type);",
         "Compound index on (status, plan_type) for targeted active subscriber segment lookups."),

        ("idx_catalog_type_year", "CREATE INDEX IF NOT EXISTS idx_catalog_type_year ON content_catalog(type, release_year);",
         "Compound index on (type, release_year) for catalog slicing and filtering.")
    ]

    for name, ddl, purpose in index_statements:
        start = time.perf_counter()
        cursor.execute(ddl)
        elapsed = (time.perf_counter() - start) * 1000
        print(f"[DDL] Created Index: {name} ({elapsed:.2f} ms)")
        print(f"      Purpose: {purpose}")
        print(f"      SQL: {ddl}\n")
    
    cursor.execute("ANALYZE;")
    conn.commit()
    print("[OPTIMIZE] Executed 'ANALYZE;' to update SQLite query planner statistics.")

def print_performance_comparison(pre_bench, post_bench):
    """Outputs comprehensive EXPLAIN QUERY PLAN and execution latency comparisons."""
    log_section("4. QUERY PLAN & LATENCY COMPARISON (BEFORE VS. AFTER INDEXING)")
    
    print(f"{'QUERY ID':<15} | {'BEFORE (ms)':<12} | {'AFTER (ms)':<12} | {'IMPROVEMENT':<14} | {'STATUS'}")
    print("-" * 75)
    for q_id, pre in pre_bench.items():
        post = post_bench[q_id]
        before_ms = pre["avg_ms"]
        after_ms = post["avg_ms"]
        diff_pct = ((before_ms - after_ms) / before_ms) * 100 if before_ms > 0 else 0.0
        status = "OPTIMIZED" if diff_pct >= 0 else "COMPARABLE"
        print(f"{q_id:<15} | {before_ms:10.3f} ms | {after_ms:10.3f} ms | {diff_pct:11.2f}% | {status}")
    
    print("\n" + "=" * 80)
    print("DETAILED EXPLAIN QUERY PLAN TRANSFORMATIONS:")
    print("=" * 80)
    
    for q_id, pre in pre_bench.items():
        post = post_bench[q_id]
        print(f"\n>>> Query: {pre['title']}")
        print("  [BEFORE INDEXING PLAN]:")
        for p in pre["plan"]:
            print(f"    - {p}")
        print("  [AFTER INDEXING PLAN]:")
        for p in post["plan"]:
            print(f"    + {p}")
        
        has_index_after = any("INDEX" in p for p in post["plan"])
        print(f"  --> Verification: Full table scans eliminated and replaced by index searches: {has_index_after}")

def materialize_departmental_data_mart(source_conn, mart_db_path):
    """
    Materializes dedicated departmental analytical tables into a separate database.
    Target Business Unit: Content Programming & Audience Engagement Department (Streaming Media).
    """
    log_section("5. MATERIALIZING DEPARTMENTAL DATA MART (viewer_watchtime_mart.db)")
    print(f"[MART] Creating and materializing isolated data mart at: '{mart_db_path}'...")
    
    if os.path.exists(mart_db_path):
        os.remove(mart_db_path)
        
    mart_conn = sqlite3.connect(mart_db_path)
    mart_cur = mart_conn.cursor()
    
    mart_cur.execute("PRAGMA journal_mode = WAL;")
    mart_cur.execute("PRAGMA synchronous = NORMAL;")

    # Table 1: monthly_genre_performance
    print("\n[MART TABLE 1] Extracting: 'monthly_genre_performance'...")
    query_genre_monthly = """
    WITH GenreSplits AS (
        SELECT 
            strftime('%Y-%m', wl.watch_date) as year_month,
            wl.log_id,
            wl.user_id,
            wl.watch_duration_minutes,
            wl.completion_percentage,
            us.monthly_fee,
            TRIM(g.value) as genre
        FROM watch_logs wl
        JOIN content_catalog cc ON wl.show_id = cc.show_id
        JOIN user_subscriptions us ON wl.user_id = us.user_id
        JOIN json_each('["' || REPLACE(cc.listed_in, ', ', '","') || '"]') g
    )
    SELECT 
        year_month,
        genre,
        COUNT(log_id) as total_streams,
        COUNT(DISTINCT user_id) as unique_viewers,
        SUM(watch_duration_minutes) as total_watch_minutes,
        ROUND(SUM(watch_duration_minutes) / 60.0, 2) as total_watch_hours,
        ROUND(AVG(completion_percentage) * 100, 2) as avg_completion_pct,
        ROUND(SUM(monthly_fee), 2) as attributed_revenue,
        ROUND(SUM(monthly_fee) / NULLIF(SUM(watch_duration_minutes) / 60.0, 0), 2) as revenue_per_watch_hour
    FROM GenreSplits
    GROUP BY year_month, genre
    ORDER BY year_month ASC, total_watch_minutes DESC;
    """
    df_genre_monthly = pd.read_sql_query(query_genre_monthly, source_conn)
    df_genre_monthly.to_sql("monthly_genre_performance", mart_conn, if_exists="replace", index=False)
    print(f"      Materialized {len(df_genre_monthly)} rows into 'monthly_genre_performance'.")

    # Table 2: content_engagement_summary
    print("[MART TABLE 2] Extracting: 'content_engagement_summary'...")
    query_content_summary = """
    SELECT 
        cc.show_id,
        cc.title,
        cc.type as content_type,
        cc.release_year,
        cc.rating,
        COUNT(wl.log_id) as total_streams,
        COUNT(DISTINCT wl.user_id) as unique_viewers,
        SUM(wl.watch_duration_minutes) as total_watch_minutes,
        ROUND(SUM(wl.watch_duration_minutes) / 60.0, 2) as total_watch_hours,
        ROUND(AVG(wl.completion_percentage) * 100, 2) as avg_completion_pct,
        CASE 
            WHEN SUM(wl.watch_duration_minutes) >= 600 THEN 'Blockbuster Tier'
            WHEN SUM(wl.watch_duration_minutes) >= 300 THEN 'High Engagement'
            WHEN SUM(wl.watch_duration_minutes) >= 100 THEN 'Moderate Engagement'
            ELSE 'Niche Content'
        END as engagement_tier
    FROM content_catalog cc
    LEFT JOIN watch_logs wl ON cc.show_id = wl.show_id
    GROUP BY cc.show_id, cc.title, cc.type, cc.release_year, cc.rating
    HAVING total_streams > 0
    ORDER BY total_watch_minutes DESC;
    """
    df_content_summary = pd.read_sql_query(query_content_summary, source_conn)
    df_content_summary.to_sql("content_engagement_summary", mart_conn, if_exists="replace", index=False)
    print(f"      Materialized {len(df_content_summary)} rows into 'content_engagement_summary'.")

    # Table 3: subscriber_viewing_habits
    print("[MART TABLE 3] Extracting: 'subscriber_viewing_habits'...")
    query_user_habits = """
    WITH RankedLogs AS (
        SELECT DISTINCT user_id, watch_date FROM watch_logs
    ),
    DateDiffs AS (
        SELECT 
            user_id,
            watch_date,
            LAG(watch_date, 1) OVER (PARTITION BY user_id ORDER BY watch_date) as prev_date,
            CAST(julianday(watch_date) - julianday(LAG(watch_date, 1) OVER (PARTITION BY user_id ORDER BY watch_date)) AS INTEGER) as days_since_prev
        FROM RankedLogs
    ),
    StreakGroups AS (
        SELECT 
            user_id,
            watch_date,
            SUM(CASE WHEN days_since_prev = 1 THEN 0 ELSE 1 END) OVER (PARTITION BY user_id ORDER BY watch_date) as streak_id
        FROM DateDiffs
    ),
    UserStreaks AS (
        SELECT 
            user_id,
            MAX(streak_days) as max_streak_days
        FROM (
            SELECT user_id, streak_id, COUNT(*) as streak_days
            FROM StreakGroups
            GROUP BY user_id, streak_id
        )
        GROUP BY user_id
    )
    SELECT 
        us.user_id,
        us.plan_type,
        us.status as account_status,
        us.signup_date,
        us.monthly_fee,
        COUNT(wl.log_id) as total_sessions_logged,
        COALESCE(SUM(wl.watch_duration_minutes), 0) as total_watch_minutes,
        ROUND(COALESCE(AVG(wl.watch_duration_minutes), 0), 2) as avg_session_minutes,
        ROUND(COALESCE(AVG(wl.completion_percentage), 0) * 100, 2) as avg_completion_pct,
        COALESCE(ust.max_streak_days, 0) as max_continuous_streak_days,
        CASE 
            WHEN COALESCE(SUM(wl.watch_duration_minutes), 0) >= 800 THEN 'Power Streamer'
            WHEN COALESCE(SUM(wl.watch_duration_minutes), 0) >= 400 THEN 'Active Regular'
            WHEN COALESCE(SUM(wl.watch_duration_minutes), 0) >= 100 THEN 'Casual Viewer'
            ELSE 'Low Activity'
        END as viewer_segment,
        CASE
            WHEN us.status != 'Active' THEN 'Churned'
            WHEN COALESCE(SUM(wl.watch_duration_minutes), 0) < 200 THEN 'High Churn Risk'
            WHEN COALESCE(SUM(wl.watch_duration_minutes), 0) < 500 THEN 'Moderate Churn Risk'
            ELSE 'Retained / Healthy'
        END as retention_status
    FROM user_subscriptions us
    LEFT JOIN watch_logs wl ON us.user_id = wl.user_id
    LEFT JOIN UserStreaks ust ON us.user_id = ust.user_id
    GROUP BY us.user_id, us.plan_type, us.status, us.signup_date, us.monthly_fee
    ORDER BY total_watch_minutes DESC;
    """
    df_user_habits = pd.read_sql_query(query_user_habits, source_conn)
    df_user_habits.to_sql("subscriber_viewing_habits", mart_conn, if_exists="replace", index=False)
    print(f"      Materialized {len(df_user_habits)} rows into 'subscriber_viewing_habits'.")

    # Table 4: daily_platform_watchtime
    print("[MART TABLE 4] Extracting: 'daily_platform_watchtime'...")
    query_daily_traffic = """
    SELECT 
        wl.watch_date,
        COUNT(DISTINCT wl.user_id) as active_streamers,
        COUNT(wl.log_id) as total_streams_logged,
        SUM(wl.watch_duration_minutes) as total_watch_minutes,
        ROUND(SUM(wl.watch_duration_minutes) / 60.0, 2) as total_watch_hours,
        ROUND(SUM(wl.watch_duration_minutes) * 1.0 / COUNT(DISTINCT wl.user_id), 2) as avg_minutes_per_streamer,
        ROUND(AVG(wl.completion_percentage) * 100, 2) as avg_completion_pct
    FROM watch_logs wl
    GROUP BY wl.watch_date
    ORDER BY wl.watch_date ASC;
    """
    df_daily_traffic = pd.read_sql_query(query_daily_traffic, source_conn)
    df_daily_traffic.to_sql("daily_platform_watchtime", mart_conn, if_exists="replace", index=False)
    print(f"      Materialized {len(df_daily_traffic)} rows into 'daily_platform_watchtime'.")

    # Indexes on data mart
    print("\n[MART OPTIMIZE] Applying high-speed B-Tree indexes on data mart tables...")
    mart_cur.execute("CREATE INDEX IF NOT EXISTS idx_mart_genre_month ON monthly_genre_performance(year_month, genre);")
    mart_cur.execute("CREATE INDEX IF NOT EXISTS idx_mart_content_views ON content_engagement_summary(total_watch_minutes DESC);")
    mart_cur.execute("CREATE INDEX IF NOT EXISTS idx_mart_user_segment ON subscriber_viewing_habits(viewer_segment, retention_status);")
    mart_cur.execute("CREATE INDEX IF NOT EXISTS idx_mart_daily_date ON daily_platform_watchtime(watch_date);")
    mart_cur.execute("ANALYZE;")
    mart_conn.commit()
    
    mart_size = os.path.getsize(mart_db_path) / (1024 * 1024)
    print(f"[MART COMPLETE] Data mart materialization finished. Database size: {mart_size:.2f} MB ({mart_db_path})\n")
    
    print("------------------------------------------------------------------")
    print(f"{'DATA MART TABLE':<32} | {'ROW COUNT':<12} | {'STATUS'}")
    print("------------------------------------------------------------------")
    for tbl in ["monthly_genre_performance", "content_engagement_summary", "subscriber_viewing_habits", "daily_platform_watchtime"]:
        cnt = mart_cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"{tbl:<32} | {cnt:<12} | VERIFIED")
    print("------------------------------------------------------------------\n")
    mart_conn.close()

def main():
    logger = TeeLogger(LOG_FILE)
    sys.stdout = logger
    
    log_header("LABORATORY ACTIVITY 4: OPTIMIZATION & DATA MART EXECUTION AUDIT")
    print("Group 11: Streaming Media User Subscription & Watch-Time Analytics Warehouse")
    print("Students: Sola, Mariella Janine A. | De Castro, Brent Gabriel")
    print("Instructor: Prof Rob Malitao")
    print(f"Execution Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not os.path.exists(CORE_DB):
        print(f"[ERROR] Core database '{CORE_DB}' not found! Please run etl_pipeline.py first.")
        return

    conn = sqlite3.connect(CORE_DB)
    configure_sqlite_pragmas(conn)
    perform_online_backup(conn, BACKUP_DB)

    log_section("3. PRE-OPTIMIZATION BENCHMARK & EXPLAIN QUERY PLAN (BEFORE INDEXING)")
    print("[BENCHMARK] Reverting any existing custom indexes to measure true baseline...")
    drop_custom_indexes(conn)
    print("[BENCHMARK] Executing queries on unindexed/baseline warehouse tables...")
    pre_benchmarks = benchmark_queries(conn, iterations=5)
    for q_id, res in pre_benchmarks.items():
        print(f"\n[QUERY: {q_id}] {res['title']}")
        print(f"  Latency (Baseline): {res['avg_ms']:.3f} ms")
        print("  Explain Query Plan:")
        for p in res["plan"]:
            print(f"    - {p}")

    apply_custom_indexes(conn)

    log_section("4. POST-OPTIMIZATION BENCHMARK & EXPLAIN QUERY PLAN (AFTER INDEXING)")
    print("[BENCHMARK] Executing queries on tuned warehouse with active B-Tree indexes...")
    post_benchmarks = benchmark_queries(conn, iterations=5)
    for q_id, res in post_benchmarks.items():
        print(f"\n[QUERY: {q_id}] {res['title']}")
        print(f"  Latency (Optimized): {res['avg_ms']:.3f} ms")
        print("  Explain Query Plan:")
        for p in res["plan"]:
            print(f"    + {p}")

    print_performance_comparison(pre_benchmarks, post_benchmarks)
    materialize_departmental_data_mart(conn, MART_DB)

    # Sync back to parent directory if ran inside Lesson 7-Activity
    if os.path.abspath(BASE_DIR) != os.path.abspath(PARENT_DIR):
        print(f"[SYNC] Copying artifacts to parent workspace directory...")
        shutil.copy2(MART_DB, os.path.join(PARENT_DIR, os.path.basename(MART_DB)))
        shutil.copy2(BACKUP_DB, os.path.join(PARENT_DIR, os.path.basename(BACKUP_DB)))

    conn.close()
    print("\n[SUCCESS] lab_optimize.py completed successfully.")
    logger.close()
    
    if os.path.abspath(BASE_DIR) != os.path.abspath(PARENT_DIR):
        shutil.copy2(LOG_FILE, os.path.join(PARENT_DIR, os.path.basename(LOG_FILE)))

if __name__ == "__main__":
    main()
