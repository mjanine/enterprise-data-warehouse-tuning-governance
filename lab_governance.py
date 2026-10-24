"""
========================================================================================
LABORATORY ACTIVITY 4: PERFORMANCE TUNING & DEPARTMENTAL DATA MART CREATION
Script 2: Data Governance & Schema Validation Framework (lab_governance.py)
========================================================================================
"""

import sqlite3
import os
import sys
import time
import json
import pymongo
from pymongo.errors import WriteError, OperationFailure

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

CORE_DB = os.path.join(PARENT_DIR, "warehouse.db") if os.path.exists(os.path.join(PARENT_DIR, "warehouse.db")) else "warehouse.db"
GOVERNED_DB = os.path.join(BASE_DIR, "warehouse_governed.db")
LOG_FILE = os.path.join(BASE_DIR, "lab4_execution_log.txt")

# MongoDB connection settings
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "streaming_governance_dw"

class AppendLogger:
    """Appends output to console and execution audit log."""
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log_file = open(filename, "a", encoding="utf-8")

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

# ==============================================================================
# PART 1: RELATIONAL DATA GOVERNANCE & SCHEMA CONSTRAINTS (SQLITE)
# ==============================================================================

def create_governed_sqlite_tables(conn):
    """Establishes strict DDL constraints: NOT NULL, CHECK, and FOREIGN KEY constraints."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    log_section("1.1 DDL DEFINITIONS: STRICT RELATIONAL SCHEMA & CONSTRAINTS")
    
    cursor.executescript("""
        DROP TABLE IF EXISTS watch_logs_governed;
        DROP TABLE IF EXISTS user_subscriptions_governed;
        DROP TABLE IF EXISTS content_catalog_governed;

        -- 1. Governed Content Catalog Dimension Table
        CREATE TABLE content_catalog_governed (
            show_id TEXT PRIMARY KEY NOT NULL,
            type TEXT NOT NULL CHECK (type IN ('Movie', 'TV Show')),
            title TEXT NOT NULL CHECK (length(trim(title)) > 0),
            director TEXT,
            cast TEXT,
            country TEXT,
            date_added TEXT,
            release_year INTEGER NOT NULL CHECK (release_year BETWEEN 1900 AND 2030),
            rating TEXT,
            duration_raw TEXT,
            duration_num INTEGER CHECK (duration_num > 0),
            duration_unit TEXT CHECK (duration_unit IN ('Min', 'Season', 'Seasons')),
            listed_in TEXT,
            description TEXT
        );

        -- 2. Governed User Subscriptions Dimension Table
        CREATE TABLE user_subscriptions_governed (
            user_id INTEGER PRIMARY KEY NOT NULL CHECK (user_id >= 1000),
            signup_date TEXT NOT NULL CHECK (signup_date IS strftime('%Y-%m-%d', signup_date)),
            plan_type TEXT NOT NULL CHECK (plan_type IN ('Basic', 'Standard', 'Premium')),
            monthly_fee REAL NOT NULL CHECK (monthly_fee > 0.0),
            auto_renew INTEGER NOT NULL CHECK (auto_renew IN (0, 1)),
            status TEXT NOT NULL CHECK (status IN ('Active', 'Cancelled', 'Expired'))
        );

        -- 3. Governed Watch Logs Fact Table
        CREATE TABLE watch_logs_governed (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            show_id TEXT NOT NULL,
            watch_date TEXT NOT NULL CHECK (watch_date IS strftime('%Y-%m-%d', watch_date)),
            watch_duration_minutes INTEGER NOT NULL CHECK (watch_duration_minutes > 0 AND watch_duration_minutes <= 1440),
            completion_percentage REAL NOT NULL CHECK (completion_percentage >= 0.0 AND completion_percentage <= 1.0),
            FOREIGN KEY (user_id) REFERENCES user_subscriptions_governed(user_id) ON DELETE CASCADE,
            FOREIGN KEY (show_id) REFERENCES content_catalog_governed(show_id) ON DELETE RESTRICT
        );
    """)
    conn.commit()
    print("[DDL SUCCESS] Created relational tables with strict CHECK, NOT NULL, and FK constraints:")
    print("  - content_catalog_governed")
    print("  - user_subscriptions_governed")
    print("  - watch_logs_governed")

def run_sqlite_governance_tests(conn):
    """Executes systematic constraint violation tests to prove DDL rules are enforced."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    log_section("1.2 RELATIONAL CONSTRAINT VIOLATION TESTS (SQLITE TEST SUITE)")
    
    test_results = []

    try:
        cursor.execute("""
            INSERT INTO content_catalog_governed (show_id, type, title, release_year, duration_num, duration_unit)
            VALUES ('s9999', 'Movie', 'Governance Test Premiere', 2024, 120, 'Min');
        """)
        cursor.execute("""
            INSERT INTO user_subscriptions_governed (user_id, signup_date, plan_type, monthly_fee, auto_renew, status)
            VALUES (9001, '2025-05-15', 'Premium', 19.99, 1, 'Active');
        """)
        cursor.execute("""
            INSERT INTO watch_logs_governed (user_id, show_id, watch_date, watch_duration_minutes, completion_percentage)
            VALUES (9001, 's9999', '2025-05-20', 95, 0.79);
        """)
        conn.commit()
        test_results.append(("Test 0: Valid Baseline Insert", "SUCCESS", "Valid dimension and fact records accepted."))
    except Exception as e:
        test_results.append(("Test 0: Valid Baseline Insert", "FAILED", str(e)))

    # Test 1: Negative monthly subscription fee
    try:
        cursor.execute("""
            INSERT INTO user_subscriptions_governed (user_id, signup_date, plan_type, monthly_fee, auto_renew, status)
            VALUES (9002, '2025-05-16', 'Standard', -15.49, 1, 'Active');
        """)
        conn.commit()
        test_results.append(("Test 1: Negative Monthly Fee (-$15.49)", "FAILED", "Constraint failed to block negative fee!"))
    except sqlite3.IntegrityError as e:
        test_results.append(("Test 1: Negative Monthly Fee (-$15.49)", "PASSED (BLOCKED)", f"IntegrityError caught: {e}"))

    # Test 2: Negative watch duration minutes
    try:
        cursor.execute("""
            INSERT INTO watch_logs_governed (user_id, show_id, watch_date, watch_duration_minutes, completion_percentage)
            VALUES (9001, 's9999', '2025-05-21', -45, 0.50);
        """)
        conn.commit()
        test_results.append(("Test 2: Negative Watch Duration (-45 mins)", "FAILED", "Constraint failed to block negative duration!"))
    except sqlite3.IntegrityError as e:
        test_results.append(("Test 2: Negative Watch Duration (-45 mins)", "PASSED (BLOCKED)", f"IntegrityError caught: {e}"))

    # Test 3: Completion percentage > 1.0 (Out of bounds)
    try:
        cursor.execute("""
            INSERT INTO watch_logs_governed (user_id, show_id, watch_date, watch_duration_minutes, completion_percentage)
            VALUES (9001, 's9999', '2025-05-22', 120, 1.85);
        """)
        conn.commit()
        test_results.append(("Test 3: Completion > 1.0 (185%)", "FAILED", "Constraint failed to block invalid completion ratio!"))
    except sqlite3.IntegrityError as e:
        test_results.append(("Test 3: Completion > 1.0 (185%)", "PASSED (BLOCKED)", f"IntegrityError caught: {e}"))

    # Test 4: Invalid Plan Type Enum ('UltraVIP')
    try:
        cursor.execute("""
            INSERT INTO user_subscriptions_governed (user_id, signup_date, plan_type, monthly_fee, auto_renew, status)
            VALUES (9003, '2025-05-17', 'UltraVIP', 29.99, 1, 'Active');
        """)
        conn.commit()
        test_results.append(("Test 4: Invalid Plan Type ('UltraVIP')", "FAILED", "Constraint failed to block unauthorized plan enum!"))
    except sqlite3.IntegrityError as e:
        test_results.append(("Test 4: Invalid Plan Type ('UltraVIP')", "PASSED (BLOCKED)", f"IntegrityError caught: {e}"))

    # Test 5: Foreign Key Violation (Orphan Watch Log with non-existent user_id)
    try:
        cursor.execute("""
            INSERT INTO watch_logs_governed (user_id, show_id, watch_date, watch_duration_minutes, completion_percentage)
            VALUES (999999, 's9999', '2025-05-23', 60, 0.50);
        """)
        conn.commit()
        test_results.append(("Test 5: FK Violation (Orphan user_id=999999)", "FAILED", "FK Constraint failed to block orphan log!"))
    except sqlite3.IntegrityError as e:
        test_results.append(("Test 5: FK Violation (Orphan user_id=999999)", "PASSED (BLOCKED)", f"IntegrityError caught: {e}"))

    # Test 6: Invalid Release Year (1820)
    try:
        cursor.execute("""
            INSERT INTO content_catalog_governed (show_id, type, title, release_year, duration_num, duration_unit)
            VALUES ('s9998', 'Movie', 'Ancient Cinema', 1820, 90, 'Min');
        """)
        conn.commit()
        test_results.append(("Test 6: Invalid Release Year (1820)", "FAILED", "Constraint failed to block invalid release year!"))
    except sqlite3.IntegrityError as e:
        test_results.append(("Test 6: Invalid Release Year (1820)", "PASSED (BLOCKED)", f"IntegrityError caught: {e}"))

    print(f"\n{'TEST DESCRIPTION':<42} | {'OUTCOME':<18} | {'VERIFICATION DETAIL'}")
    print("-" * 95)
    for desc, outcome, detail in test_results:
        print(f"{desc:<42} | {outcome:<18} | {detail[:32]}")
    print("-" * 95 + "\n")

def migrate_and_validate_core_data(core_conn, governed_conn):
    """Migrates cleansed enterprise records from warehouse.db into governed tables."""
    log_section("1.3 POPULATING GOVERNED RELATIONAL TABLES")
    c_cur = core_conn.cursor()
    g_cur = governed_conn.cursor()
    
    catalogs = c_cur.execute("SELECT * FROM content_catalog").fetchall()
    cols_cat = [d[0] for d in c_cur.description]
    placeholders_cat = ",".join(["?"] * len(cols_cat))
    g_cur.executemany(f"INSERT OR IGNORE INTO content_catalog_governed ({','.join(cols_cat)}) VALUES ({placeholders_cat})", catalogs)
    
    users = c_cur.execute("SELECT * FROM user_subscriptions").fetchall()
    cols_user = [d[0] for d in c_cur.description]
    placeholders_user = ",".join(["?"] * len(cols_user))
    g_cur.executemany(f"INSERT OR IGNORE INTO user_subscriptions_governed ({','.join(cols_user)}) VALUES ({placeholders_user})", users)

    logs = c_cur.execute("SELECT * FROM watch_logs").fetchall()
    cols_log = [d[0] for d in c_cur.description]
    placeholders_log = ",".join(["?"] * len(cols_log))
    g_cur.executemany(f"INSERT OR IGNORE INTO watch_logs_governed ({','.join(cols_log)}) VALUES ({placeholders_log})", logs)
    
    governed_conn.commit()
    
    count_cat = g_cur.execute("SELECT COUNT(*) FROM content_catalog_governed").fetchone()[0]
    count_usr = g_cur.execute("SELECT COUNT(*) FROM user_subscriptions_governed").fetchone()[0]
    count_log = g_cur.execute("SELECT COUNT(*) FROM watch_logs_governed").fetchone()[0]
    
    print(f"[DATA INTEGRITY] Successfully validated and loaded into governed schema:")
    print(f"  - content_catalog_governed:    {count_cat:,} verified records")
    print(f"  - user_subscriptions_governed:  {count_usr:,} verified records")
    print(f"  - watch_logs_governed:         {count_log:,} verified records")

# ==============================================================================
# PART 2: DOCUMENT STORE DATA GOVERNANCE ($jsonSchema VALIDATION IN MONGODB)
# ==============================================================================

WATCH_HISTORY_JSON_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "title": "WatchHistoryDocumentValidation",
        "required": [
            "session_id", "user_id", "show_id", "title",
            "content_type", "watch_date", "watch_duration_minutes",
            "completion_percentage", "device_type", "subscriber_tier"
        ],
        "properties": {
            "session_id": {
                "bsonType": "string",
                "description": "Unique session string identifier (Required)"
            },
            "user_id": {
                "bsonType": "int",
                "minimum": 1000,
                "description": "User identifier must be an integer >= 1000"
            },
            "show_id": {
                "bsonType": "string",
                "pattern": "^s[0-9]+$",
                "description": "Show identifier matching pattern 's\\d+'"
            },
            "title": {
                "bsonType": "string",
                "minLength": 1,
                "description": "Content title cannot be blank"
            },
            "content_type": {
                "enum": ["Movie", "TV Show"],
                "description": "Content category: must be 'Movie' or 'TV Show'"
            },
            "watch_date": {
                "bsonType": "string",
                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                "description": "Must be ISO date formatted YYYY-MM-DD"
            },
            "watch_duration_minutes": {
                "bsonType": "int",
                "minimum": 1,
                "maximum": 1440,
                "description": "Viewing duration must be between 1 and 1440 minutes"
            },
            "completion_percentage": {
                "bsonType": ["double", "int"],
                "minimum": 0.0,
                "maximum": 1.0,
                "description": "Completion ratio must be within [0.0, 1.0]"
            },
            "device_type": {
                "enum": ["Smart TV", "Mobile", "Tablet", "Desktop", "Gaming Console"],
                "description": "Restricted to authorized streaming client devices"
            },
            "subscriber_tier": {
                "enum": ["Basic", "Standard", "Premium"],
                "description": "Subscription tier level"
            },
            "genres": {
                "bsonType": "array",
                "items": {"bsonType": "string"},
                "description": "List of associated genre descriptors"
            }
        }
    }
}

VIEWER_FEEDBACK_JSON_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "title": "ViewerFeedbackDocumentValidation",
        "required": [
            "feedback_id", "user_id", "show_id",
            "rating_score", "sentiment", "timestamp"
        ],
        "properties": {
            "feedback_id": {
                "bsonType": "string",
                "description": "Unique feedback document ID"
            },
            "user_id": {
                "bsonType": "int",
                "minimum": 1000,
                "description": "User ID must be >= 1000"
            },
            "show_id": {
                "bsonType": "string",
                "description": "Reference catalog ID"
            },
            "rating_score": {
                "bsonType": "int",
                "minimum": 1,
                "maximum": 5,
                "description": "Rating score bounded between 1 and 5 stars"
            },
            "sentiment": {
                "enum": ["Positive", "Neutral", "Negative"],
                "description": "Categorical sentiment classification"
            },
            "review_text": {
                "bsonType": "string",
                "maxLength": 1000,
                "description": "User feedback textual payload"
            },
            "timestamp": {
                "bsonType": "string",
                "description": "ISO timestamp of submission"
            }
        }
    }
}

def execute_mongodb_governance():
    """Initializes MongoDB collections with strict $jsonSchema validators and executes unit tests."""
    log_section("2. MONGODB SCHEMA GOVERNANCE ($jsonSchema VALIDATION)")
    
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.server_info()
        db = client[MONGO_DB_NAME]
        print(f"[MONGO CONNECT] Successfully connected to live MongoDB server at '{MONGO_URI}'.")
        print(f"[MONGO DATABASE] Target Governance DB: '{MONGO_DB_NAME}'")
    except Exception as e:
        print(f"[MONGO WARNING] Could not connect to MongoDB server: {e}")
        return

    collections_to_setup = [
        ("watch_history_documents", WATCH_HISTORY_JSON_SCHEMA),
        ("viewer_feedback_documents", VIEWER_FEEDBACK_JSON_SCHEMA)
    ]

    for col_name, schema in collections_to_setup:
        if col_name in db.list_collection_names():
            db.drop_collection(col_name)
        
        db.create_collection(
            col_name,
            validator=schema,
            validationLevel="strict",
            validationAction="error"
        )
        print(f"[MONGO DDL] Created collection '{col_name}' with STRICT $jsonSchema validator.")

    log_section("2.1 MONGODB SCHEMA VALIDATION TESTS")
    wh_col = db["watch_history_documents"]
    fb_col = db["viewer_feedback_documents"]
    
    mongo_tests = []

    valid_doc = {
        "session_id": "sess_800101",
        "user_id": 1055,
        "show_id": "s1234",
        "title": "Stranger Things",
        "content_type": "TV Show",
        "watch_date": "2025-06-14",
        "watch_duration_minutes": 52,
        "completion_percentage": 0.95,
        "device_type": "Smart TV",
        "subscriber_tier": "Premium",
        "genres": ["Sci-Fi", "Drama", "Horror"]
    }
    try:
        wh_col.insert_one(valid_doc)
        mongo_tests.append(("Test M1: Valid Compliant Document Insert", "PASSED (ACCEPTED)", "Document conformed to $jsonSchema and was saved."))
    except Exception as e:
        mongo_tests.append(("Test M1: Valid Compliant Document Insert", "FAILED", str(e)))

    # Test M2: Negative Duration (-30)
    invalid_dur_doc = valid_doc.copy()
    invalid_dur_doc["session_id"] = "sess_viol_dur"
    invalid_dur_doc["watch_duration_minutes"] = -30
    try:
        wh_col.insert_one(invalid_dur_doc)
        mongo_tests.append(("Test M2: Negative Watch Duration (-30)", "FAILED", "Validator failed to block negative duration!"))
    except (WriteError, OperationFailure) as e:
        mongo_tests.append(("Test M2: Negative Watch Duration (-30)", "PASSED (BLOCKED)", "Rejected: watch_duration_minutes minimum is 1"))

    # Test M3: Out-of-bounds Completion (1.45)
    invalid_comp_doc = valid_doc.copy()
    invalid_comp_doc["session_id"] = "sess_viol_comp"
    invalid_comp_doc["completion_percentage"] = 1.45
    try:
        wh_col.insert_one(invalid_comp_doc)
        mongo_tests.append(("Test M3: Completion Percentage > 1.0 (1.45)", "FAILED", "Validator failed to block out-of-bounds ratio!"))
    except (WriteError, OperationFailure) as e:
        mongo_tests.append(("Test M3: Completion Percentage > 1.0 (1.45)", "PASSED (BLOCKED)", "Rejected: completion_percentage maximum is 1.0"))

    # Test M4: Missing Required Field
    missing_req_doc = valid_doc.copy()
    missing_req_doc["session_id"] = "sess_viol_req"
    del missing_req_doc["subscriber_tier"]
    try:
        wh_col.insert_one(missing_req_doc)
        mongo_tests.append(("Test M4: Missing Required 'subscriber_tier'", "FAILED", "Validator failed to enforce required field!"))
    except (WriteError, OperationFailure) as e:
        mongo_tests.append(("Test M4: Missing Required 'subscriber_tier'", "PASSED (BLOCKED)", "Rejected: missing required field 'subscriber_tier'"))

    # Test M5: Unauthorized Device Enum
    invalid_enum_doc = valid_doc.copy()
    invalid_enum_doc["session_id"] = "sess_viol_enum"
    invalid_enum_doc["device_type"] = "Smart Microwave"
    try:
        wh_col.insert_one(invalid_enum_doc)
        mongo_tests.append(("Test M5: Unauthorized Device ('Smart Microwave')", "FAILED", "Validator failed to enforce device enum!"))
    except (WriteError, OperationFailure) as e:
        mongo_tests.append(("Test M5: Unauthorized Device ('Smart Microwave')", "PASSED (BLOCKED)", "Rejected: device_type not in authorized enum"))

    # Test M6: Rating score > 5
    invalid_rating_fb = {
        "feedback_id": "fb_viol_99",
        "user_id": 1055,
        "show_id": "s1234",
        "rating_score": 10,
        "sentiment": "Positive",
        "review_text": "Exceeded all expectations!",
        "timestamp": "2025-06-14T20:30:00Z"
    }
    try:
        fb_col.insert_one(invalid_rating_fb)
        mongo_tests.append(("Test M6: Out-of-bounds Rating Score (10 stars)", "FAILED", "Validator failed to enforce max rating!"))
    except (WriteError, OperationFailure) as e:
        mongo_tests.append(("Test M6: Out-of-bounds Rating Score (10 stars)", "PASSED (BLOCKED)", "Rejected: rating_score maximum is 5"))

    # Test M7: Valid Feedback Insert
    valid_fb = {
        "feedback_id": "fb_valid_01",
        "user_id": 1055,
        "show_id": "s1234",
        "rating_score": 5,
        "sentiment": "Positive",
        "review_text": "Outstanding storyline and character development.",
        "timestamp": "2025-06-14T20:30:00Z"
    }
    try:
        fb_col.insert_one(valid_fb)
        mongo_tests.append(("Test M7: Valid Viewer Feedback Insert", "PASSED (ACCEPTED)", "Compliant feedback document accepted."))
    except Exception as e:
        mongo_tests.append(("Test M7: Valid Viewer Feedback Insert", "FAILED", str(e)))

    print(f"\n{'MONGO TEST CASE':<45} | {'OUTCOME':<18} | {'VERIFICATION RESULT'}")
    print("-" * 95)
    for desc, outcome, detail in mongo_tests:
        print(f"{desc:<45} | {outcome:<18} | {detail}")
    print("-" * 95)
    
    sample_docs = [
        {
            "session_id": f"sess_prod_{i}",
            "user_id": 1000 + (i % 200),
            "show_id": f"s{i + 100}",
            "title": f"Streaming Content Title #{i}",
            "content_type": "Movie" if i % 2 == 0 else "TV Show",
            "watch_date": f"2025-07-{(i % 28) + 1:02d}",
            "watch_duration_minutes": 30 + (i * 7) % 150,
            "completion_percentage": round(0.40 + (i % 60) * 0.01, 2),
            "device_type": ["Smart TV", "Mobile", "Tablet", "Desktop", "Gaming Console"][i % 5],
            "subscriber_tier": ["Basic", "Standard", "Premium"][i % 3],
            "genres": ["Drama", "Action"] if i % 2 == 0 else ["Comedy", "Sci-Fi"]
        }
        for i in range(1, 51)
    ]
    wh_col.insert_many(sample_docs)
    wh_count = wh_col.count_documents({})
    fb_count = fb_col.count_documents({})
    print(f"\n[MONGO POPULATE] Successfully loaded compliant records into governed document store:")
    print(f"  - watch_history_documents: {wh_count:,} validated documents")
    print(f"  - viewer_feedback_documents: {fb_count:,} validated documents")
    client.close()

def main():
    logger = AppendLogger(LOG_FILE)
    sys.stdout = logger
    
    log_header("LABORATORY ACTIVITY 4: DATA GOVERNANCE & SCHEMA VALIDATION AUDIT")
    print("Group 11: Streaming Media User Subscription & Watch-Time Analytics Warehouse")
    print("Students: Sola, Mariella Janine A. | De Castro, Brent Gabriel")
    print("Instructor: Prof Rob Malitao")
    print(f"Audit Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not os.path.exists(CORE_DB):
        print(f"[ERROR] Source database '{CORE_DB}' not found! Run etl_pipeline.py first.")
        return

    core_conn = sqlite3.connect(CORE_DB)
    governed_conn = sqlite3.connect(GOVERNED_DB)
    
    create_governed_sqlite_tables(governed_conn)
    run_sqlite_governance_tests(governed_conn)
    migrate_and_validate_core_data(core_conn, governed_conn)
    
    core_conn.close()
    governed_conn.close()

    execute_mongodb_governance()
    
    print("\n[SUCCESS] lab_governance.py execution completed successfully.")
    logger.close()
    
    if os.path.abspath(BASE_DIR) != os.path.abspath(PARENT_DIR):
        import shutil
        if os.path.exists(GOVERNED_DB):
            shutil.copy2(GOVERNED_DB, os.path.join(PARENT_DIR, os.path.basename(GOVERNED_DB)))
        if os.path.exists(LOG_FILE):
            shutil.copy2(LOG_FILE, os.path.join(PARENT_DIR, os.path.basename(LOG_FILE)))

if __name__ == "__main__":
    main()
