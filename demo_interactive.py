"""
INTERACTIVE DEMO: Step-by-step walkthrough for the assessment panel.

This script pauses between each step so you can explain what's happening.
It also queries Oracle and prints results directly in the console — no need
to switch to SQL Developer (though you can show both side-by-side).

Usage: python demo_interactive.py

Flow:
  RESET  → Clean slate
  STEP 1 → Day 1: Normal load (20 transactions)
  STEP 2 → Show data in Oracle
  STEP 3 → Day 2: New transactions + late-arriving corrections
  STEP 4 → Show corrections, history, hash comparison
  STEP 5 → Day 3: Bad data → quarantine demo
  STEP 6 → Show quarantine + data quality
  STEP 7 → Show reconciliation results
  STEP 8 → ROLLBACK demo (undo Day 3 batch)
  STEP 9 → Final summary — pipeline health dashboard
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import oracledb
from src.utils import load_config, setup_logging, generate_batch_id
from src.pipeline import IncrementalLoadPipeline
from src.database import DatabaseManager, ORACLE_CLIENT_DIR

config = load_config()


def pause(message=""):
    """Pause and wait for the presenter to press Enter."""
    if message:
        print(f"\n  >> {message}")
    input("\n  [ Press ENTER to continue... ]\n")


def banner(text, char="="):
    width = 70
    print(f"\n{char * width}")
    print(f"  {text}")
    print(f"{char * width}\n")


def query_and_print(conn, sql, title, headers=None, max_rows=30):
    """Run a SQL query and print results as a formatted table."""
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchmany(max_rows)
    cols = [desc[0] for desc in cursor.description] if not headers else headers
    cursor.close()

    if not rows:
        print(f"  {title}: (no rows)")
        return

    print(f"\n  {title}")
    print(f"  {'-' * len(title)}")

    col_widths = []
    for i, col in enumerate(cols):
        max_w = len(str(col))
        for row in rows:
            val = str(row[i]) if row[i] is not None else ""
            if len(val) > 40:
                val = val[:37] + "..."
            max_w = max(max_w, len(val))
        col_widths.append(min(max_w, 40))

    header_line = "  | " + " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(cols)) + " |"
    sep_line = "  +-" + "-+-".join("-" * w for w in col_widths) + "-+"

    print(sep_line)
    print(header_line)
    print(sep_line)
    for row in rows:
        vals = []
        for i, v in enumerate(row):
            s = str(v) if v is not None else ""
            if len(s) > 40:
                s = s[:37] + "..."
            vals.append(s.ljust(col_widths[i]))
        print("  | " + " | ".join(vals) + " |")
    print(sep_line)
    print(f"  ({len(rows)} rows shown)")


def get_connection():
    db = config["database"]
    if not DatabaseManager._thick_mode_initialized:
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_DIR)
        DatabaseManager._thick_mode_initialized = True
    return oracledb.connect(user=db["user"], password=db["password"], dsn=db["dsn"])


def run_demo():
    setup_logging()

    banner("LARGE-SCALE INCREMENTAL DATA LOAD — INTERACTIVE DEMO")
    print("  This demo walks through the full pipeline step-by-step.")
    print("  Press ENTER at each pause to advance to the next step.")
    print("  You can show Oracle SQL Developer side-by-side for live queries.")

    # ==================================================================
    # RESET
    # ==================================================================
    pause("First, let's reset the database to a clean state...")

    banner("STEP 0: RESET DATABASE", "-")
    print("  Running demo_reset.py to drop all tables...")
    from demo_reset import reset_database
    reset_database()

    pause("Database is clean. Let's start the pipeline.")

    # ==================================================================
    # STEP 1: Day 1 — Normal Daily Load
    # ==================================================================
    banner("STEP 1: Day 1 — Load 20 Normal Transactions")
    print("  Source file: data/daily_transactions_day1.csv")
    print("  Expected: 20 valid records, 0 invalid")
    print("  This demonstrates: Ingestion, Validation, Hashing, MERGE (INSERT)")
    pause("Running pipeline for Day 1...")

    pipeline1 = IncrementalLoadPipeline()
    result1 = pipeline1.run(file_paths=["data/daily_transactions_day1.csv"])
    batch1 = result1["batch_id"]

    s = result1["stats"]
    print(f"\n  Result: {result1['status']}")
    print(f"  Batch ID: {batch1}")
    print(f"  Records Read: {s['records_read']}  |  Valid: {s['records_valid']}  |  Invalid: {s['records_invalid']}")
    print(f"  Inserted: {s['records_inserted']}  |  Updated: {s['records_updated']}  |  Skipped: {s['records_skipped']}")

    # ==================================================================
    # STEP 2: Show data in Oracle
    # ==================================================================
    pause("Let's look at the data in Oracle...")

    banner("STEP 2: Verify Data in Oracle", "-")
    conn = get_connection()

    query_and_print(conn,
        "SELECT transaction_id, transaction_date, account_id, transaction_type, amount, version FROM target_transactions ORDER BY transaction_id",
        "TARGET_TRANSACTIONS (20 rows loaded)")

    query_and_print(conn,
        f"SELECT check_type, source_value, target_value, difference, status FROM reconciliation_results WHERE batch_id = '{batch1}' ORDER BY recon_id",
        "RECONCILIATION RESULTS — All 4 checks PASS")

    pause("All 20 rows loaded. All reconciliation checks passed. Now let's test corrections...")

    # ==================================================================
    # STEP 3: Day 2 — New Transactions + Late-Arriving Corrections
    # ==================================================================
    banner("STEP 3: Day 2 — 15 New + 5 Late-Arriving Corrections")
    print("  Source files:")
    print("    - data/daily_transactions_day2.csv     (15 new transactions)")
    print("    - data/corrections_day2.csv            (5 corrections for Day 1 records)")
    print("")
    print("  Corrections being applied:")
    print("    TXN003: Amount 750.50 -> 850.50  (Swiggy order corrected)")
    print("    TXN007: Amount 8500.00 -> 8750.00 (Insurance premium revised)")
    print("    TXN010: Amount 4500.00 -> 4200.00 (Partial refund applied)")
    print("    TXN014: Amount 2000.00 -> 2500.00 (Additional cashback)")
    print("    TXN018: Amount 9500.00 -> 9500.00 (NO CHANGE — hash will match, skip)")
    print("")
    print("  This demonstrates: Late-arriving data, Hash comparison, MERGE (UPDATE + SKIP)")

    pause("Running pipeline for Day 2 with corrections...")

    pipeline2 = IncrementalLoadPipeline()
    result2 = pipeline2.run(
        file_paths=["data/daily_transactions_day2.csv"],
        corrections_paths=["data/corrections_day2.csv"]
    )
    batch2 = result2["batch_id"]

    s = result2["stats"]
    print(f"\n  Result: {result2['status']}")
    print(f"  Batch ID: {batch2}")
    print(f"  Records Read: {s['records_read']}  |  Valid: {s['records_valid']}  |  Invalid: {s['records_invalid']}")
    print(f"  Inserted: {s['records_inserted']}  |  Updated: {s['records_updated']}  |  Skipped: {s['records_skipped']}")

    # ==================================================================
    # STEP 4: Show corrections, history, and hash comparison
    # ==================================================================
    pause("Let's see the corrections in Oracle...")

    banner("STEP 4: Verify Corrections & Audit Trail", "-")

    query_and_print(conn,
        "SELECT transaction_id, amount, version, updated_batch_id FROM target_transactions WHERE version > 1 ORDER BY transaction_id",
        "CORRECTED RECORDS (version > 1 means corrected)")

    query_and_print(conn,
        f"SELECT transaction_id, change_type, amount, version, batch_id FROM transaction_history WHERE change_type = 'CORRECTION' ORDER BY transaction_id",
        "TRANSACTION HISTORY — Correction audit trail")

    query_and_print(conn,
        f"""SELECT r.transaction_id,
               r.amount AS old_amount,
               t.amount AS new_amount,
               t.amount - r.amount AS difference
        FROM rollback_snapshots r
        JOIN target_transactions t ON r.transaction_id = t.transaction_id
        WHERE r.batch_id = '{batch2}' AND r.row_hash != t.row_hash
        ORDER BY r.transaction_id""",
        "BEFORE vs AFTER — Amount changes via rollback snapshot")

    query_and_print(conn,
        "SELECT COUNT(*) AS total_rows, COUNT(DISTINCT transaction_id) AS unique_keys FROM target_transactions WHERE is_active = 1",
        "ZERO DUPLICATES — Total rows = Unique keys (idempotent!)")

    pause("Corrections applied with full audit trail. Now let's test bad data...")

    # ==================================================================
    # STEP 5: Day 3 — Bad Data → Quarantine
    # ==================================================================
    banner("STEP 5: Day 3 — Bad Data File (Validation + Quarantine)")
    print("  Source file: data/daily_transactions_day3_bad_data.csv")
    print("")
    print("  This file intentionally contains 7 types of bad data:")
    print("    1. Missing Transaction ID (null primary key)")
    print("    2. Missing Account ID")
    print("    3. Negative amount (-500.00)")
    print("    4. Non-numeric amount ('abc')")
    print("    5. Invalid transaction type ('INVALID_TYPE')")
    print("    6. Amount exceeding safety threshold (999,999,999.99)")
    print("    7. Duplicate transaction ID within same batch")
    print("")
    print("  Expected: 3 valid records loaded, 7 quarantined with error codes")

    pause("Running pipeline for Day 3 (bad data)...")

    pipeline3 = IncrementalLoadPipeline()
    result3 = pipeline3.run(file_paths=["data/daily_transactions_day3_bad_data.csv"])
    batch3 = result3["batch_id"]

    s = result3["stats"]
    print(f"\n  Result: {result3['status']}")
    print(f"  Batch ID: {batch3}")
    print(f"  Records Read: {s['records_read']}  |  Valid: {s['records_valid']}  |  Invalid: {s['records_invalid']}")
    print(f"  Inserted: {s['records_inserted']}  |  Updated: {s['records_updated']}  |  Skipped: {s['records_skipped']}")

    # ==================================================================
    # STEP 6: Show quarantine + data quality
    # ==================================================================
    pause("Let's see the quarantined records and data quality scores...")

    banner("STEP 6: Quarantine & Data Quality", "-")

    query_and_print(conn,
        f"SELECT transaction_id, error_code, error_message FROM quarantine_records WHERE batch_id = '{batch3}' ORDER BY quarantine_id",
        "QUARANTINE TABLE — 7 bad records with error details")

    query_and_print(conn,
        "SELECT batch_id, total_records, valid_records, invalid_records, duplicate_records, quality_score_pct FROM data_quality_scores ORDER BY check_timestamp",
        "DATA QUALITY SCORES — Per-batch quality trending")

    pause("Bad data isolated in quarantine. Valid data loaded cleanly. Now reconciliation...")

    # ==================================================================
    # STEP 7: Reconciliation results across all batches
    # ==================================================================
    banner("STEP 7: Reconciliation Results — All Batches", "-")

    query_and_print(conn,
        "SELECT batch_id, check_type, source_value, target_value, difference, status FROM reconciliation_results ORDER BY recon_id",
        "ALL RECONCILIATION CHECKS — 12 checks, all PASS")

    query_and_print(conn,
        """SELECT account_id,
               COUNT(*) AS txn_count,
               SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) AS credits,
               SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END) AS debits
        FROM target_transactions WHERE is_active = 1
        GROUP BY account_id ORDER BY account_id""",
        "ACCOUNT-LEVEL AGGREGATION (analytics ready)")

    pause("All reconciliation checks passed. Now let's demo ROLLBACK...")

    # ==================================================================
    # STEP 8: ROLLBACK Demo
    # ==================================================================
    banner("STEP 8: ROLLBACK — Undo Day 3 Batch")
    print(f"  We will rollback batch: {batch3}")
    print("  This will:")
    print("    - Delete the 3 rows inserted by Day 3")
    print("    - Clean up history for that batch")
    print("    - Mark the pipeline run as ROLLED_BACK")
    print("")
    print("  This demonstrates: Production recovery capability")

    query_and_print(conn,
        "SELECT COUNT(*) AS row_count FROM target_transactions WHERE is_active = 1",
        "BEFORE ROLLBACK — Row count in target")

    pause("Rolling back Day 3 batch...")

    pipeline_rb = IncrementalLoadPipeline()
    pipeline_rb.rollback(batch3)
    print(f"\n  Rollback complete for batch: {batch3}")

    conn.close()
    conn = get_connection()

    query_and_print(conn,
        "SELECT COUNT(*) AS row_count FROM target_transactions WHERE is_active = 1",
        "AFTER ROLLBACK — Row count in target (3 rows removed)")

    query_and_print(conn,
        f"SELECT batch_id, pipeline_stage, status FROM pipeline_run_log WHERE batch_id = '{batch3}' ORDER BY run_id",
        "PIPELINE LOG — Day 3 batch marked as ROLLED_BACK")

    pause("Rollback successful. Now the final summary...")

    # ==================================================================
    # STEP 9: Final Summary — Pipeline Health Dashboard
    # ==================================================================
    banner("STEP 9: Pipeline Health Dashboard", "-")

    query_and_print(conn,
        """SELECT batch_id, pipeline_stage, status,
               records_read, records_valid, records_invalid,
               records_inserted, records_updated, records_skipped
        FROM pipeline_run_log ORDER BY run_id""",
        "PIPELINE RUN LOG — Full execution history")

    query_and_print(conn,
        "SELECT COUNT(*) AS total_transactions FROM target_transactions WHERE is_active = 1",
        "FINAL TARGET TABLE — Clean transaction count")

    query_and_print(conn,
        "SELECT COUNT(*) AS total_history_records FROM transaction_history",
        "AUDIT TRAIL — Every change tracked")

    query_and_print(conn,
        "SELECT COUNT(*) AS quarantined_records FROM quarantine_records",
        "QUARANTINE — Bad data isolated, not lost")

    conn.close()

    # ==================================================================
    # END
    # ==================================================================
    banner("DEMO COMPLETE")
    print("  What we demonstrated:")
    print("  ---------------------")
    print("  1. IDEMPOTENT incremental load — hash-based change detection")
    print("  2. LATE-ARRIVING corrections — detected, applied, audited")
    print("  3. DATA VALIDATION — 7 types of bad data caught and quarantined")
    print("  4. RECONCILIATION — 4-point checks (count, amount, keys, orphans)")
    print("  5. ROLLBACK — full batch undo via pre-merge snapshots")
    print("  6. AUDIT TRAIL — every insert, update, correction tracked")
    print("  7. CHECKPOINT/RESUME — pipeline can restart from any failed stage")
    print("  8. ALERTING — quality warnings, failure notifications")
    print("  9. DATA QUALITY SCORING — per-batch quality metrics")
    print("")
    print("  Output artifacts:")
    print("  - reports/*.json — Reconciliation & alert reports")
    print("  - logs/*.log — Structured pipeline execution logs")
    print("  - architecture_diagram.png — System architecture")
    print("  - sql/*.sql — DDL, MERGE procedures, analytics queries")
    print("")
    print("  Technology: Python 3 + Oracle 11g XE + oracledb (thick mode)")
    print("=" * 70)


if __name__ == "__main__":
    run_demo()
