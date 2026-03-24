"""
DEMO RESET: Drops all tables, sequences, and data for a fresh demo.
Run this BEFORE the demo to ensure a clean starting state.
Also run this AFTER the demo if you want to redo it.

Usage: python demo_reset.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import oracledb
from src.utils import load_config


def reset_database():
    config = load_config()
    db = config["database"]

    thick_dir = db.get("oracle_client_lib_dir") or os.environ.get("ORACLE_CLIENT_LIB_DIR")
    if thick_dir and os.path.isdir(thick_dir):
        oracledb.init_oracle_client(lib_dir=thick_dir)
        print(f"  Oracle thick mode: {thick_dir}")
    else:
        print("  Oracle thin mode (no Instant Client — same as pipeline default)")

    conn = oracledb.connect(user=db["user"], password=db["password"], dsn=db["dsn"])
    cursor = conn.cursor()

    print("=" * 60)
    print("  DEMO RESET — Cleaning all tables and sequences")
    print("=" * 60)

    tables = [
        "data_quality_scores",
        "rollback_snapshots",
        "pipeline_checkpoints",
        "reconciliation_results",
        "pipeline_run_log",
        "quarantine_records",
        "transaction_history",
        "target_transactions",
        "stg_transactions",
    ]

    sequences = [
        "seq_history_id",
        "seq_quarantine_id",
        "seq_run_id",
        "seq_recon_id",
        "seq_checkpoint_id",
        "seq_snapshot_id",
        "seq_quality_id",
    ]

    for table in tables:
        try:
            cursor.execute(f"DROP TABLE {table} CASCADE CONSTRAINTS")
            print(f"  Dropped table:    {table}")
        except oracledb.Error:
            print(f"  (skipped table:   {table} — does not exist)")

    for seq in sequences:
        try:
            cursor.execute(f"DROP SEQUENCE {seq}")
            print(f"  Dropped sequence: {seq}")
        except oracledb.Error:
            print(f"  (skipped seq:     {seq} — does not exist)")

    conn.commit()
    cursor.close()
    conn.close()

    # Clean report and log files (skip any that are locked by the current process)
    for folder in ["reports", "logs"]:
        if os.path.exists(folder):
            skipped = 0
            for f in os.listdir(folder):
                fp = os.path.join(folder, f)
                if os.path.isfile(fp):
                    try:
                        os.remove(fp)
                    except PermissionError:
                        skipped += 1
            msg = f"  Cleaned folder:   {folder}/"
            if skipped:
                msg += f" ({skipped} file(s) in use, skipped)"
            print(msg)

    print("\n  Database is now CLEAN. Ready for a fresh demo.")
    print("=" * 60)


if __name__ == "__main__":
    reset_database()
