"""
Demo Runner: Executes the full pipeline with sample data to demonstrate all features.
Run this to see the entire pipeline in action with 3 scenarios:
    1. Day 1: Normal daily load (20 transactions)
    2. Day 2: Daily load + late-arriving corrections (15 new + 5 corrections)
    3. Day 3: Bad data file (tests validation & quarantine)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.pipeline import IncrementalLoadPipeline
from src.utils import setup_logging

import logging
logger = logging.getLogger(__name__)


def print_banner(text):
    """Print a visible section banner."""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")


def run_demo():
    """Run the full demo showing all pipeline capabilities."""
    setup_logging()

    print_banner("LARGE-SCALE INCREMENTAL DATA LOAD — DEMO")
    print("This demo processes 3 batches to show:\n"
          "  1. Normal daily load\n"
          "  2. Daily load with late-arriving corrections\n"
          "  3. Bad data handling (validation + quarantine)\n")

    # ================================================================
    # SCENARIO 1: Day 1 — Normal daily transaction load
    # ================================================================
    print_banner("SCENARIO 1: Day 1 — Normal Daily Load (20 transactions)")

    pipeline1 = IncrementalLoadPipeline()
    result1 = pipeline1.run(
        file_paths=["data/daily_transactions_day1.csv"]
    )
    print(f"\nResult: {result1['status']} | Batch: {result1['batch_id']}")
    if result1["status"] == "SUCCESS":
        s = result1["stats"]
        print(f"  Read: {s['records_read']} | Valid: {s['records_valid']} | "
              f"Invalid: {s['records_invalid']}")
        print(f"  Inserted: {s['records_inserted']} | Updated: {s['records_updated']} | "
              f"Skipped: {s['records_skipped']}")

    # ================================================================
    # SCENARIO 2: Day 2 — Daily load + corrections for Day 1 records
    # ================================================================
    print_banner("SCENARIO 2: Day 2 — Daily + Corrections (15 new + 5 corrections)")

    pipeline2 = IncrementalLoadPipeline()
    result2 = pipeline2.run(
        file_paths=["data/daily_transactions_day2.csv"],
        corrections_paths=["data/corrections_day2.csv"]
    )
    print(f"\nResult: {result2['status']} | Batch: {result2['batch_id']}")
    if result2["status"] == "SUCCESS":
        s = result2["stats"]
        print(f"  Read: {s['records_read']} | Valid: {s['records_valid']} | "
              f"Invalid: {s['records_invalid']}")
        print(f"  Inserted: {s['records_inserted']} | Updated: {s['records_updated']} | "
              f"Skipped: {s['records_skipped']}")
        print("  (Corrections: TXN003, TXN007, TXN010, TXN014 amounts updated; "
              "TXN018 unchanged — hash matched, skipped)")

    # ================================================================
    # SCENARIO 3: Day 3 — Bad data file (validation & quarantine)
    # ================================================================
    print_banner("SCENARIO 3: Day 3 — Bad Data (validation + quarantine test)")

    pipeline3 = IncrementalLoadPipeline()
    result3 = pipeline3.run(
        file_paths=["data/daily_transactions_day3_bad_data.csv"]
    )
    print(f"\nResult: {result3['status']} | Batch: {result3['batch_id']}")
    if result3["status"] == "SUCCESS":
        s = result3["stats"]
        print(f"  Read: {s['records_read']} | Valid: {s['records_valid']} | "
              f"Invalid: {s['records_invalid']}")
        print(f"  Inserted: {s['records_inserted']} | Updated: {s['records_updated']} | "
              f"Skipped: {s['records_skipped']}")
        print("  (Invalid records: null IDs, negative amounts, non-numeric amounts, "
              "invalid types, threshold breaches, in-batch duplicates)")

    # ================================================================
    # FINAL SUMMARY
    # ================================================================
    print_banner("DEMO COMPLETE — Summary")
    for i, result in enumerate([result1, result2, result3], 1):
        status_icon = "OK" if result["status"] == "SUCCESS" else "FAIL"
        print(f"  Scenario {i}: [{status_icon}] Batch {result['batch_id']}")

    print("\nCheck the following for detailed output:")
    print("  - reports/ folder: Reconciliation & alert JSON reports")
    print("  - logs/ folder: Detailed pipeline execution logs")
    print("  - Oracle tables: stg_transactions, target_transactions,")
    print("    transaction_history, quarantine_records, pipeline_run_log,")
    print("    reconciliation_results, data_quality_scores")


if __name__ == "__main__":
    run_demo()
