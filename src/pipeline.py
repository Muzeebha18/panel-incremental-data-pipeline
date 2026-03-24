"""
Pipeline Orchestrator: Main entry point that coordinates all pipeline stages.

Stages:
    1. INGEST    — Read CSV files, validate records
    2. VALIDATE  — Data quality checks, quarantine bad records
    3. TRANSFORM — Deduplication, standardization, merge corrections
    4. LOAD      — MERGE into target table with hash comparison
    5. RECONCILE — 3-point verification, generate reports, send alerts

Features:
    - Idempotent: Safe to re-run without creating duplicates
    - Checkpoint/Resume: Picks up from last completed stage on failure
    - Rollback: Can revert any batch to pre-merge state
    - Auditable: Full history trail for every change
"""

import logging
import os
import sys
import time
from datetime import datetime

from src.utils import setup_logging, load_config, generate_batch_id, format_duration
from src.database import DatabaseManager
from src.ingestion import process_file
from src.transformation import deduplicate_records, standardize_records, merge_daily_and_corrections
from src.reconciliation import ReconciliationEngine
from src.rollback import RollbackManager
from src.checkpointing import CheckpointManager, PIPELINE_STAGES
from src.alerting import AlertManager

logger = logging.getLogger(__name__)


class IncrementalLoadPipeline:
    """Main pipeline orchestrator for incremental data loading."""

    def __init__(self, config_path="config.json"):
        self.config = load_config(config_path)
        self.db = DatabaseManager(self.config)
        self.checkpoint_mgr = None
        self.rollback_mgr = None
        self.recon_engine = None
        self.alert_mgr = AlertManager(self.config)
        self.batch_id = None

        self.stats = {
            "records_read": 0,
            "records_valid": 0,
            "records_invalid": 0,
            "records_inserted": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "duplicate_records": 0,
            "files_processed": 0,
        }

    def run(self, file_paths, batch_id=None, corrections_paths=None):
        """
        Execute the full pipeline for given source files.

        Args:
            file_paths: List of CSV file paths for daily transactions
            batch_id: Optional batch ID (for resuming a failed run)
            corrections_paths: Optional list of correction file paths
        """
        pipeline_start = datetime.now()

        log_file = setup_logging(
            log_dir=self.config.get("pipeline", {}).get("logs_directory", "logs")
        )

        self.batch_id = batch_id or generate_batch_id()
        logger.info("=" * 70)
        logger.info("PIPELINE START — Batch: %s", self.batch_id)
        logger.info("Source files: %s", file_paths)
        if corrections_paths:
            logger.info("Correction files: %s", corrections_paths)
        logger.info("=" * 70)

        try:
            # Connect to Oracle
            self.db.connect()
            self.db.execute_ddl()

            self.checkpoint_mgr = CheckpointManager(self.db)
            self.rollback_mgr = RollbackManager(self.db)
            self.recon_engine = ReconciliationEngine(self.db, self.config)

            # Determine resume point
            resume_index = self.checkpoint_mgr.get_resume_stage_index(self.batch_id)
            if resume_index > 0:
                logger.info("Resuming from stage index %d: %s", resume_index, PIPELINE_STAGES[resume_index])

            all_valid = []
            all_invalid = []

            # ================================================================
            # STAGE 1: INGEST — Read and validate CSV files
            # ================================================================
            if resume_index <= 0:
                self.db.log_pipeline_run(self.batch_id, "INGEST", "STARTED")
                logger.info("----- STAGE 1: INGEST -----")

                for fp in file_paths:
                    abs_path = os.path.abspath(fp)
                    logger.info("Processing file: %s", abs_path)
                    valid, invalid = process_file(abs_path)
                    all_valid.extend(valid)
                    all_invalid.extend(invalid)
                    self.stats["files_processed"] += 1

                if corrections_paths:
                    for cp in corrections_paths:
                        abs_path = os.path.abspath(cp)
                        logger.info("Processing corrections file: %s", abs_path)
                        corr_valid, corr_invalid = process_file(abs_path)
                        all_valid = merge_daily_and_corrections(all_valid, corr_valid)
                        all_invalid.extend(corr_invalid)
                        self.stats["files_processed"] += 1

                self.stats["records_read"] = len(all_valid) + len(all_invalid)
                self.stats["records_valid"] = len(all_valid)
                self.stats["records_invalid"] = len(all_invalid)

                self.checkpoint_mgr.save_checkpoint(
                    self.batch_id, "INGEST", "COMPLETED",
                    records_processed=self.stats["records_read"]
                )
                self.db.log_pipeline_run(
                    self.batch_id, "INGEST", "COMPLETED",
                    source_file=",".join(file_paths),
                    records_read=self.stats["records_read"],
                    records_valid=self.stats["records_valid"],
                    records_invalid=self.stats["records_invalid"]
                )

            # ================================================================
            # STAGE 2: VALIDATE — Additional quality checks
            # ================================================================
            if resume_index <= 1:
                self.db.log_pipeline_run(self.batch_id, "VALIDATE", "STARTED")
                logger.info("----- STAGE 2: VALIDATE -----")

                # Quarantine invalid records
                if all_invalid:
                    source_file = ",".join(file_paths)
                    self.db.insert_quarantine(all_invalid, self.batch_id, source_file)
                    logger.warning(
                        "%d records quarantined due to validation errors",
                        len(all_invalid)
                    )

                self.checkpoint_mgr.save_checkpoint(
                    self.batch_id, "VALIDATE", "COMPLETED",
                    records_processed=len(all_invalid)
                )
                self.db.log_pipeline_run(
                    self.batch_id, "VALIDATE", "COMPLETED",
                    records_invalid=self.stats["records_invalid"]
                )

            # ================================================================
            # STAGE 3: TRANSFORM — Deduplicate and standardize
            # ================================================================
            if resume_index <= 2:
                self.db.log_pipeline_run(self.batch_id, "TRANSFORM", "STARTED")
                logger.info("----- STAGE 3: TRANSFORM -----")

                all_valid, dup_count = deduplicate_records(all_valid)
                self.stats["duplicate_records"] = dup_count

                all_valid = standardize_records(all_valid)

                self.checkpoint_mgr.save_checkpoint(
                    self.batch_id, "TRANSFORM", "COMPLETED",
                    records_processed=len(all_valid)
                )
                self.db.log_pipeline_run(self.batch_id, "TRANSFORM", "COMPLETED")

            # ================================================================
            # STAGE 4: LOAD — Insert to staging, then MERGE to target
            # ================================================================
            if resume_index <= 3:
                self.db.log_pipeline_run(self.batch_id, "LOAD", "STARTED")
                logger.info("----- STAGE 4: LOAD (Staging + MERGE) -----")

                source_file = ",".join(file_paths)
                self.db.insert_staging_batch(all_valid, self.batch_id, source_file)

                inserted, updated, skipped = self.db.execute_merge(self.batch_id)
                self.stats["records_inserted"] = inserted
                self.stats["records_updated"] = updated
                self.stats["records_skipped"] = skipped

                self.checkpoint_mgr.save_checkpoint(
                    self.batch_id, "LOAD", "COMPLETED",
                    records_processed=inserted + updated + skipped
                )
                self.db.log_pipeline_run(
                    self.batch_id, "LOAD", "COMPLETED",
                    records_inserted=inserted,
                    records_updated=updated,
                    records_skipped=skipped
                )

            # ================================================================
            # STAGE 5: RECONCILE — Verify, report, alert
            # ================================================================
            if resume_index <= 4:
                self.db.log_pipeline_run(self.batch_id, "RECONCILE", "STARTED")
                logger.info("----- STAGE 5: RECONCILE -----")

                expected_amount = sum(float(r["amount"]) for r in all_valid)
                recon_pass, recon_results = self.recon_engine.run_all_checks(
                    self.batch_id,
                    expected_valid_count=len(all_valid),
                    expected_total_amount=expected_amount
                )

                quality_stats = {
                    "total_records": self.stats["records_read"],
                    "valid_records": self.stats["records_valid"],
                    "invalid_records": self.stats["records_invalid"],
                    "duplicate_records": self.stats["duplicate_records"],
                    "quality_score_pct": (
                        (self.stats["records_valid"] / max(self.stats["records_read"], 1)) * 100
                    )
                }

                # Persist data quality score
                cursor = self.db.get_cursor()
                cursor.execute("""
                    INSERT INTO data_quality_scores (
                        score_id, batch_id, total_records, valid_records, invalid_records,
                        duplicate_records, quality_score_pct
                    ) VALUES (seq_quality_id.NEXTVAL, :1, :2, :3, :4, :5, :6)
                """, [
                    self.batch_id,
                    quality_stats["total_records"],
                    quality_stats["valid_records"],
                    quality_stats["invalid_records"],
                    quality_stats["duplicate_records"],
                    quality_stats["quality_score_pct"]
                ])
                self.db.commit()
                cursor.close()

                self.alert_mgr.check_and_alert(self.batch_id, recon_results, quality_stats)

                # Keep within pipeline_run_log.status VARCHAR2(20)
                recon_status = "COMPLETED" if recon_pass else "COMPLETED_WARN"
                self.checkpoint_mgr.save_checkpoint(
                    self.batch_id, "RECONCILE", "COMPLETED"
                )
                self.db.log_pipeline_run(
                    self.batch_id, "RECONCILE", recon_status
                )

                if not recon_pass:
                    logger.warning(
                        "Reconciliation had failures. Review reports in '%s'",
                        self.config.get("pipeline", {}).get("reports_directory", "reports")
                    )

            # ================================================================
            # PIPELINE COMPLETE
            # ================================================================
            pipeline_end = datetime.now()
            duration = format_duration(pipeline_start, pipeline_end)

            logger.info("=" * 70)
            logger.info("PIPELINE COMPLETE — Batch: %s", self.batch_id)
            logger.info("Duration: %s", duration)
            logger.info("Stats: %s", self.stats)
            alert_summary = self.alert_mgr.get_summary()
            logger.info("Alerts: %s", alert_summary)
            logger.info("=" * 70)

            return {
                "batch_id": self.batch_id,
                "status": "SUCCESS",
                "duration": duration,
                "stats": self.stats,
                "alerts": alert_summary
            }

        except Exception as e:
            logger.error("PIPELINE FAILED — Batch: %s — Error: %s", self.batch_id, e, exc_info=True)
            self.alert_mgr.alert_pipeline_failure(
                self.batch_id, "UNKNOWN", str(e)
            )

            try:
                self.db.log_pipeline_run(
                    self.batch_id, "ERROR", "FAILED",
                    error_message=str(e)[:4000]
                )
            except Exception:
                pass

            return {
                "batch_id": self.batch_id,
                "status": "FAILED",
                "error": str(e),
                "stats": self.stats
            }

        finally:
            self.db.disconnect()

    def rollback(self, batch_id):
        """Rollback a specific batch (manual recovery)."""
        try:
            self.db.connect()
            self.rollback_mgr = RollbackManager(self.db)
            restored = self.rollback_mgr.rollback_batch(batch_id)
            logger.info("Rollback complete: %d rows restored for batch %s", restored, batch_id)
            return restored
        finally:
            self.db.disconnect()


# ============================================================================
# CLI Entry Point
# ============================================================================
def main():
    """Command-line interface for running the pipeline."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Incremental Data Load Pipeline with Reconciliation"
    )
    parser.add_argument(
        "files", nargs="+",
        help="CSV file paths for daily transactions"
    )
    parser.add_argument(
        "--corrections", nargs="*", default=None,
        help="CSV file paths for late-arriving corrections"
    )
    parser.add_argument(
        "--batch-id", default=None,
        help="Batch ID (for resuming a failed run)"
    )
    parser.add_argument(
        "--config", default="config.json",
        help="Path to config.json (default: config.json)"
    )
    parser.add_argument(
        "--rollback",
        help="Rollback a specific batch ID instead of running pipeline"
    )

    args = parser.parse_args()

    pipeline = IncrementalLoadPipeline(config_path=args.config)

    if args.rollback:
        pipeline.rollback(args.rollback)
    else:
        result = pipeline.run(
            file_paths=args.files,
            batch_id=args.batch_id,
            corrections_paths=args.corrections
        )
        print("\n" + "=" * 50)
        print(f"Pipeline Result: {result['status']}")
        print(f"Batch ID: {result['batch_id']}")
        if result["status"] == "SUCCESS":
            print(f"Duration: {result['duration']}")
            s = result["stats"]
            print(f"Records — Read: {s['records_read']}, Valid: {s['records_valid']}, "
                  f"Invalid: {s['records_invalid']}")
            print(f"Actions — Inserted: {s['records_inserted']}, Updated: {s['records_updated']}, "
                  f"Skipped: {s['records_skipped']}")
        else:
            print(f"Error: {result.get('error', 'Unknown')}")
        print("=" * 50)

        sys.exit(0 if result["status"] == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
