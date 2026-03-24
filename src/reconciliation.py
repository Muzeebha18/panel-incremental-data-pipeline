"""
Reconciliation module: 3-point verification (row count, total amount, distinct keys).
Generates reports and persists results to the reconciliation_results table.
"""

import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


class ReconciliationEngine:
    """Performs post-merge data reconciliation checks."""

    def __init__(self, db_manager, config):
        self.db = db_manager
        self.tolerance_pct = config.get("reconciliation", {}).get("amount_tolerance_pct", 0.01)
        self.fail_on_mismatch = config.get("reconciliation", {}).get("fail_on_mismatch", True)
        self.reports_dir = config.get("pipeline", {}).get("reports_directory", "reports")

    def run_all_checks(self, batch_id, expected_valid_count, expected_total_amount):
        """Run all reconciliation checks and return overall pass/fail."""
        results = []

        results.append(self._check_row_count(batch_id, expected_valid_count))
        results.append(self._check_total_amount(batch_id, expected_total_amount))
        results.append(self._check_distinct_keys(batch_id))
        results.append(self._check_orphan_records(batch_id))

        self._persist_results(batch_id, results)
        self._generate_report(batch_id, results)

        overall_pass = all(r["status"] in ("PASS", "WARNING") for r in results)
        pass_count = sum(1 for r in results if r["status"] == "PASS")
        fail_count = sum(1 for r in results if r["status"] == "FAIL")

        logger.info(
            "Reconciliation for batch %s: %d PASS, %d FAIL — Overall: %s",
            batch_id, pass_count, fail_count,
            "PASS" if overall_pass else "FAIL"
        )

        if not overall_pass and self.fail_on_mismatch:
            logger.error("Reconciliation FAILED for batch %s. Pipeline halted.", batch_id)

        return overall_pass, results

    def _check_row_count(self, batch_id, expected_count):
        """Verify staging valid count matches target affected count."""
        cursor = self.db.get_cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM target_transactions
            WHERE created_batch_id = :bid OR updated_batch_id = :bid
        """, {"bid": batch_id})
        actual_count = cursor.fetchone()[0]
        cursor.close()

        difference = expected_count - actual_count
        status = "PASS" if difference == 0 else "FAIL"

        result = {
            "check_type": "ROW_COUNT",
            "source_value": expected_count,
            "target_value": actual_count,
            "difference": difference,
            "tolerance_pct": 0.0,
            "status": status,
            "details": f"Expected {expected_count} rows affected, found {actual_count}"
        }

        logger.info(
            "ROW_COUNT check: Expected=%d, Actual=%d — %s",
            expected_count, actual_count, status
        )
        return result

    def _check_total_amount(self, batch_id, expected_amount):
        """Verify sum of amounts matches between staging and target."""
        cursor = self.db.get_cursor()

        cursor.execute("""
            SELECT NVL(SUM(amount), 0) FROM target_transactions
            WHERE is_active = 1
              AND (created_batch_id = :bid OR updated_batch_id = :bid)
        """, {"bid": batch_id})
        actual_amount = float(cursor.fetchone()[0])
        cursor.close()

        difference = abs(expected_amount - actual_amount)
        threshold = abs(expected_amount * self.tolerance_pct / 100) if expected_amount else 0.01

        if difference <= threshold:
            status = "PASS"
        elif difference <= threshold * 5:
            status = "WARNING"
        else:
            status = "FAIL"

        result = {
            "check_type": "TOTAL_AMOUNT",
            "source_value": expected_amount,
            "target_value": actual_amount,
            "difference": difference,
            "tolerance_pct": self.tolerance_pct,
            "status": status,
            "details": f"Source sum={expected_amount:.2f}, Target sum={actual_amount:.2f}, Diff={difference:.2f}"
        }

        logger.info(
            "TOTAL_AMOUNT check: Source=%.2f, Target=%.2f, Diff=%.2f — %s",
            expected_amount, actual_amount, difference, status
        )
        return result

    def _check_distinct_keys(self, batch_id):
        """Check for unexpected duplicates in target table."""
        cursor = self.db.get_cursor()

        cursor.execute("""
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT transaction_id) AS distinct_keys
            FROM target_transactions
            WHERE is_active = 1
        """)
        row = cursor.fetchone()
        total = row[0]
        distinct = row[1]
        cursor.close()

        difference = total - distinct
        status = "PASS" if difference == 0 else "FAIL"

        result = {
            "check_type": "DISTINCT_KEYS",
            "source_value": total,
            "target_value": distinct,
            "difference": difference,
            "tolerance_pct": 0.0,
            "status": status,
            "details": f"Total rows={total}, Distinct keys={distinct}, Duplicates={difference}"
        }

        logger.info("DISTINCT_KEYS check: Total=%d, Distinct=%d — %s", total, distinct, status)
        return result

    def _check_orphan_records(self, batch_id):
        """Check for staging records that did not make it to target."""
        cursor = self.db.get_cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM stg_transactions s
            WHERE s.batch_id = :1 AND s.record_status = 'VALID'
              AND NOT EXISTS (
                  SELECT 1 FROM target_transactions t
                  WHERE t.transaction_id = s.transaction_id
              )
        """, [batch_id])
        orphan_count = cursor.fetchone()[0]
        cursor.close()

        status = "PASS" if orphan_count == 0 else "FAIL"

        result = {
            "check_type": "ORPHAN_RECORDS",
            "source_value": orphan_count,
            "target_value": 0,
            "difference": orphan_count,
            "tolerance_pct": 0.0,
            "status": status,
            "details": f"{orphan_count} staging records not found in target"
        }

        logger.info("ORPHAN_RECORDS check: %d orphans — %s", orphan_count, status)
        return result

    def _persist_results(self, batch_id, results):
        """Save reconciliation results to the database."""
        cursor = self.db.get_cursor()
        for r in results:
            cursor.execute("""
                INSERT INTO reconciliation_results (
                    recon_id, batch_id, check_type, source_value, target_value,
                    difference, tolerance_pct, status, details
                ) VALUES (seq_recon_id.NEXTVAL, :1, :2, :3, :4, :5, :6, :7, :8)
            """, [
                batch_id, r["check_type"], r["source_value"], r["target_value"],
                r["difference"], r["tolerance_pct"], r["status"], r["details"]
            ])
        self.db.commit()
        cursor.close()

    def _generate_report(self, batch_id, results):
        """Generate a JSON reconciliation report file."""
        os.makedirs(self.reports_dir, exist_ok=True)
        report_path = os.path.join(
            self.reports_dir,
            f"recon_report_{batch_id}.json"
        )

        report = {
            "batch_id": batch_id,
            "generated_at": datetime.now().isoformat(),
            "overall_status": "PASS" if all(r["status"] in ("PASS", "WARNING") for r in results) else "FAIL",
            "checks": results
        }

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info("Reconciliation report saved: %s", report_path)
