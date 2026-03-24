"""
Alerting module: Generate alerts for data quality issues, reconciliation failures,
and pipeline anomalies. In production, these would integrate with email/Slack/PagerDuty.
"""

import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


class AlertManager:
    """Manages pipeline alerts and notifications."""

    def __init__(self, config):
        self.enabled = config.get("alerting", {}).get("enabled", True)
        self.alert_on_recon_failure = config.get("alerting", {}).get(
            "alert_on_recon_failure", True
        )
        self.duplicate_threshold = config.get("alerting", {}).get(
            "alert_on_duplicate_count", 1
        )
        self.reports_dir = config.get("pipeline", {}).get("reports_directory", "reports")
        self.alerts = []

    def check_and_alert(self, batch_id, recon_results, quality_stats):
        """Evaluate results and generate appropriate alerts."""
        if not self.enabled:
            return

        for check in recon_results:
            if check["status"] == "FAIL":
                self._add_alert(
                    batch_id=batch_id,
                    severity="CRITICAL",
                    alert_type="RECONCILIATION_FAILURE",
                    message=f"Reconciliation check FAILED: {check['check_type']} — {check['details']}"
                )
            elif check["status"] == "WARNING":
                self._add_alert(
                    batch_id=batch_id,
                    severity="WARNING",
                    alert_type="RECONCILIATION_WARNING",
                    message=f"Reconciliation check WARNING: {check['check_type']} — {check['details']}"
                )

        if quality_stats.get("invalid_records", 0) > 0:
            self._add_alert(
                batch_id=batch_id,
                severity="WARNING",
                alert_type="DATA_QUALITY",
                message=(
                    f"{quality_stats['invalid_records']} records failed validation "
                    f"and were quarantined (quality score: {quality_stats.get('quality_score_pct', 0):.1f}%)"
                )
            )

        if quality_stats.get("duplicate_records", 0) >= self.duplicate_threshold:
            self._add_alert(
                batch_id=batch_id,
                severity="WARNING",
                alert_type="DUPLICATE_DETECTED",
                message=f"{quality_stats['duplicate_records']} duplicate records detected in batch"
            )

        self._save_alert_report(batch_id)
        return self.alerts

    def alert_pipeline_failure(self, batch_id, stage, error_message):
        """Generate critical alert for pipeline failure."""
        self._add_alert(
            batch_id=batch_id,
            severity="CRITICAL",
            alert_type="PIPELINE_FAILURE",
            message=f"Pipeline FAILED at stage '{stage}': {error_message}"
        )
        self._save_alert_report(batch_id)

    def _add_alert(self, batch_id, severity, alert_type, message):
        """Add an alert to the queue and log it."""
        alert = {
            "batch_id": batch_id,
            "timestamp": datetime.now().isoformat(),
            "severity": severity,
            "alert_type": alert_type,
            "message": message
        }
        self.alerts.append(alert)

        if severity == "CRITICAL":
            logger.critical("ALERT [%s] %s: %s", batch_id, alert_type, message)
        elif severity == "WARNING":
            logger.warning("ALERT [%s] %s: %s", batch_id, alert_type, message)
        else:
            logger.info("ALERT [%s] %s: %s", batch_id, alert_type, message)

    def _save_alert_report(self, batch_id):
        """Save alerts to a JSON report file."""
        if not self.alerts:
            return

        os.makedirs(self.reports_dir, exist_ok=True)
        report_path = os.path.join(
            self.reports_dir, f"alerts_{batch_id}.json"
        )

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(self.alerts, f, indent=2)

        logger.info("Alert report saved: %s (%d alerts)", report_path, len(self.alerts))

    def get_summary(self):
        """Return a summary of all alerts raised."""
        critical = sum(1 for a in self.alerts if a["severity"] == "CRITICAL")
        warning = sum(1 for a in self.alerts if a["severity"] == "WARNING")
        return {
            "total_alerts": len(self.alerts),
            "critical": critical,
            "warnings": warning
        }
