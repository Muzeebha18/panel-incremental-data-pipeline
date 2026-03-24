"""
Checkpointing module: Track pipeline stage progress for resume-from-failure.
If a pipeline run crashes at stage 3, it can restart from stage 3 instead of re-doing everything.
"""

import json
import logging

logger = logging.getLogger(__name__)

PIPELINE_STAGES = ["INGEST", "VALIDATE", "TRANSFORM", "LOAD", "RECONCILE"]


class CheckpointManager:
    """Manages pipeline checkpoints for crash recovery."""

    def __init__(self, db_manager):
        self.db = db_manager

    def save_checkpoint(self, batch_id, stage_name, status, records_processed=0,
                        checkpoint_data=None):
        """Save checkpoint after completing a pipeline stage."""
        cursor = self.db.get_cursor()
        data_json = json.dumps(checkpoint_data) if checkpoint_data else None

        try:
            cursor.execute("""
                INSERT INTO pipeline_checkpoints (
                    checkpoint_id, batch_id, stage_name, stage_status, records_processed, checkpoint_data
                ) VALUES (seq_checkpoint_id.NEXTVAL, :1, :2, :3, :4, :5)
            """, [batch_id, stage_name, status, records_processed, data_json])
        except Exception:
            cursor.execute("""
                UPDATE pipeline_checkpoints
                SET stage_status = :1, records_processed = :2,
                    checkpoint_data = :3, created_timestamp = SYSTIMESTAMP
                WHERE batch_id = :4 AND stage_name = :5
            """, [status, records_processed, data_json, batch_id, stage_name])

        self.db.commit()
        cursor.close()
        logger.info(
            "Checkpoint saved: batch=%s, stage=%s, status=%s, records=%d",
            batch_id, stage_name, status, records_processed
        )

    def get_last_completed_stage(self, batch_id):
        """Find the last successfully completed stage for a batch."""
        cursor = self.db.get_cursor()
        cursor.execute("""
            SELECT stage_name, records_processed, checkpoint_data
            FROM (
                SELECT stage_name, records_processed, checkpoint_data
                FROM pipeline_checkpoints
                WHERE batch_id = :1 AND stage_status = 'COMPLETED'
                ORDER BY created_timestamp DESC
            ) WHERE ROWNUM = 1
        """, [batch_id])
        row = cursor.fetchone()
        cursor.close()

        if row:
            stage_name, records_processed, data_json = row
            checkpoint_data = json.loads(data_json) if data_json else None
            logger.info(
                "Last checkpoint for batch %s: stage=%s, records=%d",
                batch_id, stage_name, records_processed
            )
            return stage_name, records_processed, checkpoint_data

        logger.info("No checkpoint found for batch %s — starting from scratch", batch_id)
        return None, 0, None

    def get_resume_stage_index(self, batch_id):
        """
        Determine which stage to resume from.
        Returns the index of the NEXT stage to execute.
        """
        last_stage, _, _ = self.get_last_completed_stage(batch_id)

        if last_stage is None:
            return 0

        if last_stage in PIPELINE_STAGES:
            idx = PIPELINE_STAGES.index(last_stage)
            next_idx = idx + 1
            if next_idx >= len(PIPELINE_STAGES):
                logger.info("All stages already completed for batch %s", batch_id)
                return len(PIPELINE_STAGES)
            logger.info(
                "Resuming batch %s from stage: %s",
                batch_id, PIPELINE_STAGES[next_idx]
            )
            return next_idx

        return 0

    def clear_checkpoints(self, batch_id):
        """Remove all checkpoints for a batch (used before fresh re-run)."""
        cursor = self.db.get_cursor()
        cursor.execute(
            "DELETE FROM pipeline_checkpoints WHERE batch_id = :1", [batch_id]
        )
        self.db.commit()
        cursor.close()
        logger.info("Cleared all checkpoints for batch %s", batch_id)
