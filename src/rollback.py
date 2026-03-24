"""
Rollback module: Restore target table to pre-merge state using snapshots.
Provides batch-level rollback capability for recovery scenarios.
"""

import logging

logger = logging.getLogger(__name__)


class RollbackManager:
    """Manages rollback operations using pre-merge snapshots."""

    def __init__(self, db_manager):
        self.db = db_manager

    def rollback_batch(self, batch_id):
        """
        Rollback all changes made by a specific batch:
        1. Restore updated rows to their pre-merge state
        2. Delete newly inserted rows
        3. Clean up history records for this batch
        """
        cursor = self.db.get_cursor()
        restored_count = 0

        try:
            # Restore rows that were updated (revert to snapshot values)
            cursor.execute("""
                MERGE INTO target_transactions t
                USING (
                    SELECT * FROM rollback_snapshots WHERE batch_id = :1
                ) s
                ON (t.transaction_id = s.transaction_id)
                WHEN MATCHED THEN
                    UPDATE SET
                        t.transaction_date  = s.transaction_date,
                        t.account_id        = s.account_id,
                        t.transaction_type  = s.transaction_type,
                        t.amount            = s.amount,
                        t.currency          = s.currency,
                        t.merchant          = s.merchant,
                        t.description       = s.description,
                        t.channel           = s.channel,
                        t.row_hash          = s.row_hash,
                        t.version           = s.version,
                        t.updated_timestamp = SYSTIMESTAMP
            """, [batch_id])
            restored_count += cursor.rowcount
            logger.info("Restored %d updated rows to pre-merge state", cursor.rowcount)

            # Delete rows that were newly inserted by this batch
            cursor.execute("""
                DELETE FROM target_transactions
                WHERE created_batch_id = :1
                  AND transaction_id NOT IN (
                      SELECT transaction_id FROM rollback_snapshots WHERE batch_id = :1
                  )
            """, [batch_id])
            deleted_count = cursor.rowcount
            restored_count += deleted_count
            logger.info("Deleted %d newly inserted rows", deleted_count)

            # Clean up history for this batch
            cursor.execute(
                "DELETE FROM transaction_history WHERE batch_id = :1", [batch_id]
            )

            # Update pipeline log
            cursor.execute("""
                UPDATE pipeline_run_log
                SET status = 'ROLLED_BACK', end_time = SYSTIMESTAMP
                WHERE batch_id = :1
            """, [batch_id])

            self.db.commit()
            cursor.close()

            logger.info(
                "Rollback complete for batch %s: %d rows affected",
                batch_id, restored_count
            )
            return restored_count

        except Exception as e:
            self.db.connection.rollback()
            cursor.close()
            logger.error("Rollback FAILED for batch %s: %s", batch_id, e)
            raise

    def has_snapshot(self, batch_id):
        """Check if a rollback snapshot exists for a batch."""
        cursor = self.db.get_cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM rollback_snapshots WHERE batch_id = :1",
            [batch_id]
        )
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
