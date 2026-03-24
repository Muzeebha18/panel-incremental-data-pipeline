-- ============================================================================
-- MERGE PROCEDURE: Heart of the incremental load
-- Performs UPSERT with hash-based change detection
-- Compatible with Oracle 11g XE
-- ============================================================================

CREATE OR REPLACE PROCEDURE sp_merge_transactions(
    p_batch_id   IN VARCHAR2,
    p_inserted   OUT NUMBER,
    p_updated    OUT NUMBER,
    p_skipped    OUT NUMBER
) AS
BEGIN
    p_inserted := 0;
    p_updated  := 0;
    p_skipped  := 0;

    -- Step 1: Snapshot existing rows that will be affected (for rollback)
    INSERT INTO rollback_snapshots (
        snapshot_id, batch_id, transaction_id, transaction_date, account_id,
        transaction_type, amount, currency, merchant, description,
        channel, row_hash, version
    )
    SELECT seq_snapshot_id.NEXTVAL, p_batch_id, t.transaction_id, t.transaction_date,
           t.account_id, t.transaction_type, t.amount, t.currency, t.merchant,
           t.description, t.channel, t.row_hash, t.version
    FROM target_transactions t
    WHERE t.transaction_id IN (
        SELECT s.transaction_id FROM stg_transactions s
        WHERE s.batch_id = p_batch_id AND s.record_status = 'VALID'
    );

    -- Step 2: MERGE - the core upsert with hash comparison
    MERGE INTO target_transactions t
    USING (
        SELECT transaction_id, transaction_date, account_id, transaction_type,
               amount, currency, merchant, description, channel, row_hash
        FROM stg_transactions
        WHERE batch_id = p_batch_id
          AND record_status = 'VALID'
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
            t.version           = t.version + 1,
            t.updated_batch_id  = p_batch_id,
            t.updated_timestamp = SYSTIMESTAMP
        WHERE t.row_hash != s.row_hash  -- Only update if data actually changed
    WHEN NOT MATCHED THEN
        INSERT (transaction_id, transaction_date, account_id, transaction_type,
                amount, currency, merchant, description, channel, row_hash,
                version, is_active, created_batch_id, updated_batch_id)
        VALUES (s.transaction_id, s.transaction_date, s.account_id, s.transaction_type,
                s.amount, s.currency, s.merchant, s.description, s.channel, s.row_hash,
                1, 1, p_batch_id, p_batch_id);

    -- Step 3: Log history for inserted records (new)
    INSERT INTO transaction_history (
        history_id, transaction_id, transaction_date, account_id, transaction_type,
        amount, currency, merchant, description, channel, row_hash,
        version, change_type, batch_id
    )
    SELECT seq_history_id.NEXTVAL, s.transaction_id, s.transaction_date, s.account_id,
           s.transaction_type, s.amount, s.currency, s.merchant, s.description,
           s.channel, s.row_hash, 1, 'INSERT', p_batch_id
    FROM stg_transactions s
    WHERE s.batch_id = p_batch_id
      AND s.record_status = 'VALID'
      AND NOT EXISTS (
          SELECT 1 FROM rollback_snapshots r
          WHERE r.transaction_id = s.transaction_id AND r.batch_id = p_batch_id
      );

    p_inserted := SQL%ROWCOUNT;

    -- Step 4: Log history for updated/corrected records
    INSERT INTO transaction_history (
        history_id, transaction_id, transaction_date, account_id, transaction_type,
        amount, currency, merchant, description, channel, row_hash,
        version, change_type, batch_id
    )
    SELECT seq_history_id.NEXTVAL, t.transaction_id, t.transaction_date, t.account_id,
           t.transaction_type, t.amount, t.currency, t.merchant, t.description,
           t.channel, t.row_hash, t.version,
           CASE WHEN t.version > 1 THEN 'CORRECTION' ELSE 'UPDATE' END,
           p_batch_id
    FROM target_transactions t
    WHERE t.updated_batch_id = p_batch_id
      AND t.version > 1
      AND EXISTS (
          SELECT 1 FROM rollback_snapshots r
          WHERE r.transaction_id = t.transaction_id AND r.batch_id = p_batch_id
            AND r.row_hash != t.row_hash
      );

    p_updated := SQL%ROWCOUNT;

    -- Calculate skipped (hash matched, no actual change)
    SELECT COUNT(*) INTO p_skipped
    FROM stg_transactions s
    WHERE s.batch_id = p_batch_id
      AND s.record_status = 'VALID'
      AND EXISTS (
          SELECT 1 FROM rollback_snapshots r
          WHERE r.transaction_id = s.transaction_id
            AND r.batch_id = p_batch_id
            AND r.row_hash = s.row_hash
      );

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END sp_merge_transactions;
/

-- ============================================================================
-- ROLLBACK PROCEDURE: Restore target table to pre-merge state
-- ============================================================================
CREATE OR REPLACE PROCEDURE sp_rollback_batch(
    p_batch_id   IN VARCHAR2,
    p_restored   OUT NUMBER
) AS
BEGIN
    p_restored := 0;

    -- Restore rows that were updated (revert to snapshot)
    MERGE INTO target_transactions t
    USING (
        SELECT * FROM rollback_snapshots WHERE batch_id = p_batch_id
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
            t.updated_timestamp = SYSTIMESTAMP;

    p_restored := SQL%ROWCOUNT;

    -- Remove rows that were newly inserted in this batch
    DELETE FROM target_transactions
    WHERE created_batch_id = p_batch_id
      AND transaction_id NOT IN (
          SELECT transaction_id FROM rollback_snapshots WHERE batch_id = p_batch_id
      );

    p_restored := p_restored + SQL%ROWCOUNT;

    -- Clean up history for this batch
    DELETE FROM transaction_history WHERE batch_id = p_batch_id;

    -- Update pipeline run log
    UPDATE pipeline_run_log
    SET status = 'ROLLED_BACK', end_time = SYSTIMESTAMP
    WHERE batch_id = p_batch_id;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END sp_rollback_batch;
/
