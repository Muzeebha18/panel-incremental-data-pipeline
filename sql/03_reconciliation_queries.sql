-- ============================================================================
-- RECONCILIATION & ANALYTICS QUERIES
-- Used by the pipeline and available for manual verification
-- ============================================================================

-- ============================================================================
-- 1. ROW COUNT RECONCILIATION: Source staging vs Target
-- ============================================================================
SELECT 
    'ROW_COUNT' AS check_type,
    stg.stg_count AS source_value,
    tgt.tgt_count AS target_value,
    stg.stg_count - tgt.tgt_count AS difference,
    CASE 
        WHEN stg.stg_count = tgt.tgt_count THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM 
    (SELECT COUNT(*) AS stg_count FROM stg_transactions 
     WHERE batch_id = :batch_id AND record_status = 'VALID') stg,
    (SELECT COUNT(*) AS tgt_count FROM target_transactions 
     WHERE created_batch_id = :batch_id OR updated_batch_id = :batch_id) tgt;

-- ============================================================================
-- 2. TOTAL AMOUNT RECONCILIATION
-- ============================================================================
SELECT 
    'TOTAL_AMOUNT' AS check_type,
    stg.stg_amount AS source_value,
    tgt.tgt_amount AS target_value,
    ABS(stg.stg_amount - tgt.tgt_amount) AS difference,
    CASE 
        WHEN ABS(stg.stg_amount - NVL(tgt.tgt_amount, 0)) <= 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM 
    (SELECT NVL(SUM(amount), 0) AS stg_amount FROM stg_transactions 
     WHERE batch_id = :batch_id AND record_status = 'VALID') stg,
    (SELECT NVL(SUM(amount), 0) AS tgt_amount FROM target_transactions 
     WHERE is_active = 1 
       AND (created_batch_id = :batch_id OR updated_batch_id = :batch_id)) tgt;

-- ============================================================================
-- 3. DUPLICATE DETECTION QUERY
-- ============================================================================
SELECT 
    transaction_id, 
    COUNT(*) AS occurrence_count,
    LISTAGG(batch_id, ', ') WITHIN GROUP (ORDER BY load_timestamp) AS batches
FROM stg_transactions
WHERE batch_id = :batch_id
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- ============================================================================
-- 4. DATA QUALITY DASHBOARD QUERY
-- ============================================================================
SELECT 
    dq.batch_id,
    dq.total_records,
    dq.valid_records,
    dq.invalid_records,
    dq.duplicate_records,
    dq.quality_score_pct,
    prl.source_file,
    prl.start_time,
    prl.end_time,
    EXTRACT(SECOND FROM (prl.end_time - prl.start_time)) AS duration_seconds
FROM data_quality_scores dq
JOIN pipeline_run_log prl ON dq.batch_id = prl.batch_id 
    AND prl.pipeline_stage = 'RECONCILE'
ORDER BY dq.check_timestamp DESC;

-- ============================================================================
-- 5. CORRECTION TRACKING: All late-arriving corrections
-- ============================================================================
SELECT 
    th.transaction_id,
    th.change_type,
    th.version,
    rs.amount AS previous_amount,
    th.amount AS corrected_amount,
    th.amount - rs.amount AS amount_difference,
    th.batch_id,
    th.effective_from
FROM transaction_history th
JOIN rollback_snapshots rs 
    ON th.transaction_id = rs.transaction_id 
    AND th.batch_id = rs.batch_id
WHERE th.change_type = 'CORRECTION'
ORDER BY th.effective_from DESC;

-- ============================================================================
-- 6. ACCOUNT-LEVEL AGGREGATION (Analytics)
-- ============================================================================
SELECT 
    account_id,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) AS total_credits,
    SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END) AS total_debits,
    SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE -amount END) AS net_balance,
    MIN(transaction_date) AS first_transaction,
    MAX(transaction_date) AS last_transaction
FROM target_transactions
WHERE is_active = 1
GROUP BY account_id
ORDER BY account_id;

-- ============================================================================
-- 7. PIPELINE HEALTH MONITOR
-- ============================================================================
SELECT 
    batch_id,
    pipeline_stage,
    status,
    records_read,
    records_valid,
    records_invalid,
    records_inserted,
    records_updated,
    records_skipped,
    start_time,
    end_time,
    error_message
FROM pipeline_run_log
ORDER BY start_time DESC
FETCH FIRST 20 ROWS ONLY;

-- ============================================================================
-- 8. QUARANTINE ANALYSIS
-- ============================================================================
SELECT 
    error_code,
    error_message,
    COUNT(*) AS error_count,
    MIN(quarantine_timestamp) AS first_seen,
    MAX(quarantine_timestamp) AS last_seen
FROM quarantine_records
GROUP BY error_code, error_message
ORDER BY error_count DESC;
