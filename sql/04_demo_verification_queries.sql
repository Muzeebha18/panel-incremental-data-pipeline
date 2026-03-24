-- ============================================================================
-- DEMO VERIFICATION QUERIES
-- Keep this open in SQL Developer during the demo.
-- Run each query AFTER the corresponding step in demo_interactive.py
-- ============================================================================

-- ============================================================================
-- AFTER STEP 1: Verify Day 1 load (20 transactions)
-- ============================================================================
SELECT transaction_id, transaction_date, account_id, 
       transaction_type, amount, version, row_hash
FROM target_transactions 
ORDER BY transaction_id;

-- Quick count
SELECT COUNT(*) AS total_loaded FROM target_transactions;

-- ============================================================================
-- AFTER STEP 3: Verify corrections from Day 2
-- ============================================================================

-- Show corrected records (version > 1)
SELECT transaction_id, amount, version, created_batch_id, updated_batch_id
FROM target_transactions 
WHERE version > 1 
ORDER BY transaction_id;

-- Compare OLD vs NEW amounts using rollback snapshots
SELECT r.transaction_id,
       r.amount AS old_amount,
       t.amount AS new_amount,
       t.amount - r.amount AS difference,
       r.row_hash AS old_hash,
       t.row_hash AS new_hash
FROM rollback_snapshots r
JOIN target_transactions t ON r.transaction_id = t.transaction_id
WHERE r.row_hash != t.row_hash
ORDER BY r.transaction_id;

-- Full audit trail of corrections
SELECT transaction_id, change_type, amount, version, batch_id, effective_from
FROM transaction_history 
WHERE change_type = 'CORRECTION'
ORDER BY effective_from;

-- Prove zero duplicates (total = distinct)
SELECT COUNT(*) AS total_rows, 
       COUNT(DISTINCT transaction_id) AS unique_keys,
       CASE WHEN COUNT(*) = COUNT(DISTINCT transaction_id) 
            THEN 'NO DUPLICATES' ELSE 'DUPLICATES FOUND!' END AS status
FROM target_transactions 
WHERE is_active = 1;

-- ============================================================================
-- AFTER STEP 5: Verify quarantine (7 bad records)
-- ============================================================================

-- All quarantined records with error details
SELECT transaction_id, error_code, error_message, batch_id
FROM quarantine_records 
ORDER BY quarantine_id;

-- Error summary by type
SELECT error_code, COUNT(*) AS error_count
FROM quarantine_records
GROUP BY error_code
ORDER BY error_count DESC;

-- Data quality scores trending
SELECT batch_id, total_records, valid_records, invalid_records, 
       duplicate_records, quality_score_pct
FROM data_quality_scores 
ORDER BY check_timestamp;

-- ============================================================================
-- AFTER STEP 7: Reconciliation results
-- ============================================================================

-- All reconciliation checks (should be 12 checks, all PASS)
SELECT batch_id, check_type, source_value, target_value, 
       difference, status, details
FROM reconciliation_results 
ORDER BY recon_id;

-- Account-level financial summary
SELECT account_id,
       COUNT(*) AS txn_count,
       SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) AS total_credits,
       SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END) AS total_debits,
       SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE -amount END) AS net_balance
FROM target_transactions 
WHERE is_active = 1
GROUP BY account_id 
ORDER BY account_id;

-- ============================================================================
-- AFTER STEP 8: Verify rollback
-- ============================================================================

-- Pipeline log showing ROLLED_BACK status
SELECT batch_id, pipeline_stage, status, 
       records_inserted, records_updated, records_skipped
FROM pipeline_run_log 
ORDER BY run_id;

-- Target table count (should be 35 after rollback, not 38)
SELECT COUNT(*) AS final_count FROM target_transactions WHERE is_active = 1;

-- ============================================================================
-- FINAL: Complete pipeline health dashboard
-- ============================================================================

-- Full pipeline execution history
SELECT batch_id, pipeline_stage, status,
       records_read, records_valid, records_invalid,
       records_inserted, records_updated, records_skipped,
       start_time, end_time
FROM pipeline_run_log 
ORDER BY start_time;

-- Checkpoint history (resume-from-failure state)
SELECT batch_id, stage_name, stage_status, records_processed
FROM pipeline_checkpoints 
ORDER BY created_timestamp;

-- Complete transaction history (audit trail)
SELECT transaction_id, change_type, amount, version, batch_id, effective_from
FROM transaction_history 
ORDER BY effective_from;
