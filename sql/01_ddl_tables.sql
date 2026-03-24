-- ============================================================================
-- LARGE-SCALE INCREMENTAL DATA LOAD WITH RECONCILIATION
-- DDL Script: Core Tables, Staging, Audit, and Control Tables
-- Target Database: Oracle 11g XE | Schema: INCR_LOAD
-- ============================================================================

-- ============================================================================
-- SEQUENCES (Oracle 11g does not support IDENTITY columns)
-- ============================================================================
CREATE SEQUENCE seq_history_id    START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_quarantine_id START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_run_id        START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_recon_id      START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_checkpoint_id START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_snapshot_id   START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_quality_id    START WITH 1 INCREMENT BY 1 NOCACHE;

-- ============================================================================
-- 1. STAGING TABLE: Raw data lands here before validation & merge
-- ============================================================================
CREATE TABLE stg_transactions (
    transaction_id    VARCHAR2(50),
    transaction_date  DATE,
    account_id        VARCHAR2(50),
    transaction_type  VARCHAR2(20),
    amount            NUMBER(18,2),
    currency          VARCHAR2(10),
    merchant          VARCHAR2(200),
    description       VARCHAR2(500),
    channel           VARCHAR2(50),
    row_hash          VARCHAR2(64),
    batch_id          VARCHAR2(50),
    load_timestamp    TIMESTAMP DEFAULT SYSTIMESTAMP,
    source_file       VARCHAR2(500),
    record_status     VARCHAR2(20) DEFAULT 'PENDING'
);

CREATE INDEX idx_stg_txn_id ON stg_transactions(transaction_id);
CREATE INDEX idx_stg_batch ON stg_transactions(batch_id);

-- ============================================================================
-- 2. TARGET TABLE: Final clean transaction data (single source of truth)
-- ============================================================================
CREATE TABLE target_transactions (
    transaction_id    VARCHAR2(50)   NOT NULL,
    transaction_date  DATE           NOT NULL,
    account_id        VARCHAR2(50)   NOT NULL,
    transaction_type  VARCHAR2(20)   NOT NULL,
    amount            NUMBER(18,2)   NOT NULL,
    currency          VARCHAR2(10)   DEFAULT 'INR',
    merchant          VARCHAR2(200),
    description       VARCHAR2(500),
    channel           VARCHAR2(50),
    row_hash          VARCHAR2(64)   NOT NULL,
    version           NUMBER(10)     DEFAULT 1,
    is_active         NUMBER(1)      DEFAULT 1,
    created_batch_id  VARCHAR2(50),
    updated_batch_id  VARCHAR2(50),
    created_timestamp TIMESTAMP      DEFAULT SYSTIMESTAMP,
    updated_timestamp TIMESTAMP      DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_target_txn PRIMARY KEY (transaction_id)
);

CREATE INDEX idx_target_hash ON target_transactions(row_hash);
CREATE INDEX idx_target_acct ON target_transactions(account_id);
CREATE INDEX idx_target_date ON target_transactions(transaction_date);
CREATE INDEX idx_target_active ON target_transactions(is_active);

-- Production note: In Oracle Enterprise Edition, add RANGE partitioning
-- by transaction_date for fast date-range queries and partition pruning:
-- PARTITION BY RANGE (transaction_date)
-- (PARTITION p_2026_02 VALUES LESS THAN (TO_DATE('2026-03-01','YYYY-MM-DD')),
--  PARTITION p_2026_03 VALUES LESS THAN (TO_DATE('2026-04-01','YYYY-MM-DD')));

-- ============================================================================
-- 3. TRANSACTION HISTORY: Full audit trail of all changes (SCD Type-2 style)
-- ============================================================================
CREATE TABLE transaction_history (
    history_id        NUMBER PRIMARY KEY,
    transaction_id    VARCHAR2(50)   NOT NULL,
    transaction_date  DATE,
    account_id        VARCHAR2(50),
    transaction_type  VARCHAR2(20),
    amount            NUMBER(18,2),
    currency          VARCHAR2(10),
    merchant          VARCHAR2(200),
    description       VARCHAR2(500),
    channel           VARCHAR2(50),
    row_hash          VARCHAR2(64),
    version           NUMBER(10),
    change_type       VARCHAR2(20),
    batch_id          VARCHAR2(50),
    effective_from    TIMESTAMP DEFAULT SYSTIMESTAMP,
    effective_to      TIMESTAMP,
    created_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE INDEX idx_hist_txn_id ON transaction_history(transaction_id);
CREATE INDEX idx_hist_batch ON transaction_history(batch_id);

-- ============================================================================
-- 4. QUARANTINE TABLE: Invalid/rejected records with error details
-- ============================================================================
CREATE TABLE quarantine_records (
    quarantine_id     NUMBER PRIMARY KEY,
    transaction_id    VARCHAR2(50),
    raw_data          VARCHAR2(4000),
    error_code        VARCHAR2(50),
    error_message     VARCHAR2(1000),
    batch_id          VARCHAR2(50),
    source_file       VARCHAR2(500),
    quarantine_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE INDEX idx_quar_batch ON quarantine_records(batch_id);

-- ============================================================================
-- 5. PIPELINE RUN LOG: Track every pipeline execution
-- ============================================================================
CREATE TABLE pipeline_run_log (
    run_id            NUMBER PRIMARY KEY,
    batch_id          VARCHAR2(50)   NOT NULL,
    pipeline_stage    VARCHAR2(50),
    status            VARCHAR2(20),
    source_file       VARCHAR2(500),
    records_read      NUMBER(10)     DEFAULT 0,
    records_valid     NUMBER(10)     DEFAULT 0,
    records_invalid   NUMBER(10)     DEFAULT 0,
    records_inserted  NUMBER(10)     DEFAULT 0,
    records_updated   NUMBER(10)     DEFAULT 0,
    records_skipped   NUMBER(10)     DEFAULT 0,
    start_time        TIMESTAMP      DEFAULT SYSTIMESTAMP,
    end_time          TIMESTAMP,
    error_message     VARCHAR2(4000),
    CONSTRAINT uq_run_batch_stage UNIQUE (batch_id, pipeline_stage)
);

-- ============================================================================
-- 6. RECONCILIATION RESULTS: Audit trail of every reconciliation check
-- ============================================================================
CREATE TABLE reconciliation_results (
    recon_id          NUMBER PRIMARY KEY,
    batch_id          VARCHAR2(50)   NOT NULL,
    check_type        VARCHAR2(50),
    source_value      NUMBER(18,2),
    target_value      NUMBER(18,2),
    difference        NUMBER(18,2),
    tolerance_pct     NUMBER(5,2),
    status            VARCHAR2(20),
    details           VARCHAR2(2000),
    check_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE INDEX idx_recon_batch ON reconciliation_results(batch_id);

-- ============================================================================
-- 7. CHECKPOINT TABLE: Resume-from-failure state tracking
-- ============================================================================
CREATE TABLE pipeline_checkpoints (
    checkpoint_id     NUMBER PRIMARY KEY,
    batch_id          VARCHAR2(50)   NOT NULL,
    stage_name        VARCHAR2(50)   NOT NULL,
    stage_status      VARCHAR2(20),
    records_processed NUMBER(10)     DEFAULT 0,
    checkpoint_data   VARCHAR2(4000),
    created_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT uq_chk_batch_stage UNIQUE (batch_id, stage_name)
);

-- ============================================================================
-- 8. ROLLBACK SNAPSHOTS: Pre-merge state for recovery
-- ============================================================================
CREATE TABLE rollback_snapshots (
    snapshot_id       NUMBER PRIMARY KEY,
    batch_id          VARCHAR2(50)   NOT NULL,
    transaction_id    VARCHAR2(50)   NOT NULL,
    transaction_date  DATE,
    account_id        VARCHAR2(50),
    transaction_type  VARCHAR2(20),
    amount            NUMBER(18,2),
    currency          VARCHAR2(10),
    merchant          VARCHAR2(200),
    description       VARCHAR2(500),
    channel           VARCHAR2(50),
    row_hash          VARCHAR2(64),
    version           NUMBER(10),
    snapshot_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE INDEX idx_snap_batch ON rollback_snapshots(batch_id);
CREATE INDEX idx_snap_txn ON rollback_snapshots(transaction_id);

-- ============================================================================
-- 9. DATA QUALITY SCORES: Per-batch quality metrics
-- ============================================================================
CREATE TABLE data_quality_scores (
    score_id          NUMBER PRIMARY KEY,
    batch_id          VARCHAR2(50)   NOT NULL,
    total_records     NUMBER(10),
    valid_records     NUMBER(10),
    invalid_records   NUMBER(10),
    duplicate_records NUMBER(10),
    quality_score_pct NUMBER(5,2),
    check_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- ============================================================================
-- RBAC CONSIDERATION (run as SYSTEM/DBA for production)
-- ============================================================================
-- CREATE ROLE incr_load_readonly;
-- GRANT SELECT ON incr_load.target_transactions TO incr_load_readonly;
-- GRANT SELECT ON incr_load.reconciliation_results TO incr_load_readonly;
-- GRANT SELECT ON incr_load.data_quality_scores TO incr_load_readonly;
--
-- CREATE ROLE incr_load_operator;
-- GRANT SELECT, INSERT, UPDATE ON incr_load.stg_transactions TO incr_load_operator;
-- GRANT EXECUTE ON incr_load.sp_merge_transactions TO incr_load_operator;
--
-- Data Masking: For PII/sensitive fields, consider Oracle VPD (Virtual Private Database)
-- or DBMS_RLS to enforce row/column-level security policies.
