# Large-Scale Incremental Data Load with Reconciliation

## Problem Understanding

A client receives **daily transaction updates** along with **late-arriving corrections** for past records. The current system suffers from:
- **Duplicate rows** due to reprocessing without change detection
- **Mismatched financial totals** between source and target systems
- **No audit trail** for corrections, making debugging impossible

This solution builds an **idempotent, production-grade data pipeline** that handles all these issues with checksum-based change detection, MERGE-based upserts, and 3-point reconciliation.

---

## Architecture Overview

```
┌──────────────┐    ┌──────────────────┐    ┌────────────────────┐    ┌──────────────────┐
│  SOURCE      │    │  STAGING LAYER   │    │  PROCESSING LAYER  │    │  TARGET / DW     │
│  (CSV Files) │───>│  (Raw Ingest)    │───>│  (Dedup + Merge)   │───>│  (Final Tables)  │
│  Daily Txns  │    │  - Hash columns  │    │  - MERGE/UPSERT    │    │  - Clean data    │
│  Corrections │    │  - Audit cols    │    │  - Reconciliation  │    │  - Audit trail   │
└──────────────┘    └──────────────────┘    └────────────────────┘    └──────────────────┘
                            │                        │                        │
                            v                        v                        v
                    ┌──────────────────────────────────────────────────────────────┐
                    │                   CONTROL / METADATA LAYER                   │
                    │  - Pipeline Run Log  - Reconciliation Reports                │
                    │  - Checkpoint State  - Mismatch Alerts                       │
                    │  - Rollback Snapshots - Data Quality Scores                  │
                    └──────────────────────────────────────────────────────────────┘
```

### Pipeline Flow (Stage by Stage)

```
    ┌─────────┐     ┌──────────┐     ┌───────────┐     ┌────────┐     ┌───────────┐
    │ INGEST  │────>│ VALIDATE │────>│ TRANSFORM │────>│  LOAD  │────>│ RECONCILE │
    │ Read CSV│     │ Quality  │     │ Dedup &   │     │ MERGE  │     │ Verify &  │
    │ Parse   │     │ Checks   │     │ Standardize│    │ Upsert │     │ Report    │
    └─────────┘     └──────────┘     └───────────┘     └────────┘     └───────────┘
         │               │                                                  │
         │          ┌────v─────┐                                     ┌─────v──────┐
         │          │QUARANTINE│                                     │ ALERTS &   │
         │          │Bad Data  │                                     │ REPORTS    │
         │          └──────────┘                                     └────────────┘
         │
    ┌────v──────────────────────────────────────────────────────────┐
    │              CHECKPOINT (resume-from-failure)                  │
    └───────────────────────────────────────────────────────────────┘
```

---

## End-to-End Approach

### 1. Ingestion (`src/ingestion.py`)
- Read CSV files with `csv.DictReader`
- Validate every record: null checks, data type validation, threshold checks, in-batch duplicate detection
- Invalid records → quarantine table with error codes and reasons
- Valid records → compute SHA-256 row hash for change detection

### 2. Transformation (`src/transformation.py`)
- Cross-file deduplication (last record wins)
- Merge daily transactions with late-arriving corrections
- Standardize data formats (uppercase types, default currency)

### 3. Storage / Load (`src/database.py`)
- Bulk insert to staging table
- **Pre-merge snapshot** of affected target rows (for rollback)
- Oracle **MERGE** with hash-based change detection:
  - **Hash matches** → Skip (no duplicate created)
  - **Same key, different hash** → UPDATE (correction applied)
  - **New key** → INSERT
- Full history logging in `transaction_history` table

### 4. Reconciliation (`src/reconciliation.py`)
- **Row count check**: Staging valid count vs target affected count
- **Total amount check**: Sum of amounts matches (with configurable tolerance)
- **Distinct key check**: No duplicates in target table
- **Orphan record check**: No valid staging records missing from target
- Results persisted to `reconciliation_results` table
- JSON reports generated in `reports/` folder

---

## Technology Choices & Justification

| Component | Choice | Justification |
|-----------|--------|---------------|
| Language | Python 3.x | Required by assessment; strong ecosystem for ETL |
| Database | Oracle XE | Enterprise-grade RDBMS with full MERGE support, partitioning, and PL/SQL |
| Python DB Driver | `oracledb` (thin mode) | Oracle's official driver; no Oracle Client install needed |
| Hashing | `hashlib` (SHA-256) | Built-in, cryptographically strong, deterministic |
| File I/O | `csv` module | Built-in, handles edge cases well, no extra dependencies |
| Logging | `logging` module | Built-in, production-grade, configurable levels |
| Config | JSON file | Human-readable, no extra dependencies |

**Why minimal dependencies?** Reduces deployment complexity, avoids version conflicts, and demonstrates fundamental understanding vs. reliance on frameworks.

---

## Key Design Decisions & Trade-offs

### 1. SHA-256 Hash vs. Column-by-Column Comparison
- **Chosen**: SHA-256 hash of all business columns
- **Why**: Single column comparison in MERGE instead of comparing every field; O(1) change detection
- **Trade-off**: Hash collisions theoretically possible but probability is negligible (1 in 2^256)

### 2. Staging Table Pattern vs. Direct Load
- **Chosen**: Stage → Validate → MERGE
- **Why**: Allows validation before touching target; enables rollback; supports reconciliation
- **Trade-off**: Additional storage for staging data; extra I/O step

### 3. Snapshot-Based Rollback vs. Transaction-Level
- **Chosen**: Pre-merge snapshots in `rollback_snapshots` table
- **Why**: Works across commits; can rollback hours later; auditable
- **Trade-off**: Storage overhead for snapshots; must clean up old snapshots periodically

### 4. Quarantine vs. Reject-and-Halt
- **Chosen**: Quarantine invalid records, continue with valid ones
- **Why**: One bad record shouldn't block 10,000 good ones; quarantine enables analysis
- **Trade-off**: Partial loads require awareness from downstream consumers

---

## How GenAI Was Used

| Area | How AI Helped | Where I Refined |
|------|---------------|-----------------|
| Initial boilerplate | Generated base table DDL, basic MERGE syntax | Added hash columns, audit fields, snapshot tables, proper indexing |
| Python structure | Suggested module layout | Redesigned for checkpoint/resume pattern, added rollback capability |
| Reconciliation | Basic count comparison | Added 4-point checks (count, amount, keys, orphans), tolerance thresholds, JSON reports |
| Error handling | Try/catch patterns | Added quarantine pattern, checkpoint-based resume, pipeline run logging |
| SQL queries | Basic SELECT/JOIN | Added MERGE with WHERE clause for hash comparison, SCD-style history tracking |

**Key refinements beyond AI suggestions:**
- AI did not suggest the pre-merge snapshot pattern for rollback — added based on production experience
- AI-generated MERGE lacked the `WHERE t.row_hash != s.row_hash` clause — critical for idempotency
- Checkpoint/resume logic was designed from scratch to handle partial failures
- Reconciliation tolerance thresholds and configurable fail behavior were manual additions

---

## Production Readiness Features

### Idempotency
- Row hash comparison ensures re-running the pipeline produces no duplicates
- Checkpoint tracking prevents re-processing completed stages

### Failure Handling
- **Checkpointing**: Each stage saves progress; failed runs resume from last checkpoint
- **Rollback**: Pre-merge snapshots enable reverting any batch
- **Quarantine**: Bad data isolated, not lost; error codes enable root cause analysis
- **Retry**: Configurable retry count and delay in `config.json`

### Security Considerations
- Dedicated Oracle schema with minimal privileges (no DBA access)
- Password externalized in `config.json` (in production: use vault/KMS)
- RBAC roles defined for read-only and operator access (in DDL comments)
- Data masking can be applied via Oracle VPD for sensitive columns

### Monitoring & Observability
- Structured logging with timestamps, levels, and module names
- `pipeline_run_log` table tracks every execution with metrics
- `data_quality_scores` table provides quality trend analysis
- Alert reports generated for reconciliation failures and data quality issues

### Scalability Considerations
- Batch processing with configurable batch sizes
- Oracle indexes on frequently queried columns
- Partitioning-ready table design (by `transaction_date`)
- Staging table cleanup after successful merge

---

## Database Schema (9 Tables)

| Table | Purpose |
|-------|---------|
| `stg_transactions` | Staging: raw data landing zone |
| `target_transactions` | Target: clean, deduplicated transaction data |
| `transaction_history` | Audit: SCD Type-2 style change log |
| `quarantine_records` | Quality: invalid/rejected records with error details |
| `pipeline_run_log` | Ops: execution metrics per pipeline stage |
| `reconciliation_results` | Trust: verification check results |
| `pipeline_checkpoints` | Recovery: resume-from-failure state |
| `rollback_snapshots` | Recovery: pre-merge state for rollback |
| `data_quality_scores` | Analytics: per-batch quality metrics |

---

## How to Run

### Prerequisites
- Python 3.8+
- Oracle XE with `incr_load` schema created
- `pip install oracledb`

### Quick Start
```bash
# Install dependency
pip install -r requirements.txt

# Run the full demo (3 scenarios)
python run_demo.py

# Or run individual files via CLI
python -m src.pipeline data/daily_transactions_day1.csv
python -m src.pipeline data/daily_transactions_day2.csv --corrections data/corrections_day2.csv

# Rollback a batch
python -m src.pipeline dummy --rollback BATCH_20260301_120000_abc12345
```

### Verify in Oracle SQL Developer
```sql
-- Check loaded data
SELECT * FROM target_transactions ORDER BY transaction_id;

-- Check corrections applied
SELECT * FROM transaction_history WHERE change_type = 'CORRECTION';

-- Check quarantined records
SELECT * FROM quarantine_records;

-- Check reconciliation results
SELECT * FROM reconciliation_results ORDER BY check_timestamp DESC;

-- Check pipeline health
SELECT * FROM pipeline_run_log ORDER BY start_time DESC;

-- Check data quality trends
SELECT * FROM data_quality_scores ORDER BY check_timestamp DESC;
```

---

## How This Solution Would Evolve in a Real Client Project

1. **Orchestration**: Replace manual runs with Apache Airflow DAGs or cloud-native schedulers (AWS Step Functions / Azure Data Factory)
2. **Storage**: Move from Oracle XE to Oracle Enterprise with RAC for HA, or cloud-native (Oracle Autonomous DB)
3. **Secrets Management**: Move credentials from `config.json` to HashiCorp Vault or cloud KMS
4. **Monitoring**: Integrate with Grafana/Prometheus for dashboards, PagerDuty for alerting
5. **Partitioning**: Range-partition `target_transactions` by `transaction_date` for fast queries
6. **Data Masking**: Implement Oracle VPD or column-level masking for PII/PCI compliance
7. **Parallel Processing**: Use Python multiprocessing or Spark for large-scale file processing
8. **CI/CD**: Automated testing pipeline with data validation tests before deployment

---

## Business Impact

| Issue | Without This Solution | With This Solution |
|-------|----------------------|-------------------|
| Duplicates | Financial reports inflated; incorrect totals | Zero duplicates via hash-based idempotent MERGE |
| Late corrections | Old errors persist; manual fixes | Automatic correction with full audit trail |
| Data trust | Business users don't trust reports | 4-point reconciliation with published reports |
| Recovery | Manual intervention; data loss risk | Automated rollback + checkpoint resume |
| Compliance | No audit trail | Complete history of every change with batch tracking |

---

## Project Structure

```
Assessment-Task/
├── README.md                           # This document
├── config.json                         # Pipeline configuration
├── requirements.txt                    # Python dependencies
├── run_demo.py                         # Demo runner (3 scenarios)
├── data/
│   ├── daily_transactions_day1.csv     # 20 normal transactions
│   ├── daily_transactions_day2.csv     # 15 new transactions
│   ├── corrections_day2.csv            # 5 corrections for Day 1
│   └── daily_transactions_day3_bad_data.csv  # Intentionally bad data
├── src/
│   ├── __init__.py
│   ├── pipeline.py                     # Main orchestrator
│   ├── ingestion.py                    # CSV reading & validation
│   ├── transformation.py              # Dedup & standardization
│   ├── database.py                     # Oracle connection & MERGE
│   ├── reconciliation.py              # 4-point verification
│   ├── rollback.py                     # Snapshot & restore
│   ├── checkpointing.py              # Resume-from-failure
│   ├── alerting.py                     # Mismatch alerts
│   └── utils.py                        # Logging & hashing helpers
├── sql/
│   ├── 01_ddl_tables.sql              # Table definitions
│   ├── 02_merge_procedure.sql         # PL/SQL MERGE procedure
│   └── 03_reconciliation_queries.sql  # Analytics queries
├── reports/                            # Generated reconciliation reports
├── logs/                               # Pipeline execution logs
└── tests/                              # Test cases
```
