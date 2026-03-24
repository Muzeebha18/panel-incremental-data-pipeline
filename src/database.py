"""
Database module: Oracle connection management, DDL execution, and MERGE operations.
Uses python-oracledb: thin mode by default; optional thick mode if oracle_client_lib_dir
is set in config or ORACLE_CLIENT_LIB_DIR points to a valid Instant Client directory.
"""

import logging
import os

import oracledb

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages Oracle database connections and operations."""

    _thick_mode_initialized = False

    def __init__(self, config):
        self.db_config = config["database"]
        self.connection = None

    def connect(self):
        """Establish connection to Oracle (thin mode, or thick if client lib configured)."""
        try:
            thick_dir = (
                self.db_config.get("oracle_client_lib_dir")
                or os.environ.get("ORACLE_CLIENT_LIB_DIR")
            )
            if thick_dir and os.path.isdir(thick_dir):
                if not DatabaseManager._thick_mode_initialized:
                    oracledb.init_oracle_client(lib_dir=thick_dir)
                    DatabaseManager._thick_mode_initialized = True
                    logger.info("Oracle thick mode initialized: %s", thick_dir)
            else:
                logger.info(
                    "Using python-oracledb thin mode "
                    "(set database.oracle_client_lib_dir or ORACLE_CLIENT_LIB_DIR for thick mode)"
                )

            self.connection = oracledb.connect(
                user=self.db_config["user"],
                password=self.db_config["password"],
                dsn=self.db_config["dsn"]
            )
            self.connection.autocommit = False
            logger.info(
                "Connected to Oracle database as %s (version: %s)",
                self.db_config["user"],
                self.connection.version
            )
        except oracledb.Error as e:
            logger.error("Database connection failed: %s", e)
            raise

    def disconnect(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed.")

    def execute_ddl(self):
        """Create all required tables if they don't already exist."""
        cursor = self.connection.cursor()

        tables = {
            "stg_transactions": """
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
                )
            """,
            "target_transactions": """
                CREATE TABLE target_transactions (
                    transaction_id    VARCHAR2(50) NOT NULL,
                    transaction_date  DATE NOT NULL,
                    account_id        VARCHAR2(50) NOT NULL,
                    transaction_type  VARCHAR2(20) NOT NULL,
                    amount            NUMBER(18,2) NOT NULL,
                    currency          VARCHAR2(10) DEFAULT 'INR',
                    merchant          VARCHAR2(200),
                    description       VARCHAR2(500),
                    channel           VARCHAR2(50),
                    row_hash          VARCHAR2(64) NOT NULL,
                    version           NUMBER(10) DEFAULT 1,
                    is_active         NUMBER(1) DEFAULT 1,
                    created_batch_id  VARCHAR2(50),
                    updated_batch_id  VARCHAR2(50),
                    created_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP,
                    updated_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP,
                    CONSTRAINT pk_target_txn PRIMARY KEY (transaction_id)
                )
            """,
            "transaction_history": """
                CREATE TABLE transaction_history (
                    history_id        NUMBER PRIMARY KEY,
                    transaction_id    VARCHAR2(50) NOT NULL,
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
                )
            """,
            "quarantine_records": """
                CREATE TABLE quarantine_records (
                    quarantine_id     NUMBER PRIMARY KEY,
                    transaction_id    VARCHAR2(50),
                    raw_data          VARCHAR2(4000),
                    error_code        VARCHAR2(50),
                    error_message     VARCHAR2(1000),
                    batch_id          VARCHAR2(50),
                    source_file       VARCHAR2(500),
                    quarantine_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
                )
            """,
            "pipeline_run_log": """
                CREATE TABLE pipeline_run_log (
                    run_id            NUMBER PRIMARY KEY,
                    batch_id          VARCHAR2(50) NOT NULL,
                    pipeline_stage    VARCHAR2(50),
                    status            VARCHAR2(20),
                    source_file       VARCHAR2(500),
                    records_read      NUMBER(10) DEFAULT 0,
                    records_valid     NUMBER(10) DEFAULT 0,
                    records_invalid   NUMBER(10) DEFAULT 0,
                    records_inserted  NUMBER(10) DEFAULT 0,
                    records_updated   NUMBER(10) DEFAULT 0,
                    records_skipped   NUMBER(10) DEFAULT 0,
                    start_time        TIMESTAMP DEFAULT SYSTIMESTAMP,
                    end_time          TIMESTAMP,
                    error_message     VARCHAR2(4000)
                )
            """,
            "reconciliation_results": """
                CREATE TABLE reconciliation_results (
                    recon_id          NUMBER PRIMARY KEY,
                    batch_id          VARCHAR2(50) NOT NULL,
                    check_type        VARCHAR2(50),
                    source_value      NUMBER(18,2),
                    target_value      NUMBER(18,2),
                    difference        NUMBER(18,2),
                    tolerance_pct     NUMBER(5,2),
                    status            VARCHAR2(20),
                    details           VARCHAR2(2000),
                    check_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP
                )
            """,
            "pipeline_checkpoints": """
                CREATE TABLE pipeline_checkpoints (
                    checkpoint_id     NUMBER PRIMARY KEY,
                    batch_id          VARCHAR2(50) NOT NULL,
                    stage_name        VARCHAR2(50) NOT NULL,
                    stage_status      VARCHAR2(20),
                    records_processed NUMBER(10) DEFAULT 0,
                    checkpoint_data   VARCHAR2(4000),
                    created_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
                )
            """,
            "rollback_snapshots": """
                CREATE TABLE rollback_snapshots (
                    snapshot_id       NUMBER PRIMARY KEY,
                    batch_id          VARCHAR2(50) NOT NULL,
                    transaction_id    VARCHAR2(50) NOT NULL,
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
                )
            """,
            "data_quality_scores": """
                CREATE TABLE data_quality_scores (
                    score_id          NUMBER PRIMARY KEY,
                    batch_id          VARCHAR2(50) NOT NULL,
                    total_records     NUMBER(10),
                    valid_records     NUMBER(10),
                    invalid_records   NUMBER(10),
                    duplicate_records NUMBER(10),
                    quality_score_pct NUMBER(5,2),
                    check_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP
                )
            """
        }

        for table_name, ddl in tables.items():
            if not self._table_exists(table_name):
                try:
                    cursor.execute(ddl)
                    logger.info("Created table: %s", table_name)
                except oracledb.Error as e:
                    logger.error("Failed to create table %s: %s", table_name, e)
                    raise
            else:
                logger.info("Table already exists: %s (skipped)", table_name)

        self._create_sequences(cursor)
        self._create_constraints(cursor)
        self._create_indexes(cursor)
        self.connection.commit()
        cursor.close()
        logger.info("DDL execution complete. All tables ready.")

    def _table_exists(self, table_name):
        """Check if a table exists in the current schema."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM user_tables WHERE table_name = :1",
            [table_name.upper()]
        )
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0

    def _create_constraints(self, cursor):
        """Create unique constraints needed for upsert logic."""
        constraints = [
            ("uq_run_batch_stage", "pipeline_run_log", "batch_id, pipeline_stage"),
            ("uq_chk_batch_stage", "pipeline_checkpoints", "batch_id, stage_name"),
        ]
        for name, table, columns in constraints:
            try:
                cursor.execute(
                    f"ALTER TABLE {table} ADD CONSTRAINT {name} UNIQUE ({columns})"
                )
            except oracledb.Error:
                pass  # Constraint already exists

    def _create_sequences(self, cursor):
        """Create sequences for auto-increment columns (Oracle 11g compatible)."""
        sequences = [
            "seq_history_id",
            "seq_quarantine_id",
            "seq_run_id",
            "seq_recon_id",
            "seq_checkpoint_id",
            "seq_snapshot_id",
            "seq_quality_id",
        ]
        for seq_name in sequences:
            try:
                cursor.execute(
                    f"CREATE SEQUENCE {seq_name} START WITH 1 INCREMENT BY 1 NOCACHE"
                )
                logger.info("Created sequence: %s", seq_name)
            except oracledb.Error:
                pass  # Sequence already exists

    def _create_indexes(self, cursor):
        """Create performance indexes (silently skip if they exist)."""
        indexes = [
            ("idx_stg_txn_id", "stg_transactions", "transaction_id"),
            ("idx_stg_batch", "stg_transactions", "batch_id"),
            ("idx_target_hash", "target_transactions", "row_hash"),
            ("idx_target_acct", "target_transactions", "account_id"),
            ("idx_target_date", "target_transactions", "transaction_date"),
            ("idx_target_active", "target_transactions", "is_active"),
            ("idx_hist_txn_id", "transaction_history", "transaction_id"),
            ("idx_hist_batch", "transaction_history", "batch_id"),
            ("idx_quar_batch", "quarantine_records", "batch_id"),
            ("idx_recon_batch", "reconciliation_results", "batch_id"),
            ("idx_snap_batch", "rollback_snapshots", "batch_id"),
            ("idx_snap_txn", "rollback_snapshots", "transaction_id"),
        ]
        for idx_name, table_name, column in indexes:
            try:
                cursor.execute(
                    f"CREATE INDEX {idx_name} ON {table_name}({column})"
                )
            except oracledb.Error:
                pass  # Index already exists

    def clear_staging(self, batch_id):
        """Remove staging data for a specific batch (cleanup after merge)."""
        cursor = self.connection.cursor()
        cursor.execute(
            "DELETE FROM stg_transactions WHERE batch_id = :1", [batch_id]
        )
        self.connection.commit()
        cursor.close()
        logger.info("Cleared staging data for batch %s", batch_id)

    def insert_staging_batch(self, records, batch_id, source_file):
        """Bulk insert validated records into staging table."""
        cursor = self.connection.cursor()

        insert_sql = """
            INSERT INTO stg_transactions (
                transaction_id, transaction_date, account_id, transaction_type,
                amount, currency, merchant, description, channel,
                row_hash, batch_id, source_file, record_status
            ) VALUES (
                :1, TO_DATE(:2, 'YYYY-MM-DD'), :3, :4,
                :5, :6, :7, :8, :9,
                :10, :11, :12, :13
            )
        """

        batch_data = []
        for rec in records:
            batch_data.append((
                rec["transaction_id"],
                rec["transaction_date"],
                rec["account_id"],
                rec["transaction_type"],
                float(rec["amount"]),
                rec["currency"],
                rec["merchant"],
                rec["description"],
                rec["channel"],
                rec["row_hash"],
                batch_id,
                source_file,
                rec.get("record_status", "VALID")
            ))

        cursor.executemany(insert_sql, batch_data)
        self.connection.commit()
        cursor.close()
        logger.info(
            "Inserted %d records into staging (batch: %s)", len(batch_data), batch_id
        )
        return len(batch_data)

    def insert_quarantine(self, bad_records, batch_id, source_file):
        """Insert invalid records into quarantine with error details."""
        if not bad_records:
            return 0

        cursor = self.connection.cursor()
        insert_sql = """
            INSERT INTO quarantine_records (
                quarantine_id, transaction_id, raw_data, error_code, error_message,
                batch_id, source_file
            ) VALUES (seq_quarantine_id.NEXTVAL, :1, :2, :3, :4, :5, :6)
        """

        batch_data = []
        for rec in bad_records:
            batch_data.append((
                rec.get("transaction_id", "UNKNOWN"),
                str(rec.get("raw_data", ""))[:4000],
                rec.get("error_code", "UNKNOWN"),
                rec.get("error_message", "Unknown validation error"),
                batch_id,
                source_file
            ))

        cursor.executemany(insert_sql, batch_data)
        self.connection.commit()
        cursor.close()
        logger.info(
            "Quarantined %d invalid records (batch: %s)", len(batch_data), batch_id
        )
        return len(batch_data)

    def execute_merge(self, batch_id):
        """
        Execute the MERGE operation: upsert staging -> target with hash comparison.
        Returns (inserted_count, updated_count, skipped_count).
        """
        cursor = self.connection.cursor()

        # Step 1: Snapshot existing rows that will be affected
        cursor.execute("""
            INSERT INTO rollback_snapshots (
                snapshot_id, batch_id, transaction_id, transaction_date, account_id,
                transaction_type, amount, currency, merchant, description,
                channel, row_hash, version
            )
            SELECT seq_snapshot_id.NEXTVAL, :batch_id, t.transaction_id, t.transaction_date, t.account_id,
                   t.transaction_type, t.amount, t.currency, t.merchant, t.description,
                   t.channel, t.row_hash, t.version
            FROM target_transactions t
            WHERE t.transaction_id IN (
                SELECT s.transaction_id FROM stg_transactions s
                WHERE s.batch_id = :batch_id AND s.record_status = 'VALID'
            )
        """, {"batch_id": batch_id})
        snapshot_count = cursor.rowcount
        logger.info("Rollback snapshot: %d existing rows saved", snapshot_count)

        # Step 2: MERGE with hash-based change detection
        cursor.execute("""
            MERGE INTO target_transactions t
            USING (
                SELECT transaction_id, transaction_date, account_id, transaction_type,
                       amount, currency, merchant, description, channel, row_hash
                FROM stg_transactions
                WHERE batch_id = :batch_id AND record_status = 'VALID'
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
                    t.updated_batch_id  = :batch_id,
                    t.updated_timestamp = SYSTIMESTAMP
                WHERE t.row_hash != s.row_hash
            WHEN NOT MATCHED THEN
                INSERT (transaction_id, transaction_date, account_id, transaction_type,
                        amount, currency, merchant, description, channel, row_hash,
                        version, is_active, created_batch_id, updated_batch_id)
                VALUES (s.transaction_id, s.transaction_date, s.account_id,
                        s.transaction_type, s.amount, s.currency, s.merchant,
                        s.description, s.channel, s.row_hash,
                        1, 1, :batch_id, :batch_id)
        """, {"batch_id": batch_id})

        merge_affected = cursor.rowcount

        # Step 3: Log new inserts into history
        cursor.execute("""
            INSERT INTO transaction_history (
                history_id, transaction_id, transaction_date, account_id, transaction_type,
                amount, currency, merchant, description, channel, row_hash,
                version, change_type, batch_id
            )
            SELECT seq_history_id.NEXTVAL, s.transaction_id, s.transaction_date, s.account_id,
                   s.transaction_type, s.amount, s.currency, s.merchant,
                   s.description, s.channel, s.row_hash,
                   1, 'INSERT', :batch_id
            FROM stg_transactions s
            WHERE s.batch_id = :batch_id AND s.record_status = 'VALID'
              AND NOT EXISTS (
                  SELECT 1 FROM rollback_snapshots r
                  WHERE r.transaction_id = s.transaction_id AND r.batch_id = :batch_id
              )
        """, {"batch_id": batch_id})
        inserted_count = cursor.rowcount

        # Step 4: Log corrections/updates into history
        cursor.execute("""
            INSERT INTO transaction_history (
                history_id, transaction_id, transaction_date, account_id, transaction_type,
                amount, currency, merchant, description, channel, row_hash,
                version, change_type, batch_id
            )
            SELECT seq_history_id.NEXTVAL, t.transaction_id, t.transaction_date, t.account_id,
                   t.transaction_type, t.amount, t.currency, t.merchant,
                   t.description, t.channel, t.row_hash,
                   t.version, 'CORRECTION', :batch_id
            FROM target_transactions t
            WHERE t.updated_batch_id = :batch_id
              AND t.version > 1
              AND EXISTS (
                  SELECT 1 FROM rollback_snapshots r
                  WHERE r.transaction_id = t.transaction_id AND r.batch_id = :batch_id
                    AND r.row_hash != t.row_hash
              )
        """, {"batch_id": batch_id})
        updated_count = cursor.rowcount

        # Calculate skipped (hash matched — no actual change needed)
        cursor.execute("""
            SELECT COUNT(*) FROM stg_transactions s
            WHERE s.batch_id = :batch_id AND s.record_status = 'VALID'
              AND EXISTS (
                  SELECT 1 FROM rollback_snapshots r
                  WHERE r.transaction_id = s.transaction_id
                    AND r.batch_id = :batch_id
                    AND r.row_hash = s.row_hash
              )
        """, {"batch_id": batch_id})
        skipped_count = cursor.fetchone()[0]

        self.connection.commit()
        cursor.close()

        logger.info(
            "MERGE complete — Inserted: %d | Updated: %d | Skipped (unchanged): %d",
            inserted_count, updated_count, skipped_count
        )
        return inserted_count, updated_count, skipped_count

    def log_pipeline_run(self, batch_id, stage, status, source_file="",
                         records_read=0, records_valid=0, records_invalid=0,
                         records_inserted=0, records_updated=0, records_skipped=0,
                         error_message=None):
        """Insert or update a pipeline run log entry."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO pipeline_run_log (
                    run_id, batch_id, pipeline_stage, status, source_file,
                    records_read, records_valid, records_invalid,
                    records_inserted, records_updated, records_skipped,
                    error_message
                ) VALUES (
                    seq_run_id.NEXTVAL, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11
                )
            """, [
                batch_id, stage, status, source_file,
                records_read, records_valid, records_invalid,
                records_inserted, records_updated, records_skipped,
                error_message
            ])
        except oracledb.IntegrityError:
            cursor.execute("""
                UPDATE pipeline_run_log
                SET status = :1, records_read = :2, records_valid = :3,
                    records_invalid = :4, records_inserted = :5,
                    records_updated = :6, records_skipped = :7,
                    end_time = SYSTIMESTAMP, error_message = :8
                WHERE batch_id = :9 AND pipeline_stage = :10
            """, [
                status, records_read, records_valid, records_invalid,
                records_inserted, records_updated, records_skipped,
                error_message, batch_id, stage
            ])
        self.connection.commit()
        cursor.close()

    def get_cursor(self):
        """Return a raw cursor for custom queries."""
        return self.connection.cursor()

    def commit(self):
        """Commit the current transaction."""
        self.connection.commit()
