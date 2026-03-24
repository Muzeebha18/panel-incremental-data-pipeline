"""
Ingestion module: Read CSV files, validate records, compute row hashes.
Separates valid records from invalid ones (quarantine pattern).
"""

import csv
import logging
import os

from src.utils import compute_row_hash, HASH_COLUMNS

logger = logging.getLogger(__name__)

VALID_TRANSACTION_TYPES = {"DEBIT", "CREDIT"}
MAX_AMOUNT_THRESHOLD = 10_000_000.00  # Flag amounts above 1 crore


def read_csv_file(file_path):
    """Read a CSV file and return list of row dictionaries."""
    if not os.path.exists(file_path):
        logger.error("File not found: %s", file_path)
        raise FileNotFoundError(f"Source file not found: {file_path}")

    records = []
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stripped = {k.strip(): v.strip() for k, v in row.items()}
            records.append(stripped)

    logger.info("Read %d records from %s", len(records), file_path)
    return records


def validate_record(record, seen_ids_in_batch):
    """
    Validate a single record. Returns (is_valid, error_code, error_message).
    
    Checks performed:
    1. Transaction ID must not be empty
    2. Account ID must not be empty
    3. Amount must be numeric and positive
    4. Transaction type must be DEBIT or CREDIT
    5. Amount must not exceed safety threshold
    6. No duplicate transaction IDs within the same batch
    """
    txn_id = record.get("transaction_id", "")
    account_id = record.get("account_id", "")
    amount_str = record.get("amount", "")
    txn_type = record.get("transaction_type", "")

    if not txn_id:
        return False, "NULL_TXN_ID", "Transaction ID is missing or empty"

    if not account_id:
        return False, "NULL_ACCT_ID", "Account ID is missing or empty"

    try:
        amount = float(amount_str)
    except (ValueError, TypeError):
        return False, "INVALID_AMOUNT", f"Amount is not numeric: '{amount_str}'"

    if amount < 0:
        return False, "NEGATIVE_AMOUNT", f"Amount is negative: {amount}"

    if txn_type not in VALID_TRANSACTION_TYPES:
        return False, "INVALID_TXN_TYPE", f"Invalid transaction type: '{txn_type}'"

    if amount > MAX_AMOUNT_THRESHOLD:
        return False, "AMOUNT_THRESHOLD", f"Amount {amount} exceeds safety threshold {MAX_AMOUNT_THRESHOLD}"

    if txn_id in seen_ids_in_batch:
        return False, "DUPLICATE_IN_BATCH", f"Duplicate transaction ID in same file: {txn_id}"

    return True, None, None


def process_file(file_path):
    """
    Full ingestion pipeline for one file:
    1. Read CSV
    2. Validate each record
    3. Compute row hashes for valid records
    4. Return (valid_records, invalid_records)
    """
    raw_records = read_csv_file(file_path)

    valid_records = []
    invalid_records = []
    seen_ids = set()

    for record in raw_records:
        is_valid, error_code, error_message = validate_record(record, seen_ids)

        if is_valid:
            record["row_hash"] = compute_row_hash(record, HASH_COLUMNS)
            record["record_status"] = "VALID"
            valid_records.append(record)
            seen_ids.add(record["transaction_id"])
        else:
            invalid_records.append({
                "transaction_id": record.get("transaction_id", "UNKNOWN"),
                "raw_data": str(record),
                "error_code": error_code,
                "error_message": error_message
            })
            logger.warning(
                "Validation failed for record [%s]: %s - %s",
                record.get("transaction_id", "UNKNOWN"),
                error_code,
                error_message
            )

    logger.info(
        "File %s processed — Valid: %d | Invalid: %d | Total: %d",
        os.path.basename(file_path),
        len(valid_records),
        len(invalid_records),
        len(raw_records)
    )

    return valid_records, invalid_records
