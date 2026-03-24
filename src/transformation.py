"""
Transformation module: Deduplication and data enrichment.
Handles cross-file deduplication and data standardization.
"""

import logging

logger = logging.getLogger(__name__)


def deduplicate_records(records):
    """
    Deduplicate records within a batch. If the same transaction_id appears
    multiple times, keep the last occurrence (latest correction wins).
    Returns (deduplicated_list, duplicate_count).
    """
    seen = {}
    duplicates = 0

    for record in records:
        txn_id = record["transaction_id"]
        if txn_id in seen:
            duplicates += 1
            logger.info(
                "Dedup: Replacing earlier record for %s with later version", txn_id
            )
        seen[txn_id] = record

    deduped = list(seen.values())

    if duplicates > 0:
        logger.info(
            "Deduplication: %d duplicates removed, %d unique records remain",
            duplicates, len(deduped)
        )
    else:
        logger.info("Deduplication: No duplicates found in %d records", len(deduped))

    return deduped, duplicates


def standardize_records(records):
    """
    Standardize data formats for consistency:
    - Uppercase transaction_type
    - Trim whitespace
    - Ensure currency defaults to INR
    """
    for record in records:
        record["transaction_type"] = record.get("transaction_type", "").upper().strip()
        record["currency"] = record.get("currency", "INR").upper().strip() or "INR"
        record["channel"] = record.get("channel", "UNKNOWN").upper().strip() or "UNKNOWN"
        record["merchant"] = record.get("merchant", "").strip()
        record["description"] = record.get("description", "").strip()

    logger.info("Standardized %d records", len(records))
    return records


def merge_daily_and_corrections(daily_records, correction_records):
    """
    Merge daily transaction records with late-arriving corrections.
    Corrections override daily records for the same transaction_id.
    """
    merged = {r["transaction_id"]: r for r in daily_records}

    corrections_applied = 0
    for corr in correction_records:
        txn_id = corr["transaction_id"]
        if txn_id in merged:
            logger.info(
                "Correction applied: %s — old amount: %s, new amount: %s",
                txn_id,
                merged[txn_id].get("amount"),
                corr.get("amount")
            )
            corrections_applied += 1
        merged[txn_id] = corr

    result = list(merged.values())
    logger.info(
        "Merged %d daily + %d corrections = %d total (%d corrections applied to existing)",
        len(daily_records), len(correction_records), len(result), corrections_applied
    )
    return result
