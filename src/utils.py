"""
Utility module: Logging setup, hashing helpers, and common functions.
"""

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime


def setup_logging(log_dir="logs", log_level=logging.INFO):
    """Configure structured logging with both file and console output."""
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(
        log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logging.info("Logging initialized. Log file: %s", log_filename)
    return log_filename


def load_config(config_path="config.json"):
    """Load pipeline configuration from JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    logging.info("Configuration loaded from %s", config_path)
    return config


def generate_batch_id():
    """Generate a unique batch identifier for this pipeline run."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    batch_id = f"BATCH_{timestamp}_{short_uuid}"
    logging.info("Generated batch_id: %s", batch_id)
    return batch_id


def compute_row_hash(row_dict, columns):
    """
    Compute a SHA-256 hash of specified columns in a row.
    This is the core of our change detection — if the hash differs,
    the data has changed and needs to be updated.
    """
    hash_input = "|".join(str(row_dict.get(col, "")).strip() for col in columns)
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()


HASH_COLUMNS = [
    "transaction_id", "transaction_date", "account_id",
    "transaction_type", "amount", "currency",
    "merchant", "description", "channel"
]


def format_duration(start_time, end_time):
    """Return a human-readable duration string."""
    delta = end_time - start_time
    total_seconds = int(delta.total_seconds())
    minutes, seconds = divmod(total_seconds, 60)
    if minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"
