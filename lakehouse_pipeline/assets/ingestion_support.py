"""SQL construction and preflight checks; no connections are opened here.

Writers must be serialized externally: Iceberg does not enforce unique keys.
Only one immutable January 2024 Yellow Taxi delivery is supported. This is
not a multi-month or source-revision ingestion policy.
"""
from datetime import date, datetime
from hashlib import sha256
from math import isfinite
from numbers import Integral, Real
import re

import numpy as np
import pandas as pd

TARGET = "iceberg.raw.yellow_trips"
BUSINESS_COLUMNS = (
    ("vendorid", "integer"),
    ("tpep_pickup_datetime", "timestamp(6)"),
    ("tpep_dropoff_datetime", "timestamp(6)"),
    ("passenger_count", "double"),
    ("trip_distance", "double"),
    ("ratecodeid", "double"),
    ("store_and_fwd_flag", "varchar"),
    ("pulocationid", "integer"),
    ("dolocationid", "integer"),
    ("payment_type", "integer"),
    ("fare_amount", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tip_amount", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("total_amount", "double"),
    ("congestion_surcharge", "double"),
    ("airport_fee", "double"),
)
COLUMNS = BUSINESS_COLUMNS + (
    ("source_file_sha256", "varchar"),
    ("source_row_number", "bigint"),
)
def validate_table_name(target):
    """Require exactly catalog.schema.table, using conservative SQL identifiers."""
    if not isinstance(target, str) or not re.fullmatch(
        r"[a-z][a-z0-9_]{0,127}\.[a-z][a-z0-9_]{0,127}\.[a-z][a-z0-9_]{0,127}", target
    ):
        raise ValueError("Target must be an unquoted lowercase catalog.schema.table")
    return target


def create_table_sql(target=TARGET, location=None):
    target = validate_table_name(target)
    properties = "format = 'PARQUET', format_version = 2"
    if location is not None:
        if not isinstance(location, str) or not re.fullmatch(
            r"s3://[a-z0-9][a-z0-9.-]*/[a-zA-Z0-9_/-]+", location
        ):
            raise ValueError("Location must be a simple absolute S3 prefix")
        properties += f", location = '{location}'"
    return (
        f"CREATE TABLE IF NOT EXISTS {target} (\n"
        + ",\n".join(f"    {name} {kind}" for name, kind in COLUMNS)
        + f"\n) WITH ({properties})"
    )


CREATE_TABLE_SQL = create_table_sql()


def file_sha256(path):
    digest = sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_target(cursor, target=TARGET):
    """Fail closed, including for missing/inaccessible targets; never migrate."""
    target = validate_table_name(target)
    cursor.execute(f"DESCRIBE {target}")
    actual = {row[0].lower(): row[1].lower() for row in cursor.fetchall()}
    if actual != dict(COLUMNS):
        raise ValueError(
            "Incompatible yellow_trips schema; explicit review required. "
            "No automatic migration is performed (even for empty legacy tables)."
        )
    cursor.execute(
        f"SELECT count(*) FROM {target} WHERE source_file_sha256 IS NULL "
        "OR NOT regexp_like(source_file_sha256, '^[0-9a-f]{64}$') "
        "OR source_row_number IS NULL OR source_row_number < 0"
    )
    if cursor.fetchone()[0]:
        raise ValueError("Legacy or invalid provenance rows; reconcile before ingestion.")
    cursor.execute(
        f"SELECT count(*) FROM (SELECT source_file_sha256, source_row_number "
        f"FROM {target} GROUP BY source_file_sha256, source_row_number "
        "HAVING count(*) > 1) AS duplicate_keys"
    )
    if cursor.fetchone()[0]:
        raise ValueError("Duplicate provenance keys; reconcile before ingestion.")


def _format_value(value):
    if not pd.api.types.is_scalar(value):
        raise TypeError("Only scalar SQL values are supported")
    if pd.isna(value):
        return "NULL"
    if isinstance(value, np.datetime64):
        value = pd.Timestamp(value)
    elif isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (datetime, date)):
        return f"TIMESTAMP '{value}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, Integral):
        return str(value)
    if isinstance(value, Real):
        if not isfinite(value):
            raise ValueError("Non-finite numeric values are not supported")
        return str(value)
    raise TypeError(f"Unsupported SQL scalar type: {type(value).__name__}")


def _format_column_value(value, name, kind):
    formatted = _format_value(value)
    if kind == "integer" and formatted != "NULL":
        # Trino INTEGER is signed 32-bit. Do not let a later CAST fail after
        # earlier batches commit, or silently round fractional identifiers.
        if (isinstance(value, (bool, np.bool_)) or not isinstance(value, Real)
                or not -(2**31) <= value < 2**31 or int(value) != value):
            raise ValueError(f"Invalid signed 32-bit integer in column {name}")
    return formatted


def merge_rows(cursor, rows, source_hash, batch_size=500, *, target=TARGET):
    """Replay ordered source occurrences; return attempted, not inserted, count.

    `rows` must start at absolute file row zero and retain source order.
    Only one immutable January 2024 Yellow Taxi delivery is supported.
    Empty targets accept the first hash; existing targets accept only its replay.
    Changed files and multi-month/version loading require a separate policy.
    """
    target = validate_table_name(target)
    if (not isinstance(source_hash, str) or len(source_hash) != 64
            or any(c not in "0123456789abcdef" for c in source_hash)):
        raise ValueError("Invalid SHA-256")
    if isinstance(batch_size, bool) or not isinstance(batch_size, Integral) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    # Validate all input before committing any batch.
    rows = list(rows)
    if any(len(row) != len(BUSINESS_COLUMNS) for row in rows):
        raise ValueError("Unexpected input column count")
    serialized_rows = [tuple(
        _format_column_value(value, name, kind)
        for value, (name, kind) in zip(row, BUSINESS_COLUMNS)
    ) for row in rows]
    validate_target(cursor, target=target)
    cursor.execute(f"SELECT DISTINCT source_file_sha256 FROM {target}")
    existing_hashes = {row[0] for row in cursor.fetchall()}
    if existing_hashes and existing_hashes != {source_hash}:
        raise ValueError(
            "Source hash differs from the existing single January 2024 delivery; "
            "revisions and multi-month loading are not supported. No rows written."
        )
    names = ", ".join(name for name, _ in COLUMNS)
    for start in range(0, len(rows), batch_size):
        values = []
        for position in range(start, min(start + batch_size, len(rows))):
            row = serialized_rows[position] + (_format_value(source_hash), str(position))
            values.append("(" + ", ".join(
                f"CAST({value} AS {kind})"
                for value, (_, kind) in zip(row, COLUMNS)
            ) + ")")
        cursor.execute(
            f"MERGE INTO {target} AS target USING (VALUES "
            + ", ".join(values)
            + f") AS source ({names}) "
            "ON target.source_file_sha256 = source.source_file_sha256 "
            "AND target.source_row_number = source.source_row_number "
            f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ("
            + ", ".join(f"source.{name}" for name, _ in COLUMNS) + ")"
        )
        cursor.fetchall()
    return len(rows)
