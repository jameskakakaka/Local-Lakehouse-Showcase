"""Live tests require RUN_TRINO_ICEBERG_TESTS=1. No automatic cleanup.

Every test owns a UUID schema/table and S3 prefix; objects remain for inspection.
The production Dagster resource is not imported or materialized.
"""
from datetime import datetime
from hashlib import sha256
import os
import re
import unittest
from uuid import uuid4

import pandas as pd

from lakehouse_pipeline.assets.ingestion_support import (
    COLUMNS, TARGET, create_table_sql, merge_rows, validate_table_name,
)


def isolated_target():
    token = uuid4().hex
    schema = f"iceberg.fix4b_{token}"
    target = f"{schema}.yellow_trips_{token}"
    location = f"s3://warehouse/_integration_tests/fix4b/{token}/"
    return schema, target, location


def check_isolation(target, location):
    validate_table_name(target)
    match = re.fullmatch(r"iceberg\.fix4b_([0-9a-f]{32})\.yellow_trips_\1", target)
    if not match or target == TARGET:
        raise ValueError("Integration target must be a unique Fix 4B table")
    if location != f"s3://warehouse/_integration_tests/fix4b/{match[1]}/":
        raise ValueError("Integration storage prefix does not match the unique target")


def isolation_location(target):
    """Derive storage only from a validated isolated target."""
    validate_table_name(target)
    match = re.fullmatch(r"iceberg\.fix4b_([0-9a-f]{32})\.yellow_trips_\1", target)
    if not match:
        raise ValueError('Integration target must be a unique Fix 4B table')
    location = f's3://warehouse/_integration_tests/fix4b/{match[1]}/'
    check_isolation(target, location)
    return location


def schema_sql(target, location):
    """Construct the sole setup DDL after validating both identifiers and storage."""
    check_isolation(target, location)
    return f"CREATE SCHEMA {target.rsplit('.', 1)[0]} WITH (location = '{location}')"


def check_table_sql(sql, target):
    """Allow only harness templates, not arbitrary SQL over an allowed relation.

    This is deliberately not a SQL parser. Even harmless changes to a template
    require review here. Comment markers and delimiters are banned in literals
    too; quoted scalar data otherwise supports SQL's doubled-quote escaping.
    """
    location = isolation_location(target)
    if not isinstance(sql, str) or any(marker in sql for marker in (';', '--', '/*', '*/')):
        raise ValueError('SQL delimiters and comments are forbidden')
    names = ', '.join(name for name, _ in COLUMNS)
    allowed = {
        f'DESCRIBE {target}',
        f'SELECT {names} FROM {target} ORDER BY source_row_number',
        f'SELECT count(*) FROM {target}',
        f'SELECT DISTINCT source_file_sha256 FROM {target}',
        f"SELECT count(*) FROM {target} WHERE source_file_sha256 IS NULL "
        "OR NOT regexp_like(source_file_sha256, '^[0-9a-f]{64}$') "
        "OR source_row_number IS NULL OR source_row_number < 0",
        f'SELECT count(*) FROM (SELECT source_file_sha256, source_row_number '
        f'FROM {target} GROUP BY source_file_sha256, source_row_number '
        'HAVING count(*) > 1) AS duplicate_keys',
        create_table_sql(target, location + 'table/'),
    }
    if sql in allowed:
        return

    # Only generated CAST scalar tuples may appear in USING (VALUES ...).
    # No identifiers, functions, subqueries, or relations can enter this slot.
    scalar = r"(?:NULL|TRUE|FALSE|[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|(?:TIMESTAMP )?'(?:[^']|'')*')"
    row = r"\(" + ', '.join(
        r'CAST\(' + scalar + ' AS ' + re.escape(kind) + r'\)'
        for _, kind in COLUMNS
    ) + r"\)"
    prefix = f'MERGE INTO {target} AS target USING (VALUES '
    suffix = (
        f') AS source ({names}) '
        'ON target.source_file_sha256 = source.source_file_sha256 '
        'AND target.source_row_number = source.source_row_number '
        f'WHEN NOT MATCHED THEN INSERT ({names}) VALUES ('
        + ', '.join(f'source.{name}' for name, _ in COLUMNS) + ')'
    )
    if re.fullmatch(re.escape(prefix) + row + '(?:, ' + row + ')*' + re.escape(suffix), sql):
        return
    raise ValueError('Unsupported integration SQL template or storage location')


class IsolatedCursor:
    def __init__(self, cursor, target):
        isolation_location(target)
        self.cursor = cursor
        self.target = target
        self.merge_attempts = 0

    def execute(self, sql):
        check_table_sql(sql, self.target)
        if sql.startswith('MERGE INTO '):
            self.merge_attempts += 1
        return self.cursor.execute(sql)

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()


class InjectedFailure(RuntimeError):
    pass


class FailSecondBatch:
    """Inject failure before submission or after real fetchall confirms completion."""
    def __init__(self, cursor, after_commit):
        self.cursor = cursor
        self.after_commit = after_commit
        self.batches = 0
        self.fail_on_fetch = False

    def execute(self, sql):
        if sql.startswith('MERGE INTO '):
            self.batches += 1
            if self.batches == 2:
                if not self.after_commit:
                    raise InjectedFailure('injected before second batch')
                self.fail_on_fetch = True
        return self.cursor.execute(sql)

    def fetchall(self):
        result = self.cursor.fetchall()
        if self.fail_on_fetch:
            self.fail_on_fetch = False
            raise InjectedFailure('injected after second batch completed')
        return result

    def fetchone(self):
        return self.cursor.fetchone()


def synthetic_rows():
    timestamp = pd.Timestamp('2024-01-03 12:34:56.123456')
    duplicate = (1, timestamp, timestamp, 2.0, 3.0, None, "O'Brien", 10, 20, 1,
                 5.0, 0.0, 0.0, 1.0, 0.0, 0.0, 6.0, 0.0, 0.0)
    missing = list(duplicate)
    missing[1], missing[3], missing[5], missing[6] = pd.NaT, pd.NA, float('nan'), None
    return [duplicate, duplicate, tuple(missing), (9,) + duplicate[1:], (7,) + duplicate[1:]]


@unittest.skipUnless(os.environ.get('RUN_TRINO_ICEBERG_TESTS') == '1',
                     'Live writes disabled; set RUN_TRINO_ICEBERG_TESTS=1 explicitly')
class TrinoIcebergIntegrationTests(unittest.TestCase):
    def setUp(self):
        # Imports and connections occur only after the opt-in gate.
        from trino.dbapi import connect
        self.schema, self.target, self.location = isolated_target()
        setup_sql = schema_sql(self.target, self.location)
        self.connection = connect(
            host=os.environ.get('FIX4B_TRINO_HOST', 'localhost'),
            port=int(os.environ.get('FIX4B_TRINO_PORT', '8085')),
            user=os.environ.get('FIX4B_TRINO_USER', 'admin'),
            catalog='iceberg',
            request_timeout=30,
        )
        self.addCleanup(self.connection.close)
        raw = self.connection.cursor()
        self.addCleanup(raw.close)
        # Print identifiers before any DDL, so failed runs remain identifiable.
        print(f'\nRetained integration objects: {self.target} at {self.location}', flush=True)
        raw.execute('SELECT version()')
        print('Trino server version:', raw.fetchone()[0], flush=True)
        # Deliberately omit IF NOT EXISTS: collisions fail instead of reusing objects.
        raw.execute(setup_sql)
        raw.fetchall()
        self.cursor = IsolatedCursor(raw, self.target)
        self.cursor.execute(create_table_sql(self.target, self.location + 'table/'))
        self.cursor.fetchall()
        self.rows = synthetic_rows()
        self.digest = sha256(b'fix4b synthetic January 2024 delivery').hexdigest()
        self.assertEqual(self.read_rows(), [])

    def read_rows(self):
        names = ', '.join(name for name, _ in COLUMNS)
        self.cursor.execute(f'SELECT {names} FROM {self.target} ORDER BY source_row_number')
        return [tuple(row) for row in self.cursor.fetchall()]

    def assert_complete(self):
        self.cursor.execute(f'SELECT count(*) FROM {self.target}')
        self.assertEqual(self.cursor.fetchone()[0], len(self.rows))
        self.cursor.execute(
            f'SELECT count(*) FROM (SELECT source_file_sha256, source_row_number '
            f'FROM {self.target} GROUP BY source_file_sha256, source_row_number '
            'HAVING count(*) > 1) AS duplicate_keys'
        )
        self.assertEqual(self.cursor.fetchone()[0], 0)
        stored = self.read_rows()
        expected = []
        for position, row in enumerate(self.rows):
            values = tuple(None if pd.isna(value) else
                           value.to_pydatetime() if isinstance(value, pd.Timestamp) else value
                           for value in row)
            expected.append(values + (self.digest, position))
        self.assertEqual(stored, expected)
        self.assertEqual(stored[0][:-2], stored[1][:-2])
        self.assertNotEqual(stored[0][-1], stored[1][-1])
        self.assertIsInstance(stored[0][1], datetime)
        self.assertEqual(stored[0][1].microsecond, 123456)
        return stored

    def load(self, cursor=None):
        return merge_rows(cursor or self.cursor, self.rows, self.digest,
                          batch_size=2, target=self.target)

    def test_initial_load_identical_replay_duplicate_occurrences_nulls_and_timestamps(self):
        self.load()
        before = self.assert_complete()
        self.load()
        self.assertEqual(self.assert_complete(), before)

    def check_retry(self, after_commit):
        with self.assertRaises(InjectedFailure):
            self.load(FailSecondBatch(self.cursor, after_commit))
        self.assertEqual(len(self.read_rows()), 4 if after_commit else 2)
        # Hash rejection must also protect incomplete deliveries.
        attempts = self.cursor.merge_attempts
        before = self.read_rows()
        with self.assertRaisesRegex(ValueError, 'single January 2024 delivery'):
            merge_rows(self.cursor, self.rows, sha256(b'changed').hexdigest(),
                       batch_size=2, target=self.target)
        self.assertEqual(self.cursor.merge_attempts, attempts)
        self.assertEqual(self.read_rows(), before)
        self.load()
        self.assert_complete()

    def test_partial_failure_before_second_batch_and_retry(self):
        self.check_retry(after_commit=False)

    def test_completed_batch_with_lost_response_and_retry(self):
        self.check_retry(after_commit=True)

    def test_changed_file_rejected_without_modifying_completed_delivery(self):
        self.load()
        before = self.assert_complete()
        attempts = self.cursor.merge_attempts
        with self.assertRaisesRegex(ValueError, 'single January 2024 delivery'):
            merge_rows(self.cursor, self.rows, sha256(b'changed').hexdigest(),
                       target=self.target)
        self.assertEqual(self.cursor.merge_attempts, attempts)
        self.assertEqual(self.assert_complete(), before)
