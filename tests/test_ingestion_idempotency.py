"""Offline contract tests: the fake models serialized atomic batch commits.

These do not establish live Trino MERGE syntax or concurrency guarantees.
"""
import re
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from lakehouse_pipeline.assets.ingestion_support import (
    BUSINESS_COLUMNS, COLUMNS, CREATE_TABLE_SQL, _format_value, file_sha256, merge_rows,
)

HASH = 'a' * 64
ROW = (1, None, None, 2.0, 3.0, None, "N", 10, 20, 1,
       5.0, 0.0, 0.0, 1.0, 0.0, 0.0, 6.0, 0.0, 0.0)


class FakeCursor:
    def __init__(self):
        self.stored = {}
        self.schema = list(COLUMNS)
        self.invalid_rows = 0
        self.duplicate_keys = 0
        self.fail_batch = None
        self.fail_after_commit = False
        self.batch_number = 0
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)
        if sql.startswith('DESCRIBE'):
            self.result = [(name, kind, '', '') for name, kind in self.schema]
        elif 'AS duplicate_keys' in sql:
            self.result = [(self.duplicate_keys,)]
        elif sql.startswith('SELECT DISTINCT source_file_sha256'):
            self.result = [(digest,) for digest in sorted({key[0] for key in self.stored})]
        elif sql.startswith('SELECT'):
            self.result = [(self.invalid_rows,)]
        elif sql.startswith('MERGE'):
            self.batch_number += 1
            should_fail = self.batch_number == self.fail_batch
            if should_fail and not self.fail_after_commit:
                raise RuntimeError('injected pre-commit failure')
            self.assert_insert_only(sql)
            # Extract complete typed rows, keeping all payload values for comparisons.
            rows = re.findall(
                r"\((CAST\(.*?CAST\('([0-9a-f]{64})' AS varchar\), "
                r"CAST\((\d+) AS bigint\))\)", sql,
            )
            if not rows:
                raise AssertionError('No source rows recognized')
            for payload, digest, position in rows:
                self.stored.setdefault((digest, int(position)), payload)
            self.result = []
            if should_fail:
                raise RuntimeError('injected lost commit response')
        else:
            raise AssertionError('Unexpected statement: ' + sql)

    @staticmethod
    def assert_insert_only(sql):
        assert 'WHEN NOT MATCHED THEN INSERT' in sql
        assert 'WHEN MATCHED' not in sql
        assert 'DELETE' not in sql and 'UPDATE' not in sql
        assert 'ON target.source_file_sha256 = source.source_file_sha256' in sql
        assert 'AND target.source_row_number = source.source_row_number' in sql

    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0]


class IdempotencyTests(unittest.TestCase):
    def test_prototype_size_batch_boundaries_and_completed_load_retry(self):
        cursor = FakeCursor()
        merge_rows(cursor, [ROW] * 10_000, HASH, 500)
        self.assertEqual(cursor.batch_number, 20)
        self.assertEqual(len(cursor.stored), 10_000)
        before = dict(cursor.stored)
        # Also covers a retry after a failure in post-load metadata reporting.
        merge_rows(cursor, [ROW] * 10_000, HASH, 500)
        self.assertEqual(cursor.stored, before)
        self.assertEqual({key[1] for key in cursor.stored}, set(range(10_000)))

    def test_first_load_and_identical_replay(self):
        cursor = FakeCursor()
        self.assertEqual(merge_rows(cursor, [ROW] * 5, HASH, 2), 5)
        before = dict(cursor.stored)
        merge_rows(cursor, [ROW] * 5, HASH, 3)
        self.assertEqual(cursor.stored, before)
        self.assertEqual({key[1] for key in cursor.stored}, set(range(5)))

    def test_partial_retry_before_and_after_commit(self):
        for after_commit in (False, True):
            with self.subTest(after_commit=after_commit):
                cursor = FakeCursor()
                cursor.fail_batch = 2
                cursor.fail_after_commit = after_commit
                with self.assertRaises(RuntimeError):
                    merge_rows(cursor, [ROW] * 5, HASH, 2)
                self.assertEqual(len(cursor.stored), 4 if after_commit else 2)
                cursor.fail_batch = None
                merge_rows(cursor, [ROW] * 5, HASH, 2)
                self.assertEqual(len(cursor.stored), 5)

    def test_identical_payload_occurrences_preserved(self):
        cursor = FakeCursor()
        merge_rows(cursor, [ROW, ROW], HASH, 1)
        self.assertEqual(set(cursor.stored), {(HASH, 0), (HASH, 1)})

    def test_changed_file_rejected_without_merging_or_changing_existing_rows(self):
        cursor = FakeCursor()
        merge_rows(cursor, [ROW], HASH)
        old = dict(cursor.stored)
        changed = (2,) + ROW[1:]
        batches = cursor.batch_number
        with self.assertRaisesRegex(ValueError, 'single January 2024 delivery'):
            merge_rows(cursor, [changed], 'b' * 64)
        self.assertEqual(cursor.batch_number, batches)
        self.assertEqual(cursor.stored, old)

    def test_changed_file_rejected_after_partial_load_original_can_resume(self):
        cursor = FakeCursor()
        cursor.fail_batch = 2
        with self.assertRaises(RuntimeError):
            merge_rows(cursor, [ROW] * 5, HASH, 2)
        before = dict(cursor.stored)
        batches = cursor.batch_number
        with self.assertRaisesRegex(ValueError, 'single January 2024 delivery'):
            merge_rows(cursor, [ROW] * 5, 'b' * 64, 2)
        self.assertEqual(cursor.batch_number, batches)
        self.assertEqual(cursor.stored, before)
        cursor.fail_batch = None
        merge_rows(cursor, [ROW] * 5, HASH, 2)
        self.assertEqual(len(cursor.stored), 5)

    def test_preexisting_multiple_hashes_rejected_even_if_one_matches(self):
        cursor = FakeCursor()
        cursor.stored = {(HASH, 0): 'original', ('b' * 64, 0): 'other'}
        before = dict(cursor.stored)
        with self.assertRaisesRegex(ValueError, 'single January 2024 delivery'):
            merge_rows(cursor, [ROW], HASH)
        self.assertEqual(cursor.batch_number, 0)
        self.assertEqual(cursor.stored, before)

    def test_input_order_and_payload_preserved(self):
        cursor = FakeCursor()
        rows = [(n,) + ROW[1:] for n in (7, 3, 9)]
        merge_rows(cursor, rows, HASH, 2)
        for position, vendor in enumerate((7, 3, 9)):
            self.assertTrue(cursor.stored[(HASH, position)].startswith(f'CAST({vendor} AS integer)'))

    def test_legacy_or_incompatible_schema_fails_before_merge(self):
        for schema in (list(BUSINESS_COLUMNS), [('vendorid', 'bigint')] + list(COLUMNS[1:])):
            cursor = FakeCursor()
            cursor.schema = schema
            with self.assertRaisesRegex(ValueError, 'Incompatible'):
                merge_rows(cursor, [ROW], HASH)
            self.assertEqual(cursor.batch_number, 0)

    def test_untracked_rows_and_duplicate_keys_fail_before_merge(self):
        for attribute in ('invalid_rows', 'duplicate_keys'):
            cursor = FakeCursor()
            setattr(cursor, attribute, 1)
            with self.assertRaises(ValueError):
                merge_rows(cursor, [ROW], HASH)
            self.assertEqual(cursor.batch_number, 0)

    def test_missing_target_propagates_without_initializing(self):
        class MissingCursor(FakeCursor):
            def execute(self, sql):
                raise RuntimeError('table not found')
        cursor = MissingCursor()
        with self.assertRaisesRegex(RuntimeError, 'table not found'):
            merge_rows(cursor, [ROW], HASH)
        self.assertEqual(cursor.batch_number, 0)

    def test_bad_input_fails_before_any_sql(self):
        for rows, digest, size in (([ROW[:-1]], HASH, 2), ([ROW], 'bad', 2), ([ROW], HASH, 0)):
            cursor = FakeCursor()
            with self.assertRaises(ValueError):
                merge_rows(cursor, rows, digest, size)
            self.assertEqual(cursor.statements, [])

    def test_invalid_hash_types_and_batch_sizes_fail_before_sql(self):
        for digest, size in ((None, 2), (123, 2), (HASH, 1.5), (HASH, True)):
            cursor = FakeCursor()
            with self.assertRaises(ValueError):
                merge_rows(cursor, [ROW], digest, size)
            self.assertEqual(cursor.statements, [])

    def test_invalid_integer_in_later_batch_fails_before_any_sql(self):
        for value in (2**31, -(2**31)-1, 1.5, True, '1'):
            cursor = FakeCursor()
            bad_row = (value,) + ROW[1:]
            with self.assertRaisesRegex(ValueError, 'column vendorid'):
                merge_rows(cursor, [ROW, bad_row], HASH, 1)
            self.assertEqual(cursor.statements, [])
            self.assertEqual(cursor.stored, {})

    def test_integer_bounds_and_integral_floats_remain_accepted(self):
        cursor = FakeCursor()
        rows = [(n,) + ROW[1:] for n in (-(2**31), 2**31-1, np.int32(7), 1.0)]
        self.assertEqual(merge_rows(cursor, rows, HASH), 4)
        self.assertEqual(len(cursor.stored), 4)

    def test_quotes_nulls_and_timestamps_are_typed(self):
        from datetime import datetime
        cursor = FakeCursor()
        row = list(ROW)
        row[1] = datetime(2024, 1, 1)
        row[5] = float('nan')
        row[6] = "O'Brien"
        merge_rows(cursor, [row], HASH)
        payload = cursor.stored[(HASH, 0)]
        self.assertIn("CAST('O''Brien' AS varchar)", payload)
        self.assertIn('CAST(NULL AS double)', payload)
        self.assertIn("CAST(TIMESTAMP '2024-01-01 00:00:00' AS timestamp(6))", payload)

    def test_empty_input_does_not_merge(self):
        cursor = FakeCursor()
        self.assertEqual(merge_rows(cursor, [], HASH), 0)
        self.assertEqual(cursor.batch_number, 0)

    def test_hash_covers_complete_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'source.parquet'
            path.write_bytes(b'prefix' + b'a' * (1024 * 1024))
            first = file_sha256(path)
            self.assertEqual(file_sha256(path), first)
            with path.open('ab') as stream:
                stream.write(b'changed suffix')
            self.assertNotEqual(file_sha256(path), first)

    def test_canonical_ddl_keeps_business_schema_and_adds_provenance(self):
        self.assertIn('source_file_sha256 varchar', CREATE_TABLE_SQL)
        self.assertIn('source_row_number bigint', CREATE_TABLE_SQL)
        self.assertIn('format_version = 2', CREATE_TABLE_SQL)
        self.assertNotIn('partitioning', CREATE_TABLE_SQL)


class SerializationTests(unittest.TestCase):
    def test_missing_scalars_are_sql_null(self):
        for value in (None, float('nan'), pd.NA, pd.NaT, np.float32('nan'),
                      np.float64('nan'), np.datetime64('NaT', 'ns'), np.timedelta64('NaT', 'ns')):
            with self.subTest(value=repr(value)):
                self.assertEqual(_format_value(value), 'NULL')

    def test_valid_timestamp_scalars(self):
        for value in (pd.Timestamp('2024-01-03 12:34:56.123456'),
                      np.datetime64('2024-01-03T12:34:56.123456')):
            with self.subTest(value=repr(value)):
                self.assertEqual(_format_value(value),
                                 "TIMESTAMP '2024-01-03 12:34:56.123456'")

    def test_valid_numeric_scalars(self):
        for value, expected in ((0, '0'), (1.25, '1.25'), (np.int32(7), '7'),
                                (np.int64(2**53 + 1), str(2**53 + 1)),
                                (np.uint64(2**63), str(2**63)),
                                (np.float32(1.25), '1.25'), (np.float64(2.5), '2.5')):
            with self.subTest(value=repr(value)):
                self.assertEqual(_format_value(value), expected)

    def test_nonfinite_values_rejected(self):
        for value in (float('inf'), float('-inf'), np.float32('inf'), np.float64('-inf')):
            with self.subTest(value=repr(value)):
                with self.assertRaisesRegex(ValueError, 'Non-finite'):
                    _format_value(value)

    def test_nonfinite_later_batch_rejected_before_any_sql(self):
        cursor = FakeCursor()
        bad_row = list(ROW)
        bad_row[4] = float('inf')
        with self.assertRaisesRegex(ValueError, 'Non-finite'):
            merge_rows(cursor, [ROW, bad_row], HASH, 1)
        self.assertEqual(cursor.statements, [])
        self.assertEqual(cursor.stored, {})

    def test_strings_are_escaped_and_never_treated_as_missing(self):
        for value, expected in (("O'Brien", "'O''Brien'"), ('NaT', "'NaT'"),
                                ('nan', "'nan'"), ('<NA>', "'<NA>'"), ('', "''"),
                                (np.str_("O'Brien"), "'O''Brien'")):
            with self.subTest(value=repr(value)):
                self.assertEqual(_format_value(value), expected)

    def test_missing_scalars_in_merge_payload(self):
        cursor = FakeCursor()
        row = list(ROW)
        row[1], row[3], row[6] = pd.NaT, pd.NA, None
        merge_rows(cursor, [row], HASH)
        payload = cursor.stored[(HASH, 0)]
        for kind in ('timestamp(6)', 'double', 'varchar'):
            self.assertIn(f'CAST(NULL AS {kind})', payload)
        self.assertNotIn("TIMESTAMP 'NaT'", payload)
        self.assertNotIn('<NA>', payload)

    def test_nonscalar_and_unsupported_objects_rejected(self):
        for value in ([1, 2], {'a': 1}, np.array([1, 2]), object()):
            with self.subTest(value=repr(value)):
                with self.assertRaises(TypeError):
                    _format_value(value)


if __name__ == '__main__':
    unittest.main()
