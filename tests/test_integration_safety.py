"""Offline checks of configurable SQL targets and integration isolation guards."""
import unittest
from unittest.mock import patch

from integration import test_trino_iceberg as live_tests
from integration.test_trino_iceberg import check_isolation, check_table_sql, isolated_target
from lakehouse_pipeline.assets.ingestion_support import (
    TARGET, create_table_sql, merge_rows, validate_target,
)
from test_ingestion_idempotency import FakeCursor, HASH, ROW


class IntegrationSafetyTests(unittest.TestCase):
    def test_all_generated_sql_uses_only_isolated_target(self):
        schema, target, location = isolated_target()
        check_isolation(target, location)
        cursor = FakeCursor()
        merge_rows(cursor, [ROW, ROW], HASH, batch_size=1, target=target)
        for sql in cursor.statements:
            check_table_sql(sql, target)
            self.assertIn(target, sql)
            self.assertNotIn(TARGET, sql)
        ddl = create_table_sql(target, location + 'table/')
        check_table_sql(ddl, target)
        self.assertIn(f"location = '{location}table/'", ddl)
        self.assertNotIn(TARGET, ddl)
        self.assertNotIn('.raw.', ddl)
        self.assertNotIn('.silver.', ddl)
        self.assertNotIn('.gold.', ddl)
        self.assertEqual(schema, target.rsplit('.', 1)[0])

    def test_production_default_is_preserved(self):
        cursor = FakeCursor()
        merge_rows(cursor, [ROW], HASH)
        self.assertIn(f'MERGE INTO {TARGET} AS target', cursor.statements[-1])
        self.assertIn(f'CREATE TABLE IF NOT EXISTS {TARGET}', create_table_sql())

    def test_bad_identifiers_fail_before_sql(self):
        for target in ('raw.yellow_trips', 'iceberg.raw.t; DROP TABLE x',
                       'iceberg.raw.t -- comment', 'iceberg.raw."t"',
                       'iceberg.raw.t\n', 'iceberg.raw.t.extra',
                       'iceberg.raw.t/*x*/', 'iceberg.raw.t WHERE 1=1',
                       'iceberg.raw.t-name', None):
            for operation in ('merge', 'validate', 'ddl'):
                with self.subTest(target=target, operation=operation):
                    cursor = FakeCursor()
                    with self.assertRaises(ValueError):
                        if operation == 'merge':
                            merge_rows(cursor, [ROW], HASH, target=target)
                        elif operation == 'validate':
                            validate_target(cursor, target=target)
                        else:
                            create_table_sql(target)
                    self.assertEqual(cursor.statements, [])

    def test_production_and_mismatched_locations_rejected_by_harness(self):
        _, target, location = isolated_target()
        for bad_target, bad_location in ((TARGET, location),
                                         ('iceberg.silver.example', location),
                                         ('iceberg.gold.example', location),
                                         (target, 's3://warehouse/raw/')):
            with self.subTest(target=bad_target):
                with self.assertRaises(ValueError):
                    check_isolation(bad_target, bad_location)

    def test_cursor_guard_rejects_other_tables_and_destructive_commands(self):
        _, target, _ = isolated_target()
        for sql in (f'SELECT * FROM {TARGET}', f'DESCRIBE {TARGET}',
                    f'MERGE INTO {TARGET} AS target USING (VALUES (1)) AS source (x)',
                    f'DROP TABLE {target}', f'DELETE FROM {target}',
                    f'SELECT * FROM {target} JOIN {TARGET} ON true',
                    'SELECT count(*) FROM iceberg.silver.example',
                    'SELECT count(*) FROM iceberg.gold.example'):
            with self.subTest(sql=sql):
                with self.assertRaises(ValueError):
                    check_table_sql(sql, target)

    def assert_guard_rejects(self, sql, target):
        raw = FakeCursor()
        guarded = live_tests.IsolatedCursor(raw, target)
        with self.assertRaises(ValueError):
            guarded.execute(sql)
        self.assertEqual(raw.statements, [])
        self.assertEqual(guarded.merge_attempts, 0)

    def test_multiple_statements_and_comments_fail_before_submission(self):
        _, target, _ = isolated_target()
        select = f'SELECT count(*) FROM {target}'
        for tail in (f'; DROP TABLE {TARGET}', f'; SELECT * FROM {TARGET}',
                     f' --\nDROP TABLE {TARGET}', f' /* hidden */ DROP TABLE {TARGET}',
                     f'/**/ UNION SELECT * FROM {TARGET}', ';'):
            with self.subTest(tail=tail):
                self.assert_guard_rejects(select + tail, target)

    def test_guard_requires_isolated_target_even_for_allowed_sql(self):
        _, target, _ = isolated_target()
        for bad in (TARGET, 'iceberg.silver.example', 'iceberg.gold.example',
                    target.replace('yellow_trips_', 'yellow_trips_0'),
                    'iceberg.fix4b_example.yellow_trips_example'):
            with self.subTest(target=bad):
                with self.assertRaises(ValueError):
                    live_tests.IsolatedCursor(FakeCursor(), bad)
                with self.assertRaises(ValueError):
                    check_table_sql(f'DESCRIBE {bad}', bad)

    def test_wrong_storage_and_schema_prefixes_fail_closed(self):
        _, target, location = isolated_target()
        _, _, other_location = isolated_target()
        for bad_location in ('s3://warehouse/raw/', 's3://warehouse/silver/',
                             's3://warehouse/gold/', other_location + 'table/',
                             location, location + 'table/extra/',
                             location + '../table/',
                             location.replace('warehouse', 'other') + 'table/'):
            with self.subTest(location=bad_location):
                ddl = create_table_sql(target, location + 'table/')
                ddl = ddl.replace(location + 'table/', bad_location)
                self.assert_guard_rejects(ddl, target)
                if bad_location != location:
                    with self.assertRaises(ValueError):
                        live_tests.schema_sql(target, bad_location)
        self.assert_guard_rejects(create_table_sql(target), target)
        self.assertEqual(live_tests.schema_sql(target, location),
                         f"CREATE SCHEMA {target.rsplit('.', 1)[0]} WITH (location = '{location}')")

    def test_unsupported_commands_and_query_shapes_fail_closed(self):
        _, target, _ = isolated_target()
        for sql in (f'INSERT INTO {target} VALUES (1)', f'ALTER TABLE {target} ADD COLUMN x int',
                    f'CALL {target}()', f'TRUNCATE TABLE {target}',
                    f'EXPLAIN SELECT count(*) FROM {target}',
                    f'SELECT count(*) FROM {target}, {TARGET}',
                    f'SELECT count(*) FROM {target} UNION SELECT count(*) FROM {TARGET}',
                    f'SELECT count(*) FROM {target} DROP TABLE {TARGET}'):
            with self.subTest(sql=sql):
                self.assert_guard_rejects(sql, target)

    def test_generated_merges_allow_quotes_but_reject_changed_structure(self):
        _, target, _ = isolated_target()
        raw = FakeCursor()
        guarded = live_tests.IsolatedCursor(raw, target)
        merge_rows(guarded, live_tests.synthetic_rows(), HASH, batch_size=2, target=target)
        self.assertEqual(guarded.merge_attempts, 3)
        merge = raw.statements[-1]
        for changed in (merge + f'; DROP TABLE {TARGET}',
                        merge.replace('USING (VALUES ', f'USING (SELECT * FROM {TARGET}) '),
                        merge.replace('WHEN NOT MATCHED', 'WHEN MATCHED'),
                        merge.replace("CAST(7 AS integer)", f"CAST((SELECT count(*) FROM {TARGET}) AS integer)")):
            with self.subTest(sql=changed):
                self.assertNotEqual(changed, merge)
                self.assert_guard_rejects(changed, target)

    def test_location_sql_injection_rejected(self):
        with self.assertRaises(ValueError):
            create_table_sql(location="s3://warehouse/test/'); DROP TABLE x; --")

    def test_discovery_does_not_connect_without_opt_in(self):
        self.assertTrue(live_tests.TrinoIcebergIntegrationTests.__unittest_skip__)
        with patch('trino.dbapi.connect') as connect:
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(live_tests.TrinoIcebergIntegrationTests)
            result = unittest.TestResult()
            suite.run(result)
            connect.assert_not_called()
            self.assertEqual(len(result.skipped), 4)
            self.assertEqual(result.errors, [])


if __name__ == '__main__':
    unittest.main()
