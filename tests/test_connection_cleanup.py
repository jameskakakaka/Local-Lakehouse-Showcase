"""Exercise cleanup on both successful and failed setup execution."""
import unittest
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch

from lakehouse_pipeline.assets.ingestion import iceberg_schemas, iceberg_raw_yellow_trips
from test_source_acquisition import parquet_bytes


class ConnectionCleanupTests(unittest.TestCase):
    def test_setup_closes_cursor_and_connection_on_success_and_error(self):
        for fails in (False, True):
            with self.subTest(fails=fails):
                resource = MagicMock()
                connection = resource.get_connection.return_value
                cursor = connection.cursor.return_value
                if fails:
                    cursor.execute.side_effect = RuntimeError("query failed")
                with patch("lakehouse_pipeline.assets.ingestion.validate_target"):
                    if fails:
                        with self.assertRaises(RuntimeError):
                            iceberg_schemas(resource)
                    else:
                        iceberg_schemas(resource)
                cursor.close.assert_called_once()
                connection.close.assert_called_once()

    def test_connection_closes_if_cursor_creation_fails(self):
        resource = MagicMock()
        connection = resource.get_connection.return_value
        connection.cursor.side_effect = RuntimeError('cursor unavailable')
        with self.assertRaises(RuntimeError):
            iceberg_schemas(resource)
        connection.close.assert_called_once()

    def test_ingestion_closes_resources_on_success_and_merge_failure(self):
        for fails in (False, True):
            with self.subTest(fails=fails), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'synthetic.parquet'
                content = parquet_bytes()
                path.write_bytes(content)
                resource = MagicMock()
                connection = resource.get_connection.return_value
                cursor = connection.cursor.return_value
                cursor.fetchone.return_value = (1,)
                with patch('lakehouse_pipeline.assets.ingestion.merge_rows') as merge:
                    merge.return_value = 4
                    if fails:
                        merge.side_effect = RuntimeError('merge failed')
                    # Invoke the compute function without starting a Dagster run.
                    compute = iceberg_raw_yellow_trips.op.compute_fn.decorated_fn
                    if fails:
                        with self.assertRaises(RuntimeError):
                            compute(MagicMock(), str(path), resource)
                    else:
                        compute(MagicMock(), str(path), resource)
                cursor.close.assert_called_once()
                connection.close.assert_called_once()
                self.assertEqual(path.read_bytes(), content)

    def test_ingestion_closes_resources_after_metadata_query_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'synthetic.parquet'
            path.write_bytes(parquet_bytes())
            resource = MagicMock()
            connection = resource.get_connection.return_value
            cursor = connection.cursor.return_value
            cursor.execute.side_effect = RuntimeError('snapshot query failed')
            with patch('lakehouse_pipeline.assets.ingestion.merge_rows', return_value=4):
                with self.assertRaises(RuntimeError):
                    iceberg_raw_yellow_trips.op.compute_fn.decorated_fn(MagicMock(), str(path), resource)
            cursor.close.assert_called_once()
            connection.close.assert_called_once()
