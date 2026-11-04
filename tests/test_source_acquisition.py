"""Offline source acquisition tests: generated Parquet bytes and mocked HTTP only."""
from datetime import datetime
from hashlib import sha256
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from lakehouse_pipeline.assets import source_acquisition as source
from lakehouse_pipeline.assets.ingestion_support import BUSINESS_COLUMNS

URL = 'https://example.invalid/yellow_tripdata_2024-01.parquet'


def parquet_bytes(rows=4, missing=False, duplicate=False, extra=False,
                  overrides=None, nullable=False):
    fields, arrays = [], []
    for name, kind in BUSINESS_COLUMNS:
        if missing and name == 'vendorid':
            continue
        if kind.startswith('timestamp'):
            value, dtype = datetime(2024, 1, 1, 12, 30), pa.timestamp('us')
        elif kind == 'varchar':
            value, dtype = "O'Brien", pa.string()
        elif kind == 'integer':
            value, dtype = 1, pa.int64()
        else:
            value, dtype = 1.0, pa.float64()
        dtype, value = (overrides or {}).get(name, (dtype, value))
        fields.append(name.upper())  # Matches ingestion's case normalization.
        values = [None if nullable and i % 2 else value for i in range(rows)]
        arrays.append(pa.array(values, type=dtype))
    if duplicate or extra:
        fields.append('vendorid' if duplicate else 'unexpected')
        arrays.append(pa.array([1] * rows, type=pa.int64()))
    table = pa.Table.from_arrays(arrays, names=fields)
    output = pa.BufferOutputStream()
    pq.write_table(table, output, row_group_size=2)
    return output.getvalue().to_pybytes()


def response_for(content, headers=None):
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.status_code = 200
    response.url = URL
    response.headers = {'Content-Length': str(len(content)), 'ETag': 'observed-etag'} if headers is None else headers
    response.iter_content.return_value = iter([content[:50], b'', content[50:]])
    return response


class SourceAcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'yellow_tripdata_2024-01.parquet'
        self.content = parquet_bytes()
        get_patch = patch.object(source.requests, 'get')
        self.get = get_patch.start()
        self.addCleanup(get_patch.stop)
        self.response = response_for(self.content)
        self.get.return_value = self.response

    def assert_no_artifacts(self):
        self.assertEqual(list(self.path.parent.iterdir()), [])

    def test_successful_download_and_reproducibility_metadata(self):
        result = source.acquire_parquet(URL, self.path)
        self.get.assert_called_once_with(URL, stream=True, timeout=(10, 60),
                                         headers={'Accept-Encoding': 'identity'})
        self.response.raise_for_status.assert_called_once()
        self.response.__exit__.assert_called_once()
        self.assertEqual(self.path.read_bytes(), self.content)
        self.assertEqual(result['source_file_sha256'], sha256(self.content).hexdigest())
        self.assertEqual(result['size_bytes'], len(self.content))
        self.assertEqual(result['total_rows'], 4)
        self.assertEqual(result['sampled_rows'], 4)
        self.assertEqual(result['acquisition'], 'downloaded')
        self.assertEqual(result['requested_source_url'], URL)
        self.assertEqual(result['etag'], 'observed-etag')
        self.assertIn('VENDORID: int64', result['schema'])
        self.assertIn('+00:00', result['validated_at_utc'])
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_valid_cache_reused_without_http_and_hash_is_stable(self):
        self.path.write_bytes(self.content)
        first = source.acquire_parquet(URL, self.path)
        second = source.acquire_parquet(URL, self.path)
        self.get.assert_not_called()
        self.assertEqual(first['acquisition'], 'validated_cache')
        self.assertEqual(first['source_file_sha256'], second['source_file_sha256'])
        self.assertEqual(first['source_file_sha256'], sha256(self.content).hexdigest())
        self.assertEqual(self.path.read_bytes(), self.content)

    def test_identical_downloads_have_identical_hashes(self):
        first = source.acquire_parquet(URL, self.path)
        other = self.path.with_name('copy.parquet')
        self.get.return_value = response_for(self.content)
        second = source.acquire_parquet(URL, other)
        self.assertEqual(first['source_file_sha256'], second['source_file_sha256'])

    def test_interrupted_download_cleans_temporary_file(self):
        def interrupted():
            yield self.content[:50]
            raise requests.ConnectionError('interrupted')
        self.response.iter_content.return_value = interrupted()
        with self.assertRaises(requests.ConnectionError):
            source.acquire_parquet(URL, self.path)
        self.response.__exit__.assert_called_once()
        self.assert_no_artifacts()

    def test_http_errors_and_connection_timeouts_clean_up(self):
        for error in (requests.HTTPError('404'), requests.HTTPError('500'),
                      requests.ConnectTimeout('connect'), requests.ReadTimeout('read')):
            with self.subTest(error=error):
                self.get.side_effect = None
                self.response.raise_for_status.side_effect = None
                if isinstance(error, requests.HTTPError):
                    self.response.raise_for_status.side_effect = error
                else:
                    self.get.side_effect = error
                with self.assertRaises(type(error)):
                    source.acquire_parquet(URL, self.path)
                self.assert_no_artifacts()

    def test_read_timeout_during_stream_cleans_up(self):
        self.response.iter_content.side_effect = requests.ReadTimeout('stream stalled')
        with self.assertRaises(requests.ReadTimeout):
            source.acquire_parquet(URL, self.path)
        self.assert_no_artifacts()

    def test_content_length_missing_is_allowed_but_mismatch_or_ambiguity_fails(self):
        for length in (str(len(self.content) + 1), str(len(self.content) - 1),
                       '-1', 'abc', '1, 1', ''):
            with self.subTest(length=length):
                self.get.return_value = response_for(self.content, {'Content-Length': length})
                with self.assertRaises(ValueError):
                    source.acquire_parquet(URL, self.path)
                self.assert_no_artifacts()
        self.get.return_value = response_for(self.content, {})
        self.assertIsNone(source.acquire_parquet(URL, self.path)['content_length'])

    def test_partial_http_response_and_encoded_body_rejected(self):
        for status, headers in ((206, {}), (204, {}), (200, {'Content-Encoding': 'gzip'})):
            with self.subTest(status=status, headers=headers):
                response = response_for(self.content, headers)
                response.status_code = status
                self.get.return_value = response
                with self.assertRaises(ValueError):
                    source.acquire_parquet(URL, self.path)
                self.assert_no_artifacts()

    def test_invalid_truncated_zero_row_and_bad_columns_rejected(self):
        for content in (b'not parquet', self.content[:-10], parquet_bytes(rows=0),
                        parquet_bytes(missing=True), parquet_bytes(duplicate=True),
                        parquet_bytes(extra=True)):
            with self.subTest(size=len(content)):
                self.get.return_value = response_for(content)
                with self.assertRaises(ValueError):
                    source.acquire_parquet(URL, self.path)
                self.assert_no_artifacts()

    def test_bad_cached_files_fail_closed_without_http_or_overwrite(self):
        for content in (b'bad cache', self.content[:-10], parquet_bytes(rows=0),
                        parquet_bytes(missing=True), parquet_bytes(duplicate=True)):
            with self.subTest(size=len(content)):
                self.path.write_bytes(content)
                with self.assertRaises(ValueError):
                    source.acquire_parquet(URL, self.path)
                self.assertEqual(self.path.read_bytes(), content)
                self.get.assert_not_called()

    def test_nullable_numeric_and_timestamp_families_are_accepted(self):
        for numeric in (pa.int16(), pa.int32(), pa.int64(), pa.uint32(),
                        pa.float32(), pa.float64()):
            for unit in ('ms', 'us', 'ns'):
                with self.subTest(numeric=numeric, timestamp_unit=unit):
                    content = parquet_bytes(nullable=True, overrides={
                        'vendorid': (numeric, 1),
                        'passenger_count': (numeric, 2),
                        'fare_amount': (numeric, 3),
                        'tpep_pickup_datetime': (pa.timestamp(unit), datetime(2024, 1, 1)),
                        'tpep_dropoff_datetime': (pa.timestamp(unit), datetime(2024, 1, 2)),
                    })
                    self.path.write_bytes(content)
                    result = source.acquire_parquet(URL, self.path)
                    self.assertEqual(result['total_rows'], 4)
                    self.assertEqual(result['source_file_sha256'], sha256(content).hexdigest())
        self.get.assert_not_called()

    def test_null_only_columns_and_string_variants_are_accepted(self):
        variants = [
            {name: (pa.null(), None) for name, _ in BUSINESS_COLUMNS},
            {'store_and_fwd_flag': (pa.large_string(), 'N')},
            {'store_and_fwd_flag': (pa.dictionary(pa.int32(), pa.string()), 'N')},
        ]
        for overrides in variants:
            with self.subTest(overrides=overrides):
                self.path.write_bytes(parquet_bytes(overrides=overrides, nullable=True))
                self.assertEqual(source.acquire_parquet(URL, self.path)['total_rows'], 4)
        self.get.assert_not_called()

    def test_incompatible_types_reject_downloads_and_caches(self):
        from decimal import Decimal
        cases = (
            ('vendorid', pa.string(), '1'),
            ('vendorid', pa.bool_(), True),
            ('trip_distance', pa.binary(), b'1'),
            ('fare_amount', pa.decimal128(10, 2), Decimal('1.00')),
            ('passenger_count', pa.list_(pa.int64()), [1]),
            ('tpep_pickup_datetime', pa.int64(), 1704067200),
            ('tpep_dropoff_datetime', pa.string(), '2024-01-01'),
            ('store_and_fwd_flag', pa.int64(), 1),
            ('store_and_fwd_flag', pa.struct([('flag', pa.string())]), {'flag': 'N'}),
        )
        for column, dtype, value in cases:
            content = parquet_bytes(overrides={column: (dtype, value)}, nullable=True)
            with self.subTest(column=column, dtype=dtype, mode='download'):
                self.get.return_value = response_for(content)
                with self.assertRaisesRegex(ValueError, f'Incompatible source type for {column.upper()}'):
                    source.acquire_parquet(URL, self.path)
                self.assert_no_artifacts()
            with self.subTest(column=column, dtype=dtype, mode='cache'):
                self.path.write_bytes(content)
                self.get.reset_mock()
                with self.assertRaisesRegex(ValueError, f'Incompatible source type for {column.upper()}'):
                    source.acquire_parquet(URL, self.path)
                self.get.assert_not_called()
                self.assertEqual(self.path.read_bytes(), content)
                self.path.unlink()

    def test_symlink_and_directory_caches_are_rejected(self):
        other = self.path.with_name('other.parquet')
        other.write_bytes(self.content)
        self.path.symlink_to(other)
        with self.assertRaises(ValueError):
            source.acquire_parquet(URL, self.path)
        self.path.unlink()
        self.path.symlink_to(self.path.with_name('missing'))
        with self.assertRaises(ValueError):
            source.acquire_parquet(URL, self.path)
        self.path.unlink()
        self.path.mkdir()
        with self.assertRaises(ValueError):
            source.acquire_parquet(URL, self.path)
        self.get.assert_not_called()
        self.assertEqual(other.read_bytes(), self.content)

    def test_samples_each_row_group_and_detects_unreadable_records(self):
        damaged = bytearray(self.content)
        with pq.ParquetFile(pa.BufferReader(self.content)) as parquet:
            offset = parquet.metadata.row_group(1).column(0).data_page_offset
        damaged[offset:offset + 10] = b'\xff' * 10  # Keep footer/schema intact.
        self.path.write_bytes(damaged)
        with self.assertRaises((ValueError, OSError)):
            source.acquire_parquet(URL, self.path)
        self.get.assert_not_called()
        self.assertEqual(self.path.read_bytes(), damaged)

    def test_publication_is_atomic_and_only_after_validation(self):
        real_link = source.os.link
        def publish(temporary, final):
            self.assertEqual(Path(temporary).parent, self.path.parent)
            self.assertFalse(self.path.exists())
            self.assertEqual(Path(temporary).read_bytes(), self.content)
            self.assertEqual(source.validate_parquet(temporary)['total_rows'], 4)
            real_link(temporary, final)
            self.assertEqual(self.path.read_bytes(), self.content)
        with patch.object(source.os, 'link', side_effect=publish) as link:
            source.acquire_parquet(URL, self.path)
        link.assert_called_once()
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_publication_failure_cleans_temp(self):
        with patch.object(source.os, 'link', side_effect=OSError('publication failed')):
            with self.assertRaises(OSError):
                source.acquire_parquet(URL, self.path)
        self.assert_no_artifacts()

    def test_existing_valid_file_appearing_during_download_survives_failure(self):
        def interrupted():
            yield self.content[:50]
            self.path.write_bytes(self.content)  # Simulate a concurrent successful writer.
            raise requests.ConnectionError('interrupted')
        self.response.iter_content.return_value = interrupted()
        with self.assertRaises(requests.ConnectionError):
            source.acquire_parquet(URL, self.path)
        self.assertEqual(self.path.read_bytes(), self.content)
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_concurrent_publication_never_overwrites_existing_valid_file(self):
        other_content = parquet_bytes(rows=6)
        real_link = source.os.link
        def concurrent_publish(temporary, final):
            self.path.write_bytes(other_content)
            real_link(temporary, final)
        with patch.object(source.os, 'link', side_effect=concurrent_publish):
            with self.assertRaises(FileExistsError):
                source.acquire_parquet(URL, self.path)
        self.assertEqual(self.path.read_bytes(), other_content)
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])


if __name__ == '__main__':
    unittest.main()
