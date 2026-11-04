"""Validate and atomically acquire source bytes; never repair a cache implicitly.

SHA-256 identifies observed content, not its authenticity. Validation samples
records, not every data page. Reproducibility metadata is returned to the caller
for recording (Dagster asset metadata); no non-atomic sidecar pair is maintained.
"""
from datetime import datetime, timezone
from hashlib import sha256
import os
from pathlib import Path
import re
import stat
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from .ingestion_support import BUSINESS_COLUMNS

HTTP_TIMEOUT = (10, 60)  # Connect timeout, then socket read inactivity timeout.
SAMPLE_ROWS_PER_GROUP = 32


def validate_parquet(path):
    """Check the ingestion column contract and sample each row group, unchanged."""
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError('Source must be a regular file, not a symlink or directory')
    with path.open('rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size < 12:
            raise ValueError('Source is not a nonempty Parquet file')
        if stream.read(4) != b'PAR1':
            raise ValueError('Invalid Parquet header')
        stream.seek(-4, os.SEEK_END)
        if stream.read(4) != b'PAR1':
            raise ValueError('Invalid Parquet footer')
        stream.seek(0)
        with pq.ParquetFile(stream) as parquet:
            metadata = parquet.metadata
            if metadata.num_rows <= 0:
                raise ValueError('Source Parquet must contain rows')
            columns = [name.lower() for name in parquet.schema_arrow.names]
            required = {name for name, _ in BUSINESS_COLUMNS}
            if len(columns) != len(required) or set(columns) != required:
                raise ValueError('Unexpected source schema (missing, extra, or duplicate columns)')
            contract = dict(BUSINESS_COLUMNS)
            for field in parquet.schema_arrow:
                kind = contract[field.name.lower()]
                dtype = field.type
                if pa.types.is_dictionary(dtype):
                    dtype = dtype.value_type
                # Null-only fields and nullable fields are legitimate source data.
                # Numeric widths may vary, including floats for nullable integers.
                # Decimal is excluded: downstream scalar serialization rejects it.
                compatible = pa.types.is_null(dtype)
                if kind in ('integer', 'double'):
                    compatible |= pa.types.is_integer(dtype) or pa.types.is_floating(dtype)
                elif kind.startswith('timestamp'):
                    compatible |= pa.types.is_timestamp(dtype)
                elif kind == 'varchar':
                    compatible |= pa.types.is_string(dtype) or pa.types.is_large_string(dtype)
                if not compatible:
                    raise ValueError(
                        f'Incompatible source type for {field.name}: {field.type} '
                        f'(expected {kind} family)'
                    )
            if sum(metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)) != metadata.num_rows:
                raise ValueError('Inconsistent Parquet row counts')
            sampled = 0
            for group in range(metadata.num_row_groups):
                if metadata.row_group(group).num_rows == 0:
                    continue
                batch = next(parquet.iter_batches(
                    batch_size=SAMPLE_ROWS_PER_GROUP, row_groups=[group], use_threads=False,
                ), None)
                if batch is None or batch.num_rows == 0:
                    raise ValueError('Unreadable Parquet row group')
                batch.validate(full=True)
                batch.to_pylist()  # Exercise actual decoding, including timestamps.
                sampled += batch.num_rows
            rows = metadata.num_rows
            schema = str(parquet.schema_arrow)
        stream.seek(0)
        digest = sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
        after = os.fstat(stream.fileno())
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_size, after.st_mtime_ns, after.st_ctime_ns
        ):
            raise ValueError('Source changed during validation')
    return {
        'source_file_sha256': digest.hexdigest(),
        'size_bytes': before.st_size,
        'total_rows': rows,
        'sampled_rows': sampled,
        'schema': schema,
        'validated_at_utc': datetime.now(timezone.utc).isoformat(),
    }


def acquire_parquet(url, destination):
    """Return validated content metadata, preserving any existing final file.

    A same-directory hard link publishes a complete file atomically with
    no-clobber semantics. If another writer wins, fail for explicit review.
    """
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if os.path.lexists(destination):
        return dict(validate_parquet(destination), acquisition='validated_cache',
                    requested_source_url=url)

    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='wb', dir=destination.parent, prefix=f'.{destination.name}.', suffix='.tmp',
            delete=False,
        ) as output:
            temporary = Path(output.name)
            with requests.get(url, stream=True, timeout=HTTP_TIMEOUT,
                              headers={'Accept-Encoding': 'identity'}) as response:
                response.raise_for_status()
                # Reject partial or otherwise unexpected successful responses too.
                if response.status_code != 200:
                    raise ValueError('Expected a complete HTTP 200 response')
                if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                    raise ValueError('Encoded HTTP content is unsupported')
                length = response.headers.get('Content-Length')
                if length is not None and not re.fullmatch(r'[0-9]+', length):
                    raise ValueError('Invalid or ambiguous Content-Length')
                expected_length = int(length) if length is not None else None
                received = 0
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    received += len(chunk)
                    if expected_length is not None and received > expected_length:
                        raise ValueError('Download exceeds Content-Length')
                    output.write(chunk)
                if expected_length is not None and received != expected_length:
                    raise ValueError('Download does not match Content-Length')
                http_metadata = {
                    'response_url': response.url,
                    'content_length': expected_length,
                    'etag': response.headers.get('ETag'),
                    'last_modified': response.headers.get('Last-Modified'),
                }
            output.flush()
            os.fsync(output.fileno())
        metadata = validate_parquet(temporary)
        os.link(temporary, destination)  # Atomic, same filesystem, refuses overwrite.
        return dict(metadata, acquisition='downloaded', requested_source_url=url, **http_metadata)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
