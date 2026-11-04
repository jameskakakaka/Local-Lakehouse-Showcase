from contextlib import closing

import pyarrow.parquet as pq
from pathlib import Path

from dagster import asset, AssetExecutionContext, AssetKey, Output, MetadataValue

from ..resources.trino_resource import TrinoResource # <-- Adjusted import path
from .source_acquisition import acquire_parquet
from .ingestion_support import (
    BUSINESS_COLUMNS, CREATE_TABLE_SQL, file_sha256, merge_rows, validate_target,
)


TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = Path(__file__).parent.parent.parent / "data"
SAMPLE_SIZE = 10_000
BATCH_SIZE = 500


@asset(group_name="setup")
def iceberg_schemas(trino_resource: TrinoResource) -> Output[None]:
    """Create Iceberg schemas and the raw yellow_trips table."""
    statements = [
        "CREATE SCHEMA IF NOT EXISTS iceberg.raw",
        "CREATE SCHEMA IF NOT EXISTS iceberg.silver",
        "CREATE SCHEMA IF NOT EXISTS iceberg.gold",
        CREATE_TABLE_SQL,
    ]
    with closing(trino_resource.get_connection()) as conn:
        with closing(conn.cursor()) as cursor:
            for stmt in statements:
                cursor.execute(stmt)
                cursor.fetchall()
            validate_target(cursor)

    return Output(None, metadata={"status": MetadataValue.text("schemas + table created")})


@asset(group_name="ingestion")
def raw_taxi_file(context: AssetExecutionContext) -> Output[str]:
    """Download NYC yellow taxi parquet for Jan 2024."""
    DATA_DIR.mkdir(exist_ok=True)
    filename = "yellow_tripdata_2024-01.parquet"
    local_path = DATA_DIR / filename

    url = f"{TAXI_BASE_URL}/{filename}"
    acquisition = acquire_parquet(url, local_path)
    num_rows = acquisition['total_rows']
    context.log.info(f"File ready: {filename} ({num_rows:,} rows)")

    return Output(
        str(local_path),
        metadata={
            "filename": MetadataValue.text(filename),
            "total_rows": MetadataValue.int(num_rows),
            "source_file_sha256": MetadataValue.text(acquisition["source_file_sha256"]),
            "acquisition": MetadataValue.json(acquisition),
        },
    )


@asset(key=AssetKey(["raw", "yellow_trips"]), group_name="ingestion", deps=[iceberg_schemas])
def iceberg_raw_yellow_trips(
    context: AssetExecutionContext,
    raw_taxi_file: str,
    trino_resource: TrinoResource,
) -> Output[None]:
    """Merge ordered source occurrences into Iceberg without replay duplicates."""
    source_hash = file_sha256(raw_taxi_file)
    table = pq.read_table(raw_taxi_file)
    table = table.slice(0, SAMPLE_SIZE)
    df = table.to_pandas()
    df.columns = [c.lower() for c in df.columns]
    expected = [name for name, _ in BUSINESS_COLUMNS]
    if len(df.columns) != len(expected) or set(df.columns) != set(expected):
        raise ValueError("Unexpected source schema; no rows written")
    if file_sha256(raw_taxi_file) != source_hash:
        raise ValueError("Source file changed while reading; no rows written")

    with closing(trino_resource.get_connection()) as conn:
        with closing(conn.cursor()) as cursor:
            total = merge_rows(
                cursor,
                df[expected].itertuples(index=False, name=None),
                source_hash,
                batch_size=BATCH_SIZE,
            )
            context.log.info(f"Processed {total:,} source occurrences (existing keys skipped)")
            cursor.execute('SELECT COUNT(*) FROM iceberg.raw."yellow_trips$snapshots"')
            snap_count = cursor.fetchone()[0]

    return Output(
        None,
        metadata={
            "rows_processed": MetadataValue.int(total),
            "source_file_sha256": MetadataValue.text(source_hash),
            "total_snapshots": MetadataValue.int(snap_count),
        },
    )
