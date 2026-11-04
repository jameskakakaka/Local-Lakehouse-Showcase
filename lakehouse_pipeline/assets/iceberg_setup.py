from contextlib import closing

from dagster import asset
from .ingestion_support import CREATE_TABLE_SQL, validate_target
from ..resources.trino_resource import TrinoResource

@asset(group_name="setup")
def iceberg_schema_and_table(trino: TrinoResource):
    """
    Creates the raw database schema and the Iceberg table for NYC Taxi data.
    """
    with closing(trino.get_connection()) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.raw WITH (location = 's3://warehouse/raw/')")
            cur.fetchall()
            cur.execute(CREATE_TABLE_SQL)
            cur.fetchall()
            validate_target(cur)
