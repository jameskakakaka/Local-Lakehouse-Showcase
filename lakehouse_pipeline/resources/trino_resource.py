import trino
from dagster import ConfigurableResource

class TrinoResource(ConfigurableResource):
    """
    A Dagster resource to manage connections to our local Trino query engine.
    """
    host: str = "localhost"
    port: int = 8085  # The port mapped to our local machine in docker-compose
    user: str = "admin"
    catalog: str = "iceberg"

    def get_connection(self):
        """Returns a configured Trino database connection."""
        return trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
        )