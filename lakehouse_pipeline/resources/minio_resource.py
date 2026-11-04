import os

import boto3
from dagster import ConfigurableResource


class MinIOResource(ConfigurableResource):
    """Resolve local S3 credentials only when a client is requested."""

    endpoint_url: str = "http://localhost:9000"

    def get_client(self):
        names = ("LAKEHOUSE_S3_ACCESS_KEY_ID", "LAKEHOUSE_S3_SECRET_ACCESS_KEY")
        missing = [name for name in names if not os.environ.get(name)]
        if missing:
            raise ValueError("Missing required environment variables: " + ", ".join(missing))
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=os.environ[names[0]],
            aws_secret_access_key=os.environ[names[1]],
            region_name="us-east-1",
        )
