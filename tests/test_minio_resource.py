"""Credential resolution must stay lazy and must never fall back to ambient AWS auth."""
import os
import unittest
from unittest.mock import patch

from lakehouse_pipeline.resources.minio_resource import MinIOResource


class MinIOCredentialTests(unittest.TestCase):
    def test_construct_without_secrets_and_fail_before_sdk_use(self):
        with patch.dict(os.environ, {}, clear=True):
            resource = MinIOResource()
            with patch("lakehouse_pipeline.resources.minio_resource.boto3.client") as client:
                with self.assertRaisesRegex(ValueError, "Missing required environment variables"):
                    resource.get_client()
                client.assert_not_called()

    def test_empty_or_partial_credentials_fail_closed(self):
        for values in ({"LAKEHOUSE_S3_ACCESS_KEY_ID": "dummy"},
                       {"LAKEHOUSE_S3_ACCESS_KEY_ID": "dummy",
                        "LAKEHOUSE_S3_SECRET_ACCESS_KEY": ""}):
            with self.subTest(values=list(values)), patch.dict(os.environ, values, clear=True):
                with patch("lakehouse_pipeline.resources.minio_resource.boto3.client") as client:
                    with self.assertRaises(ValueError):
                        MinIOResource().get_client()
                    client.assert_not_called()

    def test_uses_current_environment_at_client_creation(self):
        resource = MinIOResource()
        with patch.dict(os.environ, {"LAKEHOUSE_S3_ACCESS_KEY_ID": "dummy-id",
                                     "LAKEHOUSE_S3_SECRET_ACCESS_KEY": "dummy-secret"}, clear=True):
            with patch("lakehouse_pipeline.resources.minio_resource.boto3.client") as client:
                self.assertIs(resource.get_client(), client.return_value)
                client.assert_called_once_with(
                    "s3", endpoint_url="http://localhost:9000",
                    aws_access_key_id="dummy-id", aws_secret_access_key="dummy-secret",
                    region_name="us-east-1")
