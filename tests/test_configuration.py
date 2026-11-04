"""Offline checks of security boundaries; never load .env or resolve real values."""
from pathlib import Path
import re
import subprocess
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]


class ConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.services = yaml.safe_load((ROOT / 'docker-compose.yml').read_text())['services']
        except yaml.YAMLError:
            raise ValueError('Invalid Compose YAML; parser details withheld') from None

    def test_credentials_are_required_and_explicitly_injected(self):
        mappings = {
            'minio': {'MINIO_ROOT_USER': 'MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD': 'MINIO_ROOT_PASSWORD'},
            'mc': {'MINIO_ROOT_USER': 'MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD': 'MINIO_ROOT_PASSWORD'},
            'postgres': {'POSTGRES_USER': 'POSTGRES_USER', 'POSTGRES_PASSWORD': 'POSTGRES_PASSWORD'},
            'rest': {'AWS_ACCESS_KEY_ID': 'LAKEHOUSE_S3_ACCESS_KEY_ID',
                     'AWS_SECRET_ACCESS_KEY': 'LAKEHOUSE_S3_SECRET_ACCESS_KEY',
                     'CATALOG_JDBC_USER': 'POSTGRES_USER', 'CATALOG_JDBC_PASSWORD': 'POSTGRES_PASSWORD'},
            'trino': {'LAKEHOUSE_S3_ACCESS_KEY_ID': 'LAKEHOUSE_S3_ACCESS_KEY_ID',
                      'LAKEHOUSE_S3_SECRET_ACCESS_KEY': 'LAKEHOUSE_S3_SECRET_ACCESS_KEY'},
        }
        for service, mapping in mappings.items():
            for key, variable in mapping.items():
                self.assertTrue(self.services[service]['environment'].get(key) == '${' + variable + ':?required}',
                                f'Required external reference missing: {service}/{key}')

    def test_host_ports_stay_on_loopback(self):
        for service in self.services.values():
            for port in service.get('ports', []):
                self.assertTrue(isinstance(port, str) and port.startswith('127.0.0.1:'),
                                'Published host port must bind loopback')
        self.assertFalse(self.services['postgres'].get('ports'))

    def test_initialization_preserves_policy_and_shell_references(self):
        script = self.services['mc']['entrypoint'][-1]
        self.assertFalse(re.search(r'\b(?:anonymous|policy)\s+set\b', script))
        for name in ('MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD'):
            self.assertTrue('"$${' + name + '}"' in script, 'Shell credential must expand inside container')
        result = subprocess.run(['/bin/sh', '-n'], input=script, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, 'Initialization shell syntax failed')

    def test_trino_uses_native_environment_references(self):
        properties = dict(line.split('=', 1) for line in
                          (ROOT / 'lakehouse_pipeline/trino/catalog/iceberg.properties').read_text().splitlines()
                          if line.strip() and not line.startswith('#'))
        for key, variable in [('s3.aws-access-key', 'LAKEHOUSE_S3_ACCESS_KEY_ID'),
                              ('s3.aws-secret-key', 'LAKEHOUSE_S3_SECRET_ACCESS_KEY')]:
            self.assertTrue(properties.get(key) == '${ENV:' + variable + '}',
                            'Trino credential must use its native ENV resolver')

    def test_environment_example_has_only_empty_required_inputs(self):
        example = (ROOT / '.env.example').read_text()
        names = set()
        for line in example.splitlines():
            if not line.strip() or line.lstrip().startswith('#'):
                continue
            name, value = line.split('=', 1)
            names.add(name)
            self.assertFalse(value.strip(), 'Environment example must contain no credential values')
        self.assertEqual(names, {'MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD', 'POSTGRES_USER',
                                 'POSTGRES_PASSWORD', 'LAKEHOUSE_S3_ACCESS_KEY_ID',
                                 'LAKEHOUSE_S3_SECRET_ACCESS_KEY'})

