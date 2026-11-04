"""Verify a pristine source export offline, emitting metadata only.

Run using an environment with the locked dependencies. No installation occurs.
"""
import argparse
import ast
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import warnings

ROOT = Path(__file__).resolve().parents[1]
REPORT = {'checks': {}, 'limitations': [
    'Dependency installation is external to this verifier; fresh installation and hosted CI need separate evidence.',
    'No live acceptance, credentials, policy, image or service state verified.',
    'Heuristic secret scan only; no dedicated scanner or vulnerability database.',
    'Dataset redistribution rights and remote credential exposure not queried.',
]}


def require(condition, label):
    if not condition:
        raise AssertionError(label)


def mark(label, value='PASS'):
    REPORT['checks'][label] = value
    print(label + ': ' + (value if isinstance(value, str) else 'PASS'))


def scan():
    manifest = json.loads((ROOT / 'PUBLICATION_MANIFEST.json').read_text())
    expected = {e['path']: e for e in manifest['include']}
    allowed = set(expected) | {'PUBLICATION_MANIFEST.json', 'docs/VERIFICATION_RESULTS.json'}
    files = {str(p.relative_to(ROOT)) for p in ROOT.rglob('*') if p.is_file()}
    require(files <= allowed, 'Unexpected file outside publication manifest')
    require(set(expected) <= files, 'Missing manifest file')
    findings = []
    patterns = {
        'private-key': r'-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----',
        'aws-access-key': r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b',
        'github-token': r'\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{50,})\b',
        'credential-url': r'[a-z]+://[^\s/:@]+:[^\s/@]+@',
        'private-host-path': r'/(?:home|Users)/[^\s/]+/',
    }
    for path in ROOT.rglob('*'):
        rel = str(path.relative_to(ROOT))
        require(not path.is_symlink(), 'Symlinks prohibited in export')
        require(path.name != '.git', 'Git metadata prohibited in export')
        require(path.name != '.env' and path.suffix not in ('.db','.sqlite','.sqlite3','.parquet','.pem','.key'),
                'Sensitive or runtime file prohibited in export')
        if not path.is_file():continue
        data = path.read_bytes()
        if rel in expected:
            require(hashlib.sha256(data).hexdigest() == expected[rel]['sha256'], 'Manifest digest mismatch: '+rel)
        text = data.decode('utf-8')
        # Scanner source necessarily contains patterns, not actual credentials.
        for rule, pattern in patterns.items():
            if re.search(pattern, text): findings.append({'path':rel, 'rule':rule})
        if rel.endswith(('.yml','.yaml','.properties','.py')):
            literal = re.compile(r'''(?im)^\s*(?:MINIO_ROOT_PASSWORD|POSTGRES_PASSWORD|AWS_SECRET_ACCESS_KEY|CATALOG_JDBC_PASSWORD)\s*:\s*([^\n#]+)|^s3\.aws-(?:access|secret)-key=([^\n]+)|^\s*aws_(?:access_key_id|secret_access_key)\s*:\s*str\s*=\s*["']([^"']+)["']''')
            for hit in literal.finditer(text):
                value = next(v for v in hit.groups() if v is not None).strip()
                if value and not value.startswith('${'):
                    findings.append({'path':rel, 'rule':'credential-literal'})
    REPORT['scanner'] = {'files_scanned':len(files), 'findings':findings,
                         'method':'allowlist, filename, credential-literal and common token/private-path patterns'}
    require(not findings, 'Potential secret matches; metadata recorded for private review')
    mark('manifest_and_metadata_scan')


def python_and_config():
    import tomllib
    import yaml
    for p in ROOT.rglob('*.py'):compile(p.read_text(), str(p), 'exec')
    for p in ROOT.rglob('*.yml'):yaml.safe_load(p.read_text())
    for p in (ROOT/'pyproject.toml',ROOT/'uv.lock'):
        with p.open('rb') as stream:tomllib.load(stream)
    workflow = yaml.load((ROOT/'.github/workflows/ci.yml').read_text(),Loader=yaml.BaseLoader)
    require(set(workflow['on']) == {'push','pull_request'}, 'Workflow triggers mismatch')
    require(workflow['permissions'] == {'contents':'read'}, 'Workflow permissions mismatch')
    for step in workflow['jobs']['offline']['steps']:
        if 'uses' in step:require(bool(re.search(r'@[0-9a-f]{40}$',step['uses'])), 'Unpinned CI action')
    for file in ROOT.rglob('*.md'):
        for link in re.findall(r'\]\(([^)]+)\)', file.read_text()):
            if link.startswith(('http:','https:','#','mailto:')):continue
            target = (file.parent / link.split('#',1)[0]).resolve()
            require(target.is_relative_to(ROOT) and target.exists(), 'Broken local Markdown link: '+str(file.relative_to(ROOT)))
    mark('syntax_ci_and_documentation_links')


def tests():
    sys.path.insert(0,str(ROOT/'tests'))
    with warnings.catch_warnings(), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        warnings.simplefilter('error')
        suite=unittest.defaultTestLoader.discover(str(ROOT/'tests'))
        result=unittest.TestResult()
        suite.run(result)
    REPORT['unittest']={'run':result.testsRun,'passed':result.testsRun-len(result.skipped)-len(result.errors)-len(result.failures),
                        'skipped':len(result.skipped),'errors':len(result.errors),'failures':len(result.failures)}
    require(result.wasSuccessful(), 'Unittest failed; counts recorded without exception contents')
    require(result.testsRun==74 and len(result.skipped)==4, 'Unexpected unittest inventory')
    mark('offline_unittest',REPORT['unittest'])


def dbt_and_graph(work):
    from dbt.cli.main import dbtRunner
    import shutil
    for mode in ('parse','reusable','acceptance','dev'):
        target='dev' if mode=='dev' else 'local_acceptance'
        out=work/mode
        common=['--project-dir',str(ROOT/'dbt_project'),'--profiles-dir',str(ROOT/'lakehouse_pipeline/trino'),
                '--target',target,'--target-path',str(out/'target'),'--log-path',str(out/'logs'),'--no-partial-parse']
        args=['parse' if mode=='parse' else 'compile',*common]
        if mode!='parse':args+=['--no-introspect','--no-populate-cache']
        if mode in ('acceptance','dev'):args+=['--vars','{dq_local_acceptance: true}']
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result=dbtRunner().invoke(args)
        require(result.success,'dbt '+mode+' failed; logs retained in temporary workspace')
        manifest=json.loads((out/'target/manifest.json').read_text())
        nodes=manifest['nodes']
        count=sum(n['resource_type']=='test' for n in nodes.values())
        require(count==(12 if mode=='acceptance' else 11),'dbt test count mismatch')
        schemas={n['name']:n['schema'] for n in nodes.values() if n['resource_type']=='model'}
        expected={'stg_yellow_trips':'silver_silver','fct_daily_trips':'silver_gold'} if mode=='dev' else {'stg_yellow_trips':'silver','fct_daily_trips':'gold'}
        require(schemas==expected,'dbt schema routing mismatch')
        if mode!='parse':require(all(n.get('compiled_code') for n in nodes.values() if n['resource_type'] in ('test','model')),'Uncompiled dbt node')
        if mode in ('reusable','acceptance'):
            with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
                listing=dbtRunner().invoke(['ls',*common,'--resource-type','test','--output','name']+(['--vars','{dq_local_acceptance: true}'] if mode=='acceptance' else []))
            require(listing.success and len(listing.result)==count,'dbt discovery count mismatch')
        mark('dbt_'+mode,{'tests':count,'schemas':schemas})
    # Only our own newly created manifest is ever removed. Refuse existing state.
    target=ROOT/'dbt_project/target'
    require(not target.exists(),'Default dbt target already exists; use a pristine export')
    target.mkdir()
    manifest_path=target/'manifest.json'
    try:
        shutil.copyfile(work/'reusable/target/manifest.json',manifest_path)
        from dagster import AssetKey,Definitions
        from lakehouse_pipeline.definitions import defs
        Definitions.validate_loadable(defs)
        graph=defs.resolve_asset_graph()
        for child,parent in [(['silver','stg_yellow_trips'],['raw','yellow_trips']),(['gold','fct_daily_trips'],['silver','stg_yellow_trips']),(['raw','yellow_trips'],['iceberg_schemas'])]:
            require(AssetKey(parent) in graph.get(AssetKey(child)).parent_keys,'Asset lineage mismatch')
        require(len(defs.resolve_job_def('lakehouse_ingestion_pipeline').graph.node_defs)==3,'Ingestion job mismatch')
        mark('dagster_definitions_and_lineage')
    finally:
        manifest_path.unlink(missing_ok=True)
        target.rmdir()
    modules=[m for name,m in sys.modules.items() if name=='lakehouse_pipeline' or name.startswith('lakehouse_pipeline.')]
    require(all(Path(m.__file__).resolve().is_relative_to(ROOT) for m in modules),'Application import escaped candidate')
    mark('application_import_origin',{'modules':len(modules),'all_within_candidate':True})


def external_checks(work):
    env={k:os.environ[k] for k in ('PATH','HOME') if k in os.environ}
    env['UV_CACHE_DIR']=str(work/'uv-cache')
    run=subprocess.run(['uv','lock','--check','--offline'],cwd=ROOT,env=env,capture_output=True,text=True)
    require(run.returncode==0,'Offline lock consistency failed; diagnostics withheld')
    mark('uv_lock_offline')
    # Parse the candidate's ignore rules without initializing any Git repository.
    import pathspec
    rules=pathspec.GitIgnoreSpec.from_lines((ROOT/'.gitignore').read_text().splitlines())
    paths=[entry['path'] for entry in json.loads((ROOT/'PUBLICATION_MANIFEST.json').read_text())['include']]
    ignored=['.env','.env.production','secrets/example','example.db','example.sqlite3','data/example.parquet']
    require(not any(rules.match_file(p) for p in paths),'Required source is ignored')
    require(all(rules.match_file(p) for p in ignored),'Local artifact not ignored')
    mark('gitignore_source_visibility')
    empty=work/'empty.env';empty.write_text('')
    names=('MINIO_ROOT_USER','MINIO_ROOT_PASSWORD','POSTGRES_USER','POSTGRES_PASSWORD','LAKEHOUSE_S3_ACCESS_KEY_ID','LAKEHOUSE_S3_SECRET_ACCESS_KEY')
    for missing in (None,*names,'ALL'):
        values={n:'validation-only-dummy' for n in names}
        if missing=='ALL':values.clear()
        elif missing:values.pop(missing)
        result=subprocess.run(['docker','compose','--env-file',str(empty),'-f',str(ROOT/'docker-compose.yml'),'config','--quiet'],cwd=ROOT,env={**env,**values},capture_output=True,text=True)
        require((result.returncode==0)==(missing is None),'Compose dummy validation failed; resolved output withheld')
    mark('compose_dummy_configuration',{'success_cases':1,'required_input_rejections':7})


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--report',type=Path)
    args=parser.parse_args()
    os.chdir(ROOT)
    sys.dont_write_bytecode=True
    sys.path.insert(0,str(ROOT))
    os.environ.pop('RUN_TRINO_ICEBERG_TESTS',None)
    os.environ.pop('DAGSTER_IS_DEV_CLI',None)
    os.environ['DBT_SEND_ANONYMOUS_USAGE_STATS']='false'
    # The interpreter may come from an existing environment; source must not.
    os.environ['PATH']=str(Path(sys.executable).parent)+os.pathsep+os.environ.get('PATH','')
    work=Path(tempfile.mkdtemp(prefix='lakehouse-candidate-verification-'))
    def deny(*args,**kwargs):raise AssertionError('Outbound connection attempted during offline verification')
    try:
        scan()
        python_and_config()
        with patch.object(socket.socket,'connect',deny),patch.object(socket.socket,'connect_ex',deny):
            tests()
            dbt_and_graph(work)
        external_checks(work)
        scan()  # Check that no verification artifacts entered the export.
        REPORT['status']='PASS'
    except Exception as error:
        REPORT['status']='FAIL'
        REPORT['failure_type']=type(error).__name__
        print('Verification failed; no exception values or secret contents emitted.')
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(json.dumps(REPORT,indent=2)+'\n')
    print('Verification status: '+REPORT['status'])
    return 0 if REPORT['status']=='PASS' else 1


if __name__=='__main__':
    raise SystemExit(main())
