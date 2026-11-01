"""Offline simulation world for the IR-023 pilot: fake NOAA archive transport, local bare Git remote, simulated Pages,
fake GitHub Actions API, test-only Ed25519 identity. Any non-loopback network connection fails the test."""
from __future__ import annotations
import base64, hashlib, io, json, os, shutil, socket, subprocess, sys, urllib.parse, urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
import pytest

HERE = Path(__file__).resolve()
PUBLICATION = HERE.parents[2]
PRODUCER = Path(os.environ.get('IR023_PRODUCER', PUBLICATION.parent / 'immersive-radar')).resolve()
sys.path[:0] = [str(PRODUCER / 'pipeline'), str(PUBLICATION / 'ops')]

import pilot  # noqa: E402
from immersive_radar_pipeline import fetch, foreground, publish as pub, snapshot  # noqa: E402

FIXTURES = PRODUCER / 'Data/IR015'
BUCKET_HOST = 'unidata-nexrad-level2.s3.amazonaws.com'
KEYS = ['2026/10/30/KINX/KINX20261030_152144_V06', '2026/10/30/KINX/KINX20261030_152702_V06',
        '2026/10/30/KINX/KINX20261030_153250_V06', '2026/10/30/KINX/KINX20261030_153823_V06',
        '2026/10/30/KINX/KINX20261030_154432_V06']
T0 = datetime(2026, 10, 30, 15, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    real = socket.socket.connect

    def connect(self, address):
        host = address[0] if isinstance(address, tuple) else address
        if isinstance(host, str) and host not in ('127.0.0.1', '::1', 'localhost') and self.family != socket.AF_UNIX:
            raise AssertionError(f'network connection attempted: {address}')
        return real(self, address)
    monkeypatch.setattr(socket.socket, 'connect', connect)
    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: (_ for _ in ()).throw(AssertionError(f'network: {a}')))


class _Response(io.BytesIO):
    def __init__(self, body: bytes, headers: dict, status: int = 200):
        super().__init__(body)
        self.headers, self.status = headers, status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class FakeArchive:
    """S3 ListObjectsV2 + HEAD/GET for the Unidata bucket, served from memory. Records every request."""
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.listed_size: dict[str, int] = {}
        self.modified: dict[str, datetime] = {}
        self.list_error: Exception | None = None
        self.truncate: set[str] = set()
        self.head_size: dict[str, int] = {}
        self.requests: list[tuple[str, str]] = []
        self.body_bytes = 0

    def put(self, key: str, data: bytes, modified: datetime | None = None, listed_size: int | None = None):
        self.objects[key] = data
        self.modified[key] = modified or (pilot.scan_time(key) + timedelta(minutes=6, seconds=30))
        if listed_size is not None:
            self.listed_size[key] = listed_size

    def get_requests(self):
        return [u for m, u in self.requests if m == 'GET' and 'list-type' not in u]

    def urlopen(self, request, timeout=None):
        url = request.full_url if isinstance(request, urllib.request.Request) else request
        method = request.get_method() if isinstance(request, urllib.request.Request) else 'GET'
        parts = urllib.parse.urlsplit(url)
        if parts.hostname != BUCKET_HOST or parts.scheme != 'https':
            raise AssertionError(f'unexpected URL {url}')
        self.requests.append((method, url))
        if parts.path == '/' and 'list-type=2' in parts.query:
            if self.list_error:
                raise self.list_error
            prefix = urllib.parse.parse_qs(parts.query)['prefix'][0]
            rows = ''.join(f'<Contents><Key>{k}</Key><Size>{self.listed_size.get(k, len(v))}</Size></Contents>'
                           for k, v in sorted(self.objects.items()) if k.startswith(prefix))
            xml = f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{rows}</ListBucketResult>'
            return _Response(xml.encode(), {})
        key = urllib.parse.unquote(parts.path.lstrip('/'))
        data = self.objects[key]
        etag = '"' + hashlib.md5(data).hexdigest() + '"'
        size = self.head_size.get(key, len(data))
        headers = {'Content-Length': str(size), 'ETag': etag, 'Last-Modified': format_datetime(self.modified[key], usegmt=True)}
        if method == 'HEAD':
            return _Response(b'', headers)
        body = data[: len(data) // 2] if key in self.truncate else data
        self.body_bytes += len(body)
        return _Response(body, headers)


class FakePages:
    """Whole-site atomic swap, like a Pages deployment, behind a simulated CDN.
    fail_next      deploy-pages fails; the previous deployment stays live
    unreachable    every request raises (network error / timeout / 5xx)
    origin_body    the origin serves these bytes instead of the deployment (tampered / foreign index)
    stale_plain    the plain URL keeps serving the previous deployment (CDN not yet refreshed); the cache-busted
                   origin request sees the new one
    unpublished    the whole site answers 404 (e.g. owner unpublished it)"""
    def __init__(self, root: Path):
        self.root, self.live, self.previous, self.fail_next, self.deployments = root, None, None, 0, 0
        self.unreachable = self.stale_plain = self.unpublished = False
        self.origin_body = None

    def deploy(self, site: Path):
        if self.fail_next:
            self.fail_next -= 1
            raise RuntimeError('simulated deploy-pages failure')
        target = self.root / f'deployment-{self.deployments}'
        shutil.copytree(site, target)
        self.previous, self.live, self.deployments = self.live, target, self.deployments + 1

    @staticmethod
    def _index(deployment):
        if deployment is None or not (deployment / 'kinx/index.json').exists():
            return None
        return (deployment / 'kinx/index.json').read_bytes()

    def fetch(self, url, bust=True):
        if self.unreachable:
            raise OSError('simulated Pages outage')
        if self.unpublished:
            return None
        if not bust and self.stale_plain:
            return self._index(self.previous)
        if self.origin_body is not None:
            return self.origin_body
        return self._index(self.live)


class FakeGitHub:
    def __init__(self):
        self.runs: list[dict] = []
        self.jobs: dict[int, list] = {}
        self.unavailable = False
        self.disable_fails = False
        self.disabled = False
        self.calls = 0

    def api(self, path, method='GET'):
        self.calls += 1
        if self.unavailable:
            raise pilot.Uncertain('simulated GitHub API outage')
        if '/jobs' in path:
            rid = int(path.split('/runs/')[1].split('/')[0])
            return {'jobs': self.jobs.get(rid, [])}
        page = int(urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)['page'][0])
        newest_first = sorted(self.runs, key=lambda r: -r['run_number'])
        return {'workflow_runs': newest_first[(page - 1) * 100: page * 100]}

    def new_run(self) -> dict:
        r = {'id': 1000 + len(self.runs), 'run_number': len(self.runs) + 1, 'status': 'in_progress', 'conclusion': None}
        self.runs.append(r)
        return r

    def disable(self):
        if self.disable_fails:
            raise RuntimeError('simulated workflow-disable API failure')
        self.disabled = True


def git(*args, cwd):
    return subprocess.run(['git', *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


@dataclass
class World:
    tmp: Path
    remote: Path
    archive: FakeArchive
    pages: FakePages
    gh: FakeGitHub
    signing: Path
    public_b64: str
    auth: dict
    clock: list = field(default_factory=lambda: [T0 + timedelta(minutes=50)])
    enabled: str = 'true'
    enabled_at_deploy: str | None = None      # value the deploy job sees, if flipped after stage
    deploy_delay: timedelta = timedelta(0)    # clock advance between stage and the deploy job
    runs: int = 0
    produced: list = field(default_factory=list)

    def now(self):
        return self.clock[0]

    def advance(self, minutes=5):
        self.clock[0] += timedelta(minutes=minutes)

    def checkout(self, name: str) -> Path:
        path = self.tmp / 'runners' / name
        if path.exists():
            shutil.rmtree(path)
        git('clone', '--quiet', str(self.remote), str(path), cwd=self.tmp)
        return path

    def remote_main(self) -> str:
        return git('rev-parse', 'main', cwd=self.remote)

    def ledger_totals(self):
        c = self.checkout('inspect')
        return pilot.read_ledger(c, pilot.Authorization.load(c))


def make_world(tmp_path: Path, **auth_overrides) -> World:
    keys = tmp_path / 'keys'
    keys.mkdir(parents=True)
    made = pub.keygen(keys / 'test-publisher.pem')   # test-only identity, outside any repository
    seed = tmp_path / 'seed'
    seed.mkdir()
    git('init', '--quiet', '-b', 'main', cwd=seed)
    (seed / 'site/kinx').mkdir(parents=True)
    (seed / 'site/.nojekyll').write_text('')
    auth = {'format': 'ir023-pilot-authorization', 'version': 1, 'pilotId': 'ir023-sim',
            'notBeforeUtc': pilot.iso(T0), 'expiresUtc': pilot.iso(T0 + timedelta(hours=48)),
            'maxSourceObjects': 600, 'maxSourceBytes': 8 << 30, 'maxRepositoryBytes': 400 << 20,
            'repositoryBaselineBytes': 2 << 20, 'workflowFile': 'publish-kinx-scheduled.yml',
            'allowInitialPublication': True}   # the simulated site starts empty; see the absent-index tests
    auth.update(auth_overrides)
    (seed / 'pilot').mkdir()
    (seed / 'pilot/authorization.json').write_text(json.dumps(auth, indent=2) + '\n')
    git('add', '-A', cwd=seed)
    git('-c', 'user.name=t', '-c', 'user.email=t@t', 'commit', '--quiet', '-m', 'seed', cwd=seed)
    remote = tmp_path / 'origin.git'
    git('clone', '--quiet', '--bare', str(seed), str(remote), cwd=tmp_path)
    (tmp_path / 'pages').mkdir()
    return World(tmp_path, remote, FakeArchive(), FakePages(tmp_path / 'pages'), FakeGitHub(),
                 keys / 'test-publisher.pem', made['publicKey'], auth)


@pytest.fixture
def world(tmp_path, monkeypatch):
    w = make_world(tmp_path)
    install(w, monkeypatch)
    return w


def install(w: World, monkeypatch):
    monkeypatch.setattr(urllib.request, 'urlopen', w.archive.urlopen)

    def fake_produce(key, size, root, discovered):
        """Real bounded transfer (fetch.download through the fake archive), then the preserved IR-015 package for that
        key: the scientific product itself is not re-derived here (see test_real_producer for that)."""
        budget = fetch.TransferBudget(foreground.TRANSFER_LIMIT, journal=root / 'transfer-budget.json')
        path = fetch.download(key, root / 'raw', expected_bytes=size, budget=budget)
        if path.read_bytes()[:4] != b'AR2V':
            raise fetch.IntegrityError('not a Level II volume')
        name = Path(key).name + '.irradar'
        fixture = FIXTURES / name
        if not fixture.exists():
            raise fetch.IntegrityError('volume failed scientific validation (no product)')
        shutil.copyfile(fixture, root / name)
        w.produced.append(key)
        return {'package': name, 'sha256': hashlib.sha256(fixture.read_bytes()).hexdigest(), 'source_key': key}
    w.fake_produce = fake_produce


def level2_bytes(key: str, size: int = 200_000) -> bytes:
    seed = hashlib.sha256(key.encode()).digest()
    return b'AR2V0006.' + (seed * (size // 32 + 1))[: size - 9]


def run_once(w: World, *, produce=None, cancel_after_acquire=False, interrupt=None) -> dict:
    """One scheduled execution, job by job, using the workflow's `needs`/`if` conditions exactly
    (guard -> acquire -> stage -> deploy, and stop); test_workflow_conditions checks the YAML says the same."""
    r = w.gh.new_run()
    jobs, out, cancelled = {}, {'run': r}, False
    try:
        g = pilot.guard(w.checkout(f'guard-{r["id"]}'), now=w.now(), enabled=w.enabled, api=w.gh.api,
                        repo_slug='xBR533/immersive-radar-publication', run_id=r['id'], run_number=r['run_number'])
        jobs['guard'] = 'success'
    except pilot.Failure as exc:
        g, jobs['guard'] = {'acquire': False, 'deploy': False, 'stop': False, 'reason': exc.code}, 'failure'
    out['guard'] = g
    # acquire: if needs.guard.outputs.acquire == 'true'
    if jobs['guard'] == 'success' and g['acquire']:
        try:
            out['acquire'] = pilot.acquire(
                w.checkout(f'acquire-{r["id"]}'), now_fn=w.now, enabled=w.enabled, public_b64=w.public_b64,
                signing_key=w.signing, work=w.tmp / 'work' / str(r['id']), run_id=r['id'], attempt=1,
                produce=interrupt or produce or w.fake_produce)
            jobs['acquire'] = 'success'
        except pilot.Refusal as exc:
            out['acquire'], jobs['acquire'] = {'status': 'refused', 'reason': exc.code}, 'success'
        except pilot.Failure as exc:
            out['acquire'], jobs['acquire'] = {'status': 'failed', 'reason': exc.code}, 'failure'
        except KeyboardInterrupt:
            out['acquire'], jobs['acquire'], cancelled = {'status': 'interrupted'}, 'cancelled', True
        if cancel_after_acquire:
            jobs['acquire'], cancelled = 'cancelled', True
    acquire_result = jobs.get('acquire', 'skipped')
    # stage: !cancelled() && guard success && guard.deploy && acquire.result in (success, failure, skipped)
    stage = None
    if not cancelled and jobs['guard'] == 'success' and g['deploy'] and acquire_result in ('success', 'failure', 'skipped'):
        try:
            stage = pilot.stage(w.checkout(f'stage-{r["id"]}'), public_b64=w.public_b64, enabled=w.enabled, now=w.now(),
                                live_index_url='https://example.invalid/kinx/index.json', fetch=w.pages.fetch)
            jobs['stage'] = 'success'
        except pilot.Refusal as exc:
            stage, jobs['stage'] = {'deploy': False, 'reason': exc.code}, 'success'
        except pilot.Failure as exc:
            stage, jobs['stage'] = {'deploy': False, 'reason': exc.code}, 'failure'
        out['stage'] = stage
    # deploy: !cancelled() && stage success && stage.deploy && vars.PUBLICATION_ENABLED == 'true'
    deploy_enabled = w.enabled if w.enabled_at_deploy is None else w.enabled_at_deploy
    if not cancelled and jobs.get('stage') == 'success' and stage['deploy'] and deploy_enabled == 'true':
        deploy_repo = w.checkout(f'deploy-{r["id"]}')
        try:
            out['preflight'] = pilot.preflight(deploy_repo, enabled=deploy_enabled, now=w.now() + w.deploy_delay)
            w.pages.deploy(w.tmp / 'runners' / f'stage-{r["id"]}' / 'site')
            out['probe'] = pilot.probe('u', stage['committedIndexSHA256'], fetch=w.pages.fetch, timeout_s=0, interval_s=0)
            jobs['deploy'] = 'success' if out['probe']['visible'] else 'failure'
        except pilot.Refusal as exc:
            out['preflight'], jobs['deploy'] = {'refused': exc.code}, 'failure'
        except RuntimeError:
            jobs['deploy'] = 'failure'
    # stop: needs.guard.outputs.stop == 'true'
    if jobs['guard'] == 'success' and g['stop']:
        try:
            w.gh.disable()
            jobs['stop'] = 'success'
        except RuntimeError:
            jobs['stop'] = 'failure'
    states = set(jobs.values())
    r['status'] = 'completed'
    r['conclusion'] = 'cancelled' if cancelled else 'failure' if 'failure' in states else 'success'
    w.gh.jobs[r['id']] = [{'name': n, 'conclusion': c, 'steps': [{'name': 'run'}]} for n, c in jobs.items()]
    out['jobs'], out['conclusion'] = jobs, r['conclusion']
    w.advance(5)
    return out


def live_source(w: World):
    body = w.pages.fetch('x')
    return None if body is None else json.loads(base64.b64decode(json.loads(body)['payload']))['latest']['sourceKey']


def committed_source(w: World):
    c = w.checkout('peek')
    idx = c / 'site/kinx/index.json'
    return None if not idx.exists() else json.loads(base64.b64decode(json.loads(idx.read_bytes())['payload']))['latest']['sourceKey']
