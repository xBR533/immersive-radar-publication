"""IR-023: bounded, unattended KINX publication pilot.

Wraps the unchanged IR-019/IR-020 producer and publication contracts (`immersive_radar_pipeline`, pinned commit)
with the operational controls an unattended schedule needs. Nothing here changes science, packages, the signed
index, or the native client.

Commands (one per workflow job; see .github/workflows/publish-kinx-scheduled.yml):
    guard    stdlib only, no secrets. Kill switch, owner authorization window, cross-run failure accounting
             (GitHub Actions API, fail closed), cumulative source and repository budgets. Decides what may run.
    acquire  at most ONE NOAA object: list once; if newer, push a budget reservation to `main` (compare-and-swap)
             BEFORE any object byte is requested; then produce, sign, prune, verify, and push the publication.
    stage    sync to remote `main`, verify the whole committed site, compare with the live Pages index and
             decide whether to deploy it (recovers a failed deployment without a new acquisition; never deploys
             an older observation over a newer live one).
    probe    after deployment: bounded check that the public index serves exactly the committed bytes.

Hard limits are enforced from files committed on `main` (owner authorization + append-only ledger), never from
mutable runner state, so they survive failed, cancelled and interrupted runs.
"""
from __future__ import annotations
import argparse, base64, hashlib, json, os, re, shutil, subprocess, sys, time, urllib.error, urllib.parse, urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Code ceilings. An authorization file may lower these, never raise them.
MAX_PILOT = timedelta(hours=48)
MAX_SOURCE = 64 << 20                 # IR-019B per-object ceiling, unchanged.
CEILING_OBJECTS = 600
CEILING_SOURCE_BYTES = 8 << 30
CEILING_REPOSITORY_BYTES = 400 << 20
MAX_CONSECUTIVE_FAILURES = 3
KEEP = 8                              # Packages kept on the host = app rolling history (IR-020).
MAX_PACKAGE = 16 << 20                # Native LocalRadarPackage.maximumBytes.
COMMIT_OVERHEAD = 8 << 10             # Commit + tree objects per pilot commit (measured < 2 KiB; see tests).
LEDGER_ENTRY_MAX = 4 << 10
# Worst-case repository bytes one publication can add: package + receipt + index + ledger entries + overhead.
WORST_PUBLICATION_BYTES = MAX_PACKAGE + (64 << 10) + 2 * (LEDGER_ENTRY_MAX + COMMIT_OVERHEAD)
API_MAX_PAGES = 10                    # 10 x 100 runs > 576 scheduled runs in 48 h.
FAILURE_CONCLUSIONS = {'failure', 'timed_out', 'startup_failure'}
NEUTRAL_CONCLUSIONS = {'skipped', 'neutral'}
SITE = Path('site')
KINX = SITE / 'kinx'
AUTHORIZATION = Path('pilot/authorization.json')
LEDGER = Path('pilot/ledger')
ORIGIN_LABEL = 'ir023-pilot'
SCAN_RE = re.compile(r'KINX(\d{8}_\d{6})_V\d{2}')


class Refusal(Exception):
    """A clean, deliberate decision not to proceed. Not an error of the run."""
    def __init__(self, code: str, detail: str = '', stop: bool = False):
        super().__init__(f'{code}: {detail}' if detail else code)
        self.code, self.detail, self.stop = code, detail, stop


class Failure(Exception):
    """The run must fail (it counts toward the consecutive-failure stop)."""
    def __init__(self, code: str, detail: str = ''):
        super().__init__(f'{code}: {detail}' if detail else code)
        self.code, self.detail = code, detail


class Uncertain(Failure):
    """Failure accounting could not be established. Never authorizes acquisition."""
    def __init__(self, detail: str):
        super().__init__('failure-accounting-uncertain', detail)


def utc(value: str) -> datetime:
    t = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if t.tzinfo is None:
        raise ValueError('timestamp lacks UTC offset')
    return t.astimezone(timezone.utc)


def iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).isoformat()


def scan_time(source_key: str) -> datetime:
    m = SCAN_RE.search(source_key)
    if not m:
        raise Failure('unrecognised-source-key', source_key)
    return datetime.strptime(m[1], '%Y%m%d_%H%M%S').replace(tzinfo=timezone.utc)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------- authorization and ledger (committed state)

@dataclass(frozen=True)
class Authorization:
    pilot_id: str
    not_before: datetime
    expires: datetime
    max_objects: int
    max_source_bytes: int
    max_repository_bytes: int
    repository_baseline_bytes: int
    workflow_file: str

    @staticmethod
    def load(repo: Path) -> 'Authorization':
        path = repo / AUTHORIZATION
        try:
            a = json.loads(path.read_bytes())
            auth = Authorization(
                pilot_id=a['pilotId'], not_before=utc(a['notBeforeUtc']), expires=utc(a['expiresUtc']),
                max_objects=int(a['maxSourceObjects']), max_source_bytes=int(a['maxSourceBytes']),
                max_repository_bytes=int(a['maxRepositoryBytes']),
                repository_baseline_bytes=int(a['repositoryBaselineBytes']), workflow_file=a['workflowFile'])
            ok = (a.get('format'), a.get('version')) == ('ir023-pilot-authorization', 1)
        except FileNotFoundError:
            raise Refusal('authorization-missing', str(AUTHORIZATION), stop=True)
        except (ValueError, KeyError, TypeError) as exc:
            raise Refusal('authorization-invalid', str(exc), stop=True)
        problems = []
        if not ok:
            problems.append('format/version')
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]{2,40}', auth.pilot_id):
            problems.append('pilotId')
        if not auth.not_before < auth.expires or auth.expires - auth.not_before > MAX_PILOT:
            problems.append('window must be positive and at most 48 h')
        if not 0 < auth.max_objects <= CEILING_OBJECTS:
            problems.append('maxSourceObjects')
        if not MAX_SOURCE <= auth.max_source_bytes <= CEILING_SOURCE_BYTES:
            problems.append('maxSourceBytes')
        if not 0 <= auth.repository_baseline_bytes < auth.max_repository_bytes <= CEILING_REPOSITORY_BYTES:
            problems.append('repository bytes')
        if not re.fullmatch(r'[a-z0-9-]+\.yml', auth.workflow_file):
            problems.append('workflowFile')
        if problems:
            raise Refusal('authorization-invalid', ', '.join(problems), stop=True)
        return auth


@dataclass
class Totals:
    objects: int = 0
    source_bytes: int = 0
    repository_bytes: int = 0
    publications: int = 0
    entries: list = field(default_factory=list)


def ledger_dir(repo: Path, auth: Authorization) -> Path:
    return repo / LEDGER / auth.pilot_id


def read_ledger(repo: Path, auth: Authorization) -> Totals:
    """Append-only ledger: one small file per event, sharded by hour so no tree grows without bound.
    Totals are recomputed from every entry on every run; nothing is trusted from a running counter."""
    t = Totals(repository_bytes=auth.repository_baseline_bytes)
    root = ledger_dir(repo, auth)
    for path in sorted(root.rglob('*.json')) if root.exists() else []:
        try:
            e = json.loads(path.read_bytes())
            kind, git_bytes = e['kind'], int(e['gitBytes'])
            if e['pilotId'] != auth.pilot_id or git_bytes <= 0:
                raise ValueError('foreign or malformed entry')
            if kind == 'reserve':
                size = int(e['sourceBytes'])
                if not 0 < size <= MAX_SOURCE:
                    raise ValueError('reservation size')
                t.objects += 1
                t.source_bytes += size
            elif kind == 'publish':
                t.publications += 1
            else:
                raise ValueError('kind')
        except (ValueError, KeyError, TypeError) as exc:
            # A ledger that cannot be read cannot prove the budget: refuse, do not guess.
            raise Refusal('ledger-invalid', f'{path.relative_to(repo)}: {exc}', stop=True)
        t.repository_bytes += git_bytes
        t.entries.append(e)
    return t


def check_window(auth: Authorization, now: datetime):
    if now < auth.not_before:
        raise Refusal('not-yet-authorized', iso(auth.not_before))
    if now >= auth.expires:
        raise Refusal('expired', iso(auth.expires), stop=True)


def check_kill_switch(value: str | None):
    if value != 'true':
        raise Refusal('kill-switch-off', 'PUBLICATION_ENABLED is not exactly "true"')


def acquisition_headroom(auth: Authorization, totals: Totals, next_source_bytes: int = MAX_SOURCE):
    """Raises unless one more acquisition of `next_source_bytes` (worst case by default) and one more worst-case
    publication fit every hard limit."""
    if totals.objects + 1 > auth.max_objects:
        raise Refusal('source-object-budget-exhausted', f'{totals.objects}/{auth.max_objects}', stop=True)
    if totals.source_bytes + next_source_bytes > auth.max_source_bytes:
        raise Refusal('source-byte-budget-exhausted', f'{totals.source_bytes}+{next_source_bytes}>{auth.max_source_bytes}', stop=True)
    if totals.repository_bytes + WORST_PUBLICATION_BYTES > auth.max_repository_bytes:
        raise Refusal('repository-budget-exhausted', f'{totals.repository_bytes}+{WORST_PUBLICATION_BYTES}>{auth.max_repository_bytes}', stop=True)


# ---------------------------------------------------------------- cross-run failure accounting

def consecutive_failures(api, repo_slug: str, workflow_file: str, run_id: int, run_number: int,
                         since: datetime, jobs_name: str = 'publish', limit: int = MAX_CONSECUTIVE_FAILURES) -> int:
    """Counts failed executions since the last healthy one, newest first, within this authorization window.

    healthy  run concluded success AND its `publish` job concluded success (a no-newer listing is healthy)
    neutral  run concluded success/skipped/neutral with `publish` skipped (kill switch off, refusals); does not reset
    failure  failure, timed_out, startup_failure; cancelled once any job had started (an interruption is not health)
    superseded  cancelled before any job started (a pending run replaced in the concurrency group): neutral
    anything else (incomplete earlier run, unknown conclusion, malformed or unavailable API) -> Uncertain
    """
    created = urllib.parse.quote('>=' + since.strftime('%Y-%m-%dT%H:%M:%SZ'))
    count = 0
    for page in range(1, API_MAX_PAGES + 1):
        data = api(f'/repos/{repo_slug}/actions/workflows/{workflow_file}/runs?per_page=100&page={page}&created={created}')
        runs = data.get('workflow_runs') if isinstance(data, dict) else None
        if not isinstance(runs, list):
            raise Uncertain('malformed workflow-runs response')
        for r in runs:
            try:
                rid, number, status, conclusion = int(r['id']), int(r['run_number']), r['status'], r['conclusion']
            except (KeyError, TypeError, ValueError):
                raise Uncertain('malformed run record')
            if rid == run_id or number >= run_number:
                continue  # this run, or a newer queued one
            if status != 'completed':
                raise Uncertain(f'earlier run {rid} is {status}')
            if conclusion in FAILURE_CONCLUSIONS:
                count += 1
                if count >= limit:
                    return count
            elif conclusion == 'cancelled':
                listed = _jobs(api, repo_slug, rid)
                if any(j.get('steps') or j.get('conclusion') not in ('cancelled', 'skipped', None) for j in listed):
                    count += 1
                    if count >= limit:
                        return count
            elif conclusion == 'success':
                listed = _jobs(api, repo_slug, rid)
                publish = [j for j in listed if j.get('name') == jobs_name]
                state = publish[0].get('conclusion') if publish else 'skipped'
                if state == 'success':
                    return count
                if state != 'skipped':
                    raise Uncertain(f'run {rid} succeeded but publish job is {state}')
            elif conclusion in NEUTRAL_CONCLUSIONS:
                continue
            else:
                raise Uncertain(f'run {rid} has unclassified conclusion {conclusion!r}')
        if len(runs) < 100:
            return count
    raise Uncertain('lookback exhausted before a healthy run or the authorization start')


def _jobs(api, repo_slug: str, rid: int) -> list:
    jobs = api(f'/repos/{repo_slug}/actions/runs/{rid}/jobs?filter=latest&per_page=100')
    listed = jobs.get('jobs') if isinstance(jobs, dict) else None
    if not isinstance(listed, list) or not all(isinstance(j, dict) for j in listed):
        raise Uncertain(f'malformed jobs response for run {rid}')
    return listed


def github_api(token: str, base: str = 'https://api.github.com', timeout: float = 15):
    def call(path: str, method: str = 'GET'):
        request = urllib.request.Request(base + path, method=method, headers={
            'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'ir023-pilot-guard'})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as r:
                body = r.read(4 << 20)
            return json.loads(body) if body else {}
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise Uncertain(f'GitHub API {method} {path.split("?")[0]}: {exc}')
    return call


# ---------------------------------------------------------------- guard

def guard(repo: Path, *, now: datetime, enabled: str | None, api, repo_slug: str, run_id: int, run_number: int) -> dict:
    """Decision for this execution. Every refusal path sets acquire=False. `stop` asks the separate, isolated job to
    disable the workflow; the refusals themselves do not depend on that call succeeding."""
    decision = {'acquire': False, 'deploy': False, 'stop': False, 'reason': '', 'checkedUtc': iso(now)}
    try:
        check_kill_switch(enabled)
        auth = Authorization.load(repo)
        check_window(auth, now)
        failures = consecutive_failures(api, repo_slug, auth.workflow_file, run_id, run_number, auth.not_before)
        decision['consecutiveFailures'] = failures
        if failures >= MAX_CONSECUTIVE_FAILURES:
            raise Refusal('consecutive-failures', f'{failures} consecutive failed executions', stop=True)
        totals = read_ledger(repo, auth)
        decision.update(objects=totals.objects, sourceBytes=totals.source_bytes, repositoryBytes=totals.repository_bytes)
        try:
            acquisition_headroom(auth, totals)
        except Refusal as r:
            decision.update(deploy=True, stop=True, reason=r.code)  # committed work may still be recovered
            return decision
        decision.update(acquire=True, deploy=True, reason='authorized')
        return decision
    except Refusal as r:
        decision.update(stop=r.stop, reason=r.code, detail=r.detail)
        return decision


# ---------------------------------------------------------------- git (compare-and-swap on main)

class Git:
    def __init__(self, repo: Path, remote: str = 'origin', branch: str = 'main'):
        self.repo, self.remote, self.branch = repo, remote, branch

    def run(self, *args, check=True) -> str:
        p = subprocess.run(['git', '-C', str(self.repo), *args], capture_output=True, text=True)
        if check and p.returncode:
            raise Failure('git', f'git {" ".join(args)}: {p.stderr.strip()[-400:]}')
        return p.stdout.strip()

    def head(self) -> str:
        return self.run('rev-parse', 'HEAD')

    def staged_blob_bytes(self) -> int:
        """Bytes of every added or modified blob in the index: an upper bound (plus a fixed per-commit allowance) on
        what this commit adds to the repository, independent of GitHub's delayed size estimate."""
        total = 0
        for line in self.run('diff', '--cached', '--name-status', '--no-renames').splitlines():
            status, path = line.split('\t', 1)
            if status[0] in 'AM':
                total += int(self.run('cat-file', '-s', f':{path}'))
        return total

    def commit_and_push(self, message: str):
        self.run('-c', 'user.name=github-actions[bot]',
                 '-c', 'user.email=41898282+github-actions[bot]@users.noreply.github.com',
                 'commit', '--quiet', '-m', message)
        p = subprocess.run(['git', '-C', str(self.repo), 'push', '--quiet', self.remote, f'HEAD:refs/heads/{self.branch}'],
                           capture_output=True, text=True)
        if p.returncode:  # Never forced: a moved `main` means another writer won.
            raise Failure('push-rejected', p.stderr.strip()[-400:])

    def sync(self):
        self.run('fetch', '--quiet', self.remote, self.branch)
        self.run('reset', '--quiet', '--hard', 'FETCH_HEAD')
        self.run('clean', '-q', '-fdx', '--', str(SITE), str(LEDGER))


def ledger_entry(repo: Path, git: Git, auth: Authorization, now: datetime, run_id: int, attempt: int, kind: str,
                 body: dict) -> dict:
    entry = {'pilotId': auth.pilot_id, 'kind': kind, 'runId': run_id, 'runAttempt': attempt, 'recordedUtc': iso(now), **body}
    path = ledger_dir(repo, auth) / now.strftime('%Y%m%d%H') / f'{now.strftime("%Y%m%dT%H%M%S")}-{run_id}-{attempt}-{kind}.json'
    if path.exists():
        raise Failure('ledger-collision', str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entry, sort_keys=True) + '\n')  # provisional, to stage and measure
    git.run('add', '--', str(path.relative_to(repo)))
    entry['gitBytes'] = git.staged_blob_bytes() - path.stat().st_size + LEDGER_ENTRY_MAX + COMMIT_OVERHEAD
    data = json.dumps(entry, sort_keys=True).encode() + b'\n'
    if len(data) > LEDGER_ENTRY_MAX:
        raise Failure('ledger-entry-too-large', str(len(data)))
    path.write_bytes(data)
    git.run('add', '--', str(path.relative_to(repo)))
    return entry


# ---------------------------------------------------------------- site verification (uses the unchanged contracts)

def load_public(public_b64: str) -> bytes:
    raw = base64.b64decode(public_b64, validate=True)
    if len(raw) != 32:
        raise Failure('public-key-invalid')
    return raw


def verify_site(site: Path, public: bytes) -> dict:
    """publish.verify (signature, latest package, index/package agreement) plus every retained package's content
    address and receipt pairing, so a deployment never carries an orphaned or altered file."""
    from immersive_radar_pipeline import publish as pub
    from immersive_radar_pipeline.fetch import IntegrityError
    try:
        index = pub.verify(site, public)
    except (IntegrityError, OSError, KeyError, ValueError) as exc:
        raise Failure('committed-site-invalid', str(exc))
    packages = {p.stem for p in (site / 'packages').glob('*.irradar')}
    receipts = {p.stem for p in (site / 'receipts').glob('*.json')}
    if packages != receipts:
        raise Failure('committed-site-invalid', 'packages and receipts do not pair')
    if len(packages) > KEEP:
        raise Failure('committed-site-invalid', f'{len(packages)} packages exceeds {KEEP}')
    for digest in packages:
        data = (site / 'packages' / f'{digest}.irradar').read_bytes()
        receipt = json.loads((site / 'receipts' / f'{digest}.json').read_bytes())
        if sha256(data) != digest or receipt.get('sha256') != digest or receipt.get('bytes') != len(data):
            raise Failure('committed-site-invalid', f'content address mismatch for {digest}')
    return index


def check_signing_key(path: Path, public: bytes):
    """Before any archive traffic: the key must exist, be Ed25519, and match the configured public key."""
    from immersive_radar_pipeline import publish as pub
    from immersive_radar_pipeline.fetch import IntegrityError
    try:
        key = pub._private_key(path)
    except (OSError, ValueError, TypeError, IntegrityError) as exc:
        raise Failure('signing-key-unavailable', type(exc).__name__)
    if pub.raw_public(key) != public:
        raise Failure('signing-key-mismatch', 'private key does not match the configured public key')


# ---------------------------------------------------------------- acquire (at most one NOAA object)

def acquire(repo: Path, *, now_fn, enabled: str | None, public_b64: str, signing_key: Path, work: Path,
            run_id: int, attempt: int, produce=None, candidate=None) -> dict:
    from immersive_radar_pipeline import publish as pub, foreground, snapshot
    produce = produce or foreground.produce
    candidate = candidate or snapshot.latest_candidate
    git = Git(repo)
    now = now_fn()
    check_kill_switch(enabled)
    auth = Authorization.load(repo)
    check_window(auth, now)
    totals = read_ledger(repo, auth)
    acquisition_headroom(auth, totals)
    public = load_public(public_b64)
    check_signing_key(signing_key, public)
    site = repo / KINX
    prior = verify_site(site, public)['latest'] if (site / 'index.json').exists() else None
    base = git.head()
    listed = now_fn()
    try:
        key, size = candidate('KINX', listed)
    except Exception as exc:  # listing failures of every kind: no object requested
        raise Failure('archive-unavailable', f'{type(exc).__name__}: {exc}')
    if prior and scan_time(key) <= scan_time(prior['sourceKey']):
        return {'status': 'no-newer', 'candidate': key, 'published': prior['sourceKey'], 'sourceBytesRequested': 0}
    if not 0 < size <= MAX_SOURCE:
        raise Failure('source-oversized', f'{key} {size}')
    acquisition_headroom(auth, totals, size)

    # 1. Reservation: committed and pushed before the first object byte is requested.
    try:
        reserved = ledger_entry(repo, git, auth, now_fn(), run_id, attempt, 'reserve',
                                {'sourceKey': key, 'sourceBytes': size, 'listedUtc': iso(listed)})
        git.commit_and_push(f'IR-023 pilot: reserve {size} source bytes for {Path(key).name} (run {run_id})')
    except BaseException:
        git.run('reset', '--quiet', '--hard', base)
        raise
    reserved_head = git.head()

    # 2. One object through the unchanged producer and publication contract.
    try:
        work.mkdir(parents=True, exist_ok=False)
        item = produce(key, size, work, iso(listed))
        receipt = pub.publish(work / item['package'], site, signing_key, origin=ORIGIN_LABEL)
        pruned = pub.prune(site, KEEP)
        verify_site(site, public)
        manifest = next(work.glob('product-*/delivery-manifest.json'), None)
        detail = json.loads(manifest.read_text()) if manifest else {}
        acquisition = next(work.glob('product-*/acquisition.json'), None)
        acq = json.loads(acquisition.read_text()) if acquisition else {}
        git.run('add', '-A', '--', str(SITE))
        published = ledger_entry(repo, git, auth, now_fn(), run_id, attempt, 'publish', {
            'sourceKey': key, 'packageSHA256': receipt['sha256'], 'packageBytes': receipt['bytes'], 'pruned': pruned,
            'freshness': {
                'observationStartUtc': receipt['observationStartUtc'], 'observationEndUtc': receipt['observationEndUtc'],
                'archiveLastModifiedUtc': detail.get('archive_last_modified_utc'),
                'listedUtc': iso(listed), 'receivedUtc': acq.get('received_utc'), 'processedUtc': receipt['processedUtc'],
                'publishedUtc': receipt['publishedUtc']}})
        git.commit_and_push(f'IR-023 pilot: publish {Path(key).name} (run {run_id})')
    except BaseException as exc:  # incl. interruption: the reservation stays charged; nothing else is kept
        git.run('reset', '--quiet', '--hard', reserved_head)
        git.run('clean', '-q', '-fdx', '--', str(SITE))
        if isinstance(exc, (Failure, Refusal)) or not isinstance(exc, Exception):
            raise
        raise Failure('acquisition-or-production-failed', f'{type(exc).__name__}: {exc}') from exc
    return {'status': 'published', 'sourceKey': key, 'sourceBytesReserved': size, 'reservation': reserved,
            'publication': published, 'commit': git.head()}


# ---------------------------------------------------------------- stage (deploy decision, incl. recovery)

def observation_newer(a: dict, b: dict) -> bool:
    return utc(a['observationStartUtc']) > utc(b['observationStartUtc']) and utc(a['observationEndUtc']) > utc(b['observationEndUtc'])


def fetch_url(url: str, limit: int = 64 << 10, timeout: float = 15, bust: bool = True) -> bytes | None:
    """Live public index; by default bypassing the CDN cache key. None for 404 (no site yet)."""
    if bust:
        url = f"{url}{'&' if '?' in url else '?'}ir023={time.time_ns()}"
    request = urllib.request.Request(url, headers={'Cache-Control': 'no-cache', 'User-Agent': 'ir023-pilot'})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as r:
            return r.read(limit + 1)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def stage(repo: Path, *, public_b64: str, live_index_url: str, fetch=fetch_url, sync: bool = True) -> dict:
    from immersive_radar_pipeline import publish as pub
    from immersive_radar_pipeline.fetch import IntegrityError
    if sync:
        Git(repo).sync()  # deploy exactly what `main` holds, never a partial working tree
    site = repo / KINX
    if not (site / 'index.json').exists():
        return {'deploy': False, 'reason': 'nothing-committed'}
    public = load_public(public_b64)
    committed = verify_site(site, public)
    committed_bytes = (site / 'index.json').read_bytes()
    try:
        live_bytes = fetch(live_index_url)
    except Exception as exc:
        live_bytes, unreachable = None, f'{type(exc).__name__}: {exc}'
    else:
        unreachable = None
    result = {'committedIndexSHA256': sha256(committed_bytes), 'committedSourceKey': committed['latest']['sourceKey']}
    if live_bytes == committed_bytes:
        return {**result, 'deploy': False, 'reason': 'live-matches-committed'}
    live = None
    if live_bytes is not None:
        try:
            live = pub.open_index(live_bytes, public)
        except (IntegrityError, ValueError, KeyError):
            live = None
    if live is not None:
        result['liveSourceKey'] = live['latest']['sourceKey']
        if observation_newer(live['latest'], committed['latest']):
            raise Failure('live-newer-than-committed', 'refusing to deploy an older observation over a newer one')
        if live['latest']['sourceSHA256'] == committed['latest']['sourceSHA256']:
            return {**result, 'deploy': False, 'reason': 'live-same-observation'}
        return {**result, 'deploy': True, 'reason': 'committed-newer-than-live'}
    return {**result, 'deploy': True, 'reason': 'live-unreachable' if unreachable else 'live-missing-or-unverifiable',
            **({'liveError': unreachable} if unreachable else {})}


def probe(url: str, expected_sha256: str, *, fetch=fetch_url, timeout_s: float = 90, interval_s: float = 10,
          clock=time.monotonic, sleep=time.sleep) -> dict:
    start = clock()
    while True:
        try:
            body = fetch(url)
            seen = sha256(body) if body is not None else None
        except Exception as exc:
            seen = f'error: {type(exc).__name__}'
        if seen == expected_sha256:
            result = {'visible': True, 'afterSeconds': round(clock() - start, 1), 'visibleUtc': iso(datetime.now(timezone.utc))}
            try:  # what the headset requests: the plain URL, which a CDN may still hold for up to max-age
                plain = fetch(url, bust=False)
                result['plainURLServesCommitted'] = plain is not None and sha256(plain) == expected_sha256
            except Exception as exc:
                result['plainURLServesCommitted'] = f'error: {type(exc).__name__}'
            return result
        if clock() - start + interval_s > timeout_s:
            return {'visible': False, 'lastSeen': seen, 'afterSeconds': round(clock() - start, 1)}
        sleep(interval_s)


# ---------------------------------------------------------------- CLI

def _emit(result: dict, outputs: dict):
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as f:
            f.write('```json\n' + json.dumps(result, indent=2, sort_keys=True, default=str) + '\n```\n')
    target = os.environ.get('GITHUB_OUTPUT')
    if target:
        with open(target, 'a') as f:
            for k, v in outputs.items():
                f.write(f'{k}={str(v).lower() if isinstance(v, bool) else v}\n')


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('command', choices=['guard', 'acquire', 'stage', 'probe'])
    p.add_argument('--repo', type=Path, default=Path('.'))
    p.add_argument('--work', type=Path)
    p.add_argument('--signing-key', type=Path)
    p.add_argument('--url')
    p.add_argument('--expect-sha256')
    a = p.parse_args(argv)
    env = os.environ
    now = lambda: datetime.now(timezone.utc)
    try:
        if a.command == 'guard':
            api = github_api(env.get('GH_TOKEN', ''))
            d = guard(a.repo, now=now(), enabled=env.get('PUBLICATION_ENABLED'), api=api, repo_slug=env['GITHUB_REPOSITORY'],
                      run_id=int(env['GITHUB_RUN_ID']), run_number=int(env['GITHUB_RUN_NUMBER']))
            _emit(d, {'acquire': d['acquire'], 'deploy': d['deploy'], 'stop': d['stop'], 'reason': d['reason']})
        elif a.command == 'acquire':
            r = acquire(a.repo, now_fn=now, enabled=env.get('PUBLICATION_ENABLED'), public_b64=env['PUBLIC_KEY'],
                        signing_key=a.signing_key, work=a.work, run_id=int(env['GITHUB_RUN_ID']),
                        attempt=int(env.get('GITHUB_RUN_ATTEMPT', '1')))
            _emit(r, {'status': r['status']})
        elif a.command == 'stage':
            r = stage(a.repo, public_b64=env['PUBLIC_KEY'], live_index_url=a.url)
            _emit(r, {'deploy': r['deploy'], 'index_sha256': r.get('committedIndexSHA256', '')})
        else:
            r = probe(a.url, a.expect_sha256)
            _emit(r, {'visible': r['visible']})
            if not r['visible']:
                print(f'::warning::Deployed index not yet served at {a.url} after {r["afterSeconds"]} s')
        return 0
    except Refusal as r:
        _emit({'status': 'refused', 'reason': r.code, 'detail': r.detail, 'stop': r.stop}, {'status': 'refused'})
        return 0
    except Failure as f:
        _emit({'status': 'failed', 'reason': f.code, 'detail': f.detail}, {'status': 'failed'})
        print(f'::error::{f.code}: {f.detail}')
        return 1
    except Exception as exc:  # anything unforeseen fails the run (and counts toward the stop)
        _emit({'status': 'failed', 'reason': 'unexpected', 'detail': f'{type(exc).__name__}: {exc}'}, {'status': 'failed'})
        print(f'::error::unexpected: {type(exc).__name__}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
