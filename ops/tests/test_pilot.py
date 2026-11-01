"""IR-023 Gate 2 failure simulations. Offline: fake archive, local bare Git remote, simulated Pages, fake Actions API."""
import base64, json, re, subprocess
from datetime import timedelta
from pathlib import Path
import pytest
from conftest import (KEYS, T0, FIXTURES, PUBLICATION, World, committed_source, git, install, level2_bytes, live_source,
                      make_world, run_once)
import pilot
from immersive_radar_pipeline import publish as pub

MiB = 1 << 20


def put(w: World, *keys, size=200_000):
    for k in keys:
        w.archive.put(k, level2_bytes(k, size))


def totals(w):
    return w.ledger_totals()


# ------------------------------------------------------------------ the normal path

def test_one_valid_newer_observation_is_reserved_published_deployed_and_visible(world):
    put(world, KEYS[0])
    out = run_once(world)
    assert out['guard']['acquire'] and out['acquire']['status'] == 'published'
    assert out['conclusion'] == 'success' and out['jobs'] == {'guard': 'success', 'publish': 'success', 'deploy': 'success'}
    assert committed_source(world) == live_source(world) == KEYS[0]
    assert out['probe']['visible'] is True
    t = totals(world)
    assert (t.objects, t.source_bytes, t.publications) == (1, 200_000, 1)
    assert len(world.archive.get_requests()) == 1 and world.archive.body_bytes == 200_000
    # Reservation commit precedes the publication commit on main.
    log = git('log', '--format=%s', 'main', cwd=world.remote).splitlines()
    assert log[0].startswith('IR-023 pilot: publish') and log[1].startswith('IR-023 pilot: reserve 200000')
    site = world.checkout('verify') / 'site/kinx'
    assert pub.verify(site, base64.b64decode(world.public_b64))['provenance']['publisher'] == 'ir023-pilot'


def test_no_newer_observation_downloads_nothing_and_writes_nothing(world):
    put(world, KEYS[0])
    run_once(world)
    head, gets = world.remote_main(), len(world.archive.get_requests())
    out = run_once(world)
    assert out['acquire']['status'] == 'no-newer' and out['acquire']['sourceBytesRequested'] == 0
    assert out['stage']['reason'] == 'live-matches-committed' and 'deploy' not in out['jobs']
    assert world.remote_main() == head and len(world.archive.get_requests()) == gets
    assert out['conclusion'] == 'success'


def test_successive_scans_publish_in_order_and_prune_to_eight(world):
    for k in KEYS:
        put(world, k)
        out = run_once(world)
        assert out['acquire']['status'] == 'published', out
    assert live_source(world) == KEYS[-1] and totals(world).publications == 5
    assert len(list((world.checkout('c') / 'site/kinx/packages').glob('*.irradar'))) == 5


# ------------------------------------------------------------------ older / duplicate / scientific failures

def test_duplicate_or_older_archive_candidate_never_replaces_newer(world):
    put(world, KEYS[2])
    run_once(world)
    world.archive.objects.clear()
    put(world, KEYS[0], KEYS[1])                     # archive now only offers older scans
    out = run_once(world)
    assert out['acquire']['status'] == 'no-newer' and committed_source(world) == live_source(world) == KEYS[2]
    assert totals(world).objects == 1


def test_publish_contract_itself_refuses_an_older_package(world, tmp_path):
    site = tmp_path / 's'
    pub.publish(FIXTURES / 'KINX20261030_153250_V06.irradar', site, world.signing, origin='t')
    with pytest.raises(Exception, match='not newer'):
        pub.publish(FIXTURES / 'KINX20261030_152702_V06.irradar', site, world.signing, origin='t')


def test_live_newer_than_committed_is_never_overwritten(world, tmp_path, monkeypatch):
    put(world, KEYS[3])
    run_once(world)                                   # live = KEYS[3]
    live_before = world.pages.live
    # A second publication history that only reaches an older scan (e.g. a mistaken reset of main), same Pages site.
    other = make_world(tmp_path / 'other')
    other.signing, other.public_b64, other.pages = world.signing, world.public_b64, world.pages
    install(other, monkeypatch)
    put(other, KEYS[1])
    out = run_once(other)
    assert out['acquire']['status'] == 'published' and committed_source(other) == KEYS[1]
    assert out['stage']['reason'] == 'live-newer-than-committed' and out['jobs']['publish'] == 'failure'
    assert 'deploy' not in out['jobs'] and world.pages.live == live_before and live_source(world) == KEYS[3]


def test_partial_download_fails_with_reservation_charged_and_nothing_published(world):
    put(world, KEYS[0])
    world.archive.truncate.add(KEYS[0])
    out = run_once(world)
    assert out['acquire'] == {'status': 'failed', 'reason': 'acquisition-or-production-failed'}
    assert committed_source(world) is None and live_source(world) is None and out['conclusion'] == 'failure'
    t = totals(world)
    assert (t.objects, t.source_bytes, t.publications) == (1, 200_000, 0)
    assert world.archive.body_bytes <= t.source_bytes


def test_oversized_objects_are_never_requested(world):
    put(world, KEYS[0])
    run_once(world)
    world.archive.put(KEYS[1], b'AR2V' + b'x' * 100, listed_size=65 * MiB)   # listing claims > 64 MiB
    out = run_once(world)
    assert out['acquire']['status'] == 'no-newer'      # filtered by the unchanged candidate rule
    assert not any(KEYS[1].rsplit('/', 1)[1] in u for _, u in world.archive.requests if 'list-type' not in u)


def test_object_larger_at_download_than_listed_fails_before_any_body(world):
    put(world, KEYS[0])
    world.archive.head_size[KEYS[0]] = 70 * MiB
    out = run_once(world)
    assert out['acquire']['reason'] == 'acquisition-or-production-failed' and world.archive.body_bytes == 0


def test_corrupt_source_data_fails_closed(world):
    world.archive.put(KEYS[0], b'GARBAGE!' * 1000)
    out = run_once(world)
    assert out['acquire']['status'] == 'failed' and committed_source(world) is None
    assert totals(world).objects == 1


def test_scientific_validation_failure_publishes_nothing(world):
    put(world, KEYS[0])

    def bad_product(key, size, root, discovered):
        r = world.fake_produce(key, size, root, discovered)
        p = json.loads((root / r['package']).read_text())
        p['descriptor']['numericalPolicyID'] = 'irvol-v1-altered'
        (root / r['package']).write_text(json.dumps(p))
        return r
    out = run_once(world, produce=bad_product)
    assert out['acquire']['status'] == 'failed' and committed_source(world) is None and live_source(world) is None


# ------------------------------------------------------------------ credentials

@pytest.mark.parametrize('damage', ['missing', 'empty', 'wrong-key', 'not-ed25519'])
def test_missing_or_invalid_signing_material_fails_before_any_archive_request(world, tmp_path, damage):
    put(world, KEYS[0])
    bad = tmp_path / 'bad.pem'
    if damage == 'empty':
        bad.write_text('\n')
    elif damage == 'wrong-key':
        pub.keygen(bad)
    elif damage == 'not-ed25519':
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.hazmat.primitives import serialization
        bad.write_bytes(ec.generate_private_key(ec.SECP256R1()).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    world.signing = bad
    out = run_once(world)
    assert out['acquire']['status'] == 'failed'
    assert out['acquire']['reason'] in ('signing-key-unavailable', 'signing-key-mismatch')
    assert world.archive.requests == [] and totals(world).objects == 0


# ------------------------------------------------------------------ git conflicts and concurrency

def test_rejected_reservation_push_means_no_download(world):
    put(world, KEYS[0])
    p = world.checkout('slow-publisher')
    other = world.checkout('other-writer')
    (other / 'README.md').write_text('moved\n')
    git('add', '-A', cwd=other)
    git('-c', 'user.name=o', '-c', 'user.email=o@o', 'commit', '--quiet', '-m', 'concurrent', cwd=other)
    git('push', '--quiet', 'origin', 'main', cwd=other)
    with pytest.raises(pilot.Failure, match='push-rejected'):
        pilot.acquire(p, now_fn=world.now, enabled='true', public_b64=world.public_b64, signing_key=world.signing,
                      work=world.tmp / 'w1', run_id=1, attempt=1, produce=world.fake_produce)
    assert world.archive.get_requests() == [] and totals(world).objects == 0
    assert git('status', '--porcelain', '--', 'site', 'pilot', cwd=p) == ''


def test_concurrent_publishers_only_one_publication_lands(world):
    put(world, KEYS[0])
    a, b = world.checkout('a'), world.checkout('b')
    first = pilot.acquire(a, now_fn=world.now, enabled='true', public_b64=world.public_b64, signing_key=world.signing,
                          work=world.tmp / 'wa', run_id=1, attempt=1, produce=world.fake_produce)
    assert first['status'] == 'published'
    with pytest.raises(pilot.Failure, match='push-rejected'):   # b decided on stale main: its reservation is refused
        pilot.acquire(b, now_fn=world.now, enabled='true', public_b64=world.public_b64, signing_key=world.signing,
                      work=world.tmp / 'wb', run_id=2, attempt=1, produce=world.fake_produce)
    assert len(world.archive.get_requests()) == 1 and totals(world).publications == 1


def test_publication_push_rejected_after_download_discards_it_but_keeps_the_charge(world):
    put(world, KEYS[0])
    a = world.checkout('a')

    def produce_then_race(key, size, root, discovered):
        r = world.fake_produce(key, size, root, discovered)
        o = world.checkout('racer')
        (o / 'README.md').write_text('race\n')
        git('add', '-A', cwd=o)
        git('-c', 'user.name=o', '-c', 'user.email=o@o', 'commit', '--quiet', '-m', 'race', cwd=o)
        git('push', '--quiet', 'origin', 'main', cwd=o)
        return r
    with pytest.raises(pilot.Failure, match='push-rejected'):
        pilot.acquire(a, now_fn=world.now, enabled='true', public_b64=world.public_b64, signing_key=world.signing,
                      work=world.tmp / 'wa', run_id=1, attempt=1, produce=produce_then_race)
    t = totals(world)
    assert (t.objects, t.publications) == (1, 0) and committed_source(world) is None


# ------------------------------------------------------------------ Pages failures and recovery

def test_failed_deployment_is_recovered_from_git_without_a_new_volume(world):
    put(world, KEYS[0])
    run_once(world)
    put(world, KEYS[1])
    world.pages.fail_next = 1
    out = run_once(world)
    assert out['acquire']['status'] == 'published' and out['jobs']['deploy'] == 'failure'
    assert committed_source(world) == KEYS[1] and live_source(world) == KEYS[0]    # last valid stays live
    gets = len(world.archive.get_requests())
    out = run_once(world)                              # archive offers nothing newer
    assert out['acquire']['status'] == 'no-newer' and out['stage']['reason'] == 'committed-newer-than-live'
    assert out['jobs']['deploy'] == 'success' and live_source(world) == KEYS[1]
    assert len(world.archive.get_requests()) == gets and totals(world).objects == 2


def test_runner_interrupted_after_publication_commit_is_recovered_next_run(world):
    put(world, KEYS[0])
    out = run_once(world, cancel_publish=True)         # killed after the push, before staging/deploy
    assert out['conclusion'] == 'cancelled' and committed_source(world) == KEYS[0] and live_source(world) is None
    out = run_once(world)
    assert out['stage']['reason'] in ('live-missing-or-unverifiable',) and live_source(world) == KEYS[0]


def test_runner_interrupted_during_acquisition_keeps_the_reservation_charged(world):
    put(world, KEYS[0])

    def interrupted(key, size, root, discovered):
        raise KeyboardInterrupt
    out = run_once(world, interrupt=interrupted)
    assert out['acquire'] == {'status': 'interrupted'} and out['conclusion'] == 'cancelled'
    t = totals(world)
    assert (t.objects, t.source_bytes, t.publications) == (1, 200_000, 0)
    out = run_once(world)                              # the next run re-reserves before trying again
    assert out['acquire']['status'] == 'published' and totals(world).objects == 2


def test_recovery_is_still_possible_when_the_source_budget_is_exhausted(tmp_path, monkeypatch):
    w = make_world(tmp_path, maxSourceBytes=pilot.MAX_SOURCE + 399_999)
    install(w, monkeypatch)
    put(w, KEYS[0])
    run_once(w)
    put(w, KEYS[1])
    w.pages.fail_next = 1
    run_once(w)                                        # 2 objects, 400 000 bytes: budget now exhausted
    gets = len(w.archive.get_requests())
    put(w, KEYS[2])
    out = run_once(w)
    assert out['guard']['reason'] == 'source-byte-budget-exhausted' and not out['guard']['acquire']
    assert out['guard']['stop'] and out['jobs']['stop'] == 'success'
    assert out['jobs']['deploy'] == 'success' and live_source(w) == KEYS[1]
    assert len(w.archive.get_requests()) == gets          # recovery requested no source object


# ------------------------------------------------------------------ authorization, expiry, kill switch

def test_kill_switch_off_refuses_without_any_api_or_archive_call(world):
    put(world, KEYS[0])
    world.enabled = 'false'
    calls = world.gh.calls
    out = run_once(world)
    assert out['guard']['reason'] == 'kill-switch-off' and out['jobs'] == {'guard': 'success'}
    assert world.gh.calls == calls and world.archive.requests == [] and not world.gh.disabled
    for value in ('', 'TRUE', 'yes', None):
        world.enabled = value
        assert run_once(world)['guard']['reason'] == 'kill-switch-off'
    world.enabled = 'true'                             # only the owner turning it back on resumes
    assert run_once(world)['acquire']['status'] == 'published'


def test_kill_switch_flipped_between_guard_and_publish_still_refuses(world):
    put(world, KEYS[0])
    with pytest.raises(pilot.Refusal, match='kill-switch-off'):
        pilot.acquire(world.checkout('p'), now_fn=world.now, enabled='false', public_b64=world.public_b64,
                      signing_key=world.signing, work=world.tmp / 'w', run_id=1, attempt=1, produce=world.fake_produce)
    assert world.archive.requests == []


def test_expired_pilot_refuses_even_when_the_disable_call_fails_and_never_resumes(world):
    put(world, KEYS[0])
    world.clock[0] = T0 + timedelta(hours=48)
    world.gh.disable_fails = True
    for _ in range(3):
        out = run_once(world)
        assert out['guard']['reason'] == 'expired' and out['guard']['stop'] and not out['guard']['acquire']
        assert out['jobs']['stop'] == 'failure' and 'publish' not in out['jobs']
    assert world.archive.requests == []
    with pytest.raises(pilot.Refusal, match='expired'):  # the publish job re-checks independently
        pilot.acquire(world.checkout('p'), now_fn=world.now, enabled='true', public_b64=world.public_b64,
                      signing_key=world.signing, work=world.tmp / 'w', run_id=9, attempt=1, produce=world.fake_produce)


def test_before_window_refuses_without_stopping(tmp_path, monkeypatch):
    w = make_world(tmp_path, notBeforeUtc=pilot.iso(T0 + timedelta(hours=2)), expiresUtc=pilot.iso(T0 + timedelta(hours=10)))
    install(w, monkeypatch)
    out = run_once(w)
    assert out['guard']['reason'] == 'not-yet-authorized' and not out['guard']['stop']


@pytest.mark.parametrize('change', [
    {'expiresUtc': pilot.iso(T0 + timedelta(hours=48, seconds=1))},
    {'maxSourceBytes': (8 << 30) + 1}, {'maxSourceObjects': 601}, {'maxRepositoryBytes': (400 << 20) + 1},
    {'format': 'other'}, {'pilotId': 'Bad Id'}, {'workflowFile': '../x.yml'}])
def test_authorization_cannot_exceed_code_ceilings(tmp_path, monkeypatch, change):
    w = make_world(tmp_path, **change)
    install(w, monkeypatch)
    out = run_once(w)
    assert out['guard']['reason'] == 'authorization-invalid' and out['guard']['stop'] and not out['guard']['acquire']


def test_missing_authorization_refuses(world):
    c = world.checkout('rm')
    git('rm', '--quiet', 'pilot/authorization.json', cwd=c)
    git('-c', 'user.name=o', '-c', 'user.email=o@o', 'commit', '--quiet', '-m', 'rm', cwd=c)
    git('push', '--quiet', 'origin', 'main', cwd=c)
    assert run_once(world)['guard']['reason'] == 'authorization-missing'


# ------------------------------------------------------------------ cumulative budgets

def test_cumulative_source_budget_counts_failed_downloads_and_is_a_hard_cap(tmp_path, monkeypatch):
    size = 300_000
    w = make_world(tmp_path, maxSourceBytes=pilot.MAX_SOURCE + 2 * size)
    install(w, monkeypatch)
    w.archive.put(KEYS[0], level2_bytes(KEYS[0], size))
    w.archive.truncate.add(KEYS[0])                    # 1st: failed download, still charged
    assert run_once(w)['acquire']['status'] == 'failed'
    w.archive.truncate.clear()
    assert run_once(w)['acquire']['status'] == 'published'
    w.archive.put(KEYS[1], level2_bytes(KEYS[1], size))
    assert run_once(w)['acquire']['status'] == 'published'
    w.archive.put(KEYS[2], level2_bytes(KEYS[2], size))
    out = run_once(w)
    assert out['guard']['reason'] == 'source-byte-budget-exhausted' and out['guard']['stop']
    t = w.ledger_totals()
    assert t.source_bytes == 3 * size and w.archive.body_bytes <= t.source_bytes <= w.auth['maxSourceBytes']
    for _ in range(2):                                 # stays exhausted: the ledger only grows
        assert run_once(w)['guard']['reason'] == 'source-byte-budget-exhausted'


def test_cumulative_object_budget(tmp_path, monkeypatch):
    w = make_world(tmp_path, maxSourceObjects=2)
    install(w, monkeypatch)
    for k in KEYS[:3]:
        put(w, k)
        run_once(w)
    assert w.ledger_totals().objects == 2 and committed_source(w) == KEYS[1]


def test_precise_check_uses_the_actual_object_size(tmp_path, monkeypatch):
    w = make_world(tmp_path, maxSourceBytes=pilot.MAX_SOURCE)  # worst case allows exactly one 64 MiB reservation
    install(w, monkeypatch)
    put(w, KEYS[0])
    assert run_once(w)['acquire']['status'] == 'published'
    assert run_once(w)['guard']['reason'] == 'source-byte-budget-exhausted'


def test_ledger_tampering_or_corruption_refuses(world):
    put(world, KEYS[0])
    run_once(world)
    c = world.checkout('t')
    entry = next((c / 'pilot/ledger').rglob('*-reserve.json'))
    entry.write_text('{not json')
    git('-c', 'user.name=o', '-c', 'user.email=o@o', 'commit', '--quiet', '-am', 'corrupt', cwd=c)
    git('push', '--quiet', 'origin', 'main', cwd=c)
    out = run_once(world)
    assert out['guard']['reason'] == 'ledger-invalid' and out['guard']['stop'] and not out['guard']['acquire']


# ------------------------------------------------------------------ repository growth

def bare_repo_bytes(remote: Path) -> int:
    subprocess.run(['git', 'gc', '--quiet', '--aggressive', '--prune=now'], cwd=remote, check=True)
    stats = dict(l.split(': ') for l in subprocess.run(['git', 'count-objects', '-v'], cwd=remote, check=True,
                                                         capture_output=True, text=True).stdout.splitlines())
    return (int(stats['size']) + int(stats['size-pack'])) * 1024


def bare_repo_loose_bytes(remote: Path) -> int:
    """Upper bound without delta compression: every object stored on its own (zlib), as a fresh push could."""
    out = subprocess.run(['git', 'cat-file', '--batch-all-objects', '--batch-check=%(objectsize)'], cwd=remote,
                         check=True, capture_output=True, text=True).stdout.split()
    return sum(map(int, out))


def test_repository_growth_accounting_is_an_upper_bound_and_the_ceiling_stops_acquisition(tmp_path, monkeypatch):
    w = make_world(tmp_path, repositoryBaselineBytes=0, maxRepositoryBytes=pilot.WORST_PUBLICATION_BYTES + 1_000_000)
    install(w, monkeypatch)
    base_objects = bare_repo_loose_bytes(w.remote)
    published = 0
    for k in KEYS:
        put(w, k)
        out = run_once(w)
        if out['guard']['reason'] == 'repository-budget-exhausted':
            break
        assert out['acquire']['status'] == 'published'
        published += 1
    t = w.ledger_totals()
    actual = bare_repo_loose_bytes(w.remote) - base_objects   # uncompressed object bytes added by the pilot
    assert actual <= t.repository_bytes, (actual, t.repository_bytes)
    assert bare_repo_bytes(w.remote) <= t.repository_bytes + base_objects
    assert 1 <= published < len(KEYS) and out['guard']['stop']
    w.measured = {'publications': published, 'accounted': t.repository_bytes, 'actualUncompressed': actual}
    print('repository-growth', json.dumps(w.measured))


# ------------------------------------------------------------------ cross-run failure accounting

def test_three_consecutive_failures_stop_and_stay_stopped(world):
    put(world, KEYS[0])
    world.archive.list_error = OSError('simulated archive outage')
    for i in range(3):
        out = run_once(world)
        assert out['acquire']['reason'] == 'archive-unavailable' and out['conclusion'] == 'failure'
    world.archive.list_error = None                    # archive healthy again
    world.gh.disable_fails = True                      # and the self-disable call fails
    for _ in range(3):
        out = run_once(world)
        assert out['guard']['reason'] == 'consecutive-failures' and out['guard']['stop'] and not out['guard']['acquire']
    assert committed_source(world) is None and world.archive.get_requests() == []


def test_recovery_after_a_correctly_handled_transient_failure(world):
    put(world, KEYS[0])
    world.archive.list_error = OSError('blip')
    assert run_once(world)['conclusion'] == 'failure'
    assert run_once(world)['conclusion'] == 'failure'
    world.archive.list_error = None
    out = run_once(world)
    assert out['guard']['consecutiveFailures'] == 2 and out['acquire']['status'] == 'published'
    world.archive.list_error = OSError('blip')
    out = run_once(world)
    assert out['conclusion'] == 'failure'
    world.archive.list_error = None
    assert run_once(world)['guard']['consecutiveFailures'] == 1    # the healthy run reset the streak


def test_failure_accounting_api_unavailable_never_authorizes_acquisition(world):
    put(world, KEYS[0])
    world.gh.unavailable = True
    for _ in range(3):
        out = run_once(world)
        assert out['jobs'] == {'guard': 'failure'} and out['conclusion'] == 'failure'
    assert world.archive.requests == []
    world.gh.unavailable = False                       # the outage runs themselves count
    out = run_once(world)
    assert out['guard']['reason'] == 'consecutive-failures'


def test_neutral_runs_do_not_reset_the_streak(world):
    put(world, KEYS[0])
    world.archive.list_error = OSError('x')
    run_once(world)
    run_once(world)
    world.enabled = 'false'
    for _ in range(4):
        run_once(world)                               # kill-switch runs: success, publish skipped -> neutral
    world.enabled = 'true'
    out = run_once(world)                              # third failure in a row (archive still down)
    assert out['conclusion'] == 'failure'
    assert run_once(world)['guard']['reason'] == 'consecutive-failures'


def test_cancelled_runs_count_as_failures(world):
    put(world, KEYS[0])
    for _ in range(3):
        assert run_once(world, cancel_publish=True)['conclusion'] == 'cancelled'
    assert run_once(world)['guard']['reason'] == 'consecutive-failures'


@pytest.mark.parametrize('runs,jobs,expect', [
    ([{'id': 1, 'run_number': 1, 'status': 'in_progress', 'conclusion': None}], {}, 'uncertain'),
    ([{'id': 1, 'run_number': 1, 'status': 'waiting', 'conclusion': None}], {}, 'uncertain'),
    ([{'id': 1, 'run_number': 1, 'status': 'completed', 'conclusion': 'action_required'}], {}, 'uncertain'),
    ([{'id': 1, 'run_number': 1, 'status': 'completed', 'conclusion': 'stale'}], {}, 'uncertain'),
    ([{'id': 1, 'run_number': 1, 'status': 'completed', 'conclusion': 'success'}], {1: [{'name': 'publish', 'conclusion': 'failure'}]}, 'uncertain'),
    ([{'id': 1, 'run_number': 1, 'status': 'completed'}], {}, 'uncertain'),
    ([{'id': 1, 'run_number': 1, 'status': 'completed', 'conclusion': 'timed_out'},
      {'id': 2, 'run_number': 2, 'status': 'completed', 'conclusion': 'startup_failure'}], {}, 2),
    ([{'id': 1, 'run_number': 1, 'status': 'completed', 'conclusion': 'success'}], {1: [{'name': 'guard', 'conclusion': 'success'}]}, 0),
    ([{'id': 5, 'run_number': 9, 'status': 'queued', 'conclusion': None}], {}, 0),   # newer queued run: ignored
    ([{'id': 1, 'run_number': 1, 'status': 'completed', 'conclusion': 'cancelled'}], {1: []}, 0),   # superseded pending
    ([{'id': 1, 'run_number': 1, 'status': 'completed', 'conclusion': 'cancelled'}],
     {1: [{'name': 'guard', 'conclusion': 'cancelled', 'steps': []}]}, 0),
    ([{'id': 1, 'run_number': 1, 'status': 'completed', 'conclusion': 'cancelled'}],
     {1: [{'name': 'guard', 'conclusion': 'success', 'steps': [{'name': 'x'}]}]}, 1),
    ([{'id': 1, 'run_number': 1, 'status': 'completed', 'conclusion': 'cancelled'}], {1: 'garbage'}, 'uncertain'),
])
def test_failure_classification(runs, jobs, expect):
    def api(path, method='GET'):
        if '/jobs' in path:
            return {'jobs': jobs.get(int(path.split('/runs/')[1].split('/')[0]), [])}
        return {'workflow_runs': sorted(runs, key=lambda r: -r['run_number'])}
    if expect == 'uncertain':
        with pytest.raises(pilot.Uncertain):
            pilot.consecutive_failures(api, 'o/r', 'w.yml', run_id=99, run_number=5, since=T0)
    else:
        assert pilot.consecutive_failures(api, 'o/r', 'w.yml', run_id=99, run_number=5, since=T0) == expect


@pytest.mark.parametrize('payload', [None, [], {'workflow_runs': 'x'}, {'nothing': 1}])
def test_malformed_api_responses_are_uncertain(payload):
    with pytest.raises(pilot.Uncertain):
        pilot.consecutive_failures(lambda p, m='GET': payload, 'o/r', 'w.yml', run_id=1, run_number=2, since=T0)


def test_lookback_pages_through_neutral_runs_and_fails_closed_when_exhausted():
    neutral = [{'id': i, 'run_number': i, 'status': 'completed', 'conclusion': 'skipped'} for i in range(1, 1201)]
    pages = []

    def api(path, method='GET'):
        page = int(re.search(r'[?&]page=(\d+)', path)[1])
        pages.append(page)
        rows = sorted(neutral, key=lambda r: -r['run_number'])
        return {'workflow_runs': rows[(page - 1) * 100: page * 100]}
    with pytest.raises(pilot.Uncertain, match='lookback'):
        pilot.consecutive_failures(api, 'o/r', 'w.yml', run_id=5000, run_number=5000, since=T0)
    assert pages == list(range(1, pilot.API_MAX_PAGES + 1))


def test_real_http_api_errors_become_uncertain(monkeypatch):
    import urllib.error
    def boom(*a, **k):
        raise urllib.error.HTTPError('u', 503, 'unavailable', {}, None)
    monkeypatch.setattr('urllib.request.urlopen', boom)
    with pytest.raises(pilot.Uncertain):
        pilot.github_api('t')('/repos/o/r/actions/workflows/w.yml/runs')


# ------------------------------------------------------------------ CLI exit codes (what the workflow observes)

def test_cli_refusal_exits_zero_and_failure_exits_one(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    w = make_world(tmp_path, notBeforeUtc=pilot.iso(now - timedelta(hours=1)), expiresUtc=pilot.iso(now + timedelta(hours=1)))
    out_file, summary = tmp_path / 'out', tmp_path / 'summary'
    for k, v in {'GITHUB_OUTPUT': out_file, 'GITHUB_STEP_SUMMARY': summary, 'GITHUB_REPOSITORY': 'o/r',
                 'GITHUB_RUN_ID': 7, 'GITHUB_RUN_NUMBER': 7, 'PUBLICATION_ENABLED': 'false', 'GH_TOKEN': 'x'}.items():
        monkeypatch.setenv(k, str(v))
    c = w.checkout('cli')
    assert pilot.main(['guard', '--repo', str(c)]) == 0
    assert 'acquire=false' in out_file.read_text() and 'kill-switch-off' in summary.read_text()
    monkeypatch.setenv('PUBLICATION_ENABLED', 'true')
    monkeypatch.setattr('urllib.request.urlopen', lambda *a, **k: (_ for _ in ()).throw(OSError('down')))
    assert pilot.main(['guard', '--repo', str(c)]) == 1     # uncertainty fails the run, never proceeds
    assert 'failure-accounting-uncertain' in summary.read_text() and out_file.read_text().count('acquire=true') == 0


# ------------------------------------------------------------------ workflow shape (static)

WORKFLOW = PUBLICATION / '.github/workflows/publish-kinx-scheduled.yml'


def jobs_text():
    text = WORKFLOW.read_text()
    blocks = re.split(r'\n  (?=[a-z]+:\s)', text.split('\njobs:\n', 1)[1])
    return text, {b.split(':', 1)[0].strip(): b for b in blocks if b.strip()}


def test_workflow_triggers_only_on_schedule_with_bounded_concurrency():
    text, _ = jobs_text()
    on = text.split('\non:\n', 1)[1].split('\npermissions:', 1)[0]
    assert re.search(r'cron: "2-57/5 \* \* \* \*"', on) and 'workflow_dispatch' not in on and 'push' not in on
    assert 'permissions: {}' in text and 'group: publish' in text and 'cancel-in-progress: false' in text


def test_every_action_is_pinned_to_a_full_commit():
    text, _ = jobs_text()
    uses = re.findall(r'uses:\s*(\S+)', text)
    assert uses and all(re.fullmatch(r'actions/[a-z-]+@[0-9a-f]{40}', u) for u in uses), uses


def test_least_privilege_per_job():
    _, jobs = jobs_text()
    def scoped(name):
        section = jobs[name].split('permissions:\n', 1)[1]
        return dict(re.findall(r'^      ([a-z-]+): (read|write)$', re.split(r'\n    (?=\S)', section, maxsplit=1)[0], re.M))
    assert scoped('guard') == {'contents': 'read', 'actions': 'read'}
    assert scoped('publish') == {'contents': 'write'}
    assert scoped('deploy') == {'contents': 'read', 'pages': 'write', 'id-token': 'write'}
    assert scoped('stop') == {'actions': 'write'}
    for name, block in jobs.items():
        has_secrets = 'secrets.' in block
        assert has_secrets == (name == 'publish'), name
    assert 'actions: write' not in jobs['publish'] + jobs['deploy'] + jobs['guard']
    assert 'uses:' not in jobs['stop'] and 'environment:' in jobs['deploy']


def test_python_dependencies_are_hash_locked_and_installed_as_wheels_only():
    lock = (PUBLICATION / 'ops/requirements.lock').read_text()
    pins = re.findall(r'^([a-z0-9-]+)==([0-9.]+)', lock, re.M)
    assert {n for n, _ in pins} == {'cryptography', 'cffi', 'pycparser', 'numpy'}
    for name, _ in pins:
        block = re.split(r'\n(?=[a-z])', lock.split(f'{name}==', 1)[1], maxsplit=1)[0]
        assert re.search(r'--hash=sha256:[0-9a-f]{64}', block)
    text, _ = jobs_text()
    assert "--require-hashes --only-binary ':all:' --no-deps -r ops/requirements.lock" in text
    assert 'pip install -e' not in text
