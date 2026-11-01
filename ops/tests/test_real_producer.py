"""End-to-end with the UNCHANGED scientific producer on preserved, previously acquired KINX Level II volumes, served by
the fake archive (no NOAA request). Skipped where the preserved research volumes are not present."""
import base64, json, os
from pathlib import Path
import pytest
from conftest import FIXTURES, live_source, run_once
from immersive_radar_pipeline import foreground

RAW = Path(os.environ.get('IR023_RAW_DIR', Path.home() / 'Research/immersive-radar/ir015/acquire-20261030/raw'))
VOLUMES = {'2026/10/30/KINX/KINX20261030_152702_V06': 'b756124cb3bfd5e142ef3e6f0153b7940421e74031fd399ca3b97dd82a1fb884',
           '2026/10/30/KINX/KINX20261030_153250_V06': 'ce8c2143692b6e9e4fc3684ba7c6ffb2f08fc6ea6fd7150511b67db861622a64'}
pytestmark = pytest.mark.skipif(not all((RAW / d / Path(k).name).exists() for k, d in VOLUMES.items()),
                                reason='preserved IR-015 raw volumes not available')


def artifact_sha(package: bytes) -> str:
    return json.loads(package)['descriptor']['sha256']


def test_unchanged_producer_reproduces_the_accepted_ir015_products_through_the_pilot(world):
    for key, directory in VOLUMES.items():
        world.archive.put(key, (RAW / directory / Path(key).name).read_bytes())
        out = run_once(world, produce=foreground.produce)   # the real, unchanged producer
        assert out['acquire']['status'] == 'published', out
        assert live_source(world) == key
        site = world.checkout('verify') / 'site/kinx'
        index = json.loads(base64.b64decode(json.loads((site / 'index.json').read_bytes())['payload']))
        produced = (site / index['latest']['path']).read_bytes()
        accepted = (FIXTURES / (Path(key).name + '.irradar')).read_bytes()
        # Scientific product (IRVOL artifact) is byte-identical to the accepted IR-015 package for the same volume.
        assert artifact_sha(produced) == artifact_sha(accepted)
        assert json.loads(produced)['descriptor']['numericalPolicyID'] == 'irvol-v1-linearZ-numeric-2'
        fresh = out['acquire']['publication']['freshness']
        assert fresh['archiveLastModifiedUtc'] and fresh['receivedUtc'] and fresh['processedUtc']
    assert world.ledger_totals().objects == 2
