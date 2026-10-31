# Immersive Radar — experimental publication (IR-019B)

Static HTTPS publication of ONE processed NOAA NEXRAD Level II observation (radar KINX, public downtown-Tulsa
64 km research crop) for a private hardware evaluation of the Immersive Radar visionOS app.

* Served by GitHub Pages from `site/`: `kinx/index.json` (Ed25519-signed envelope), `kinx/packages/<sha256>.irradar`,
  `kinx/receipts/<sha256>.json`.
* Source data: NOAA NEXRAD Level II via the Unidata public archive. The source volume is identified by the SHA-256
  computed at receipt.
* **The signature is the Immersive Radar experimental publisher key (key ID `888a89015b6da4a7`). It is not a NOAA
  signature and does not establish NOAA authorship.** This key is for this experiment only.
* Not a live feed, not an operational or warning product. Published once, manually; there is no schedule.

The workflow is manual (`workflow_dispatch`), requires typing `PUBLISH-ONE`, acquires at most one source object
(≤ 64 MiB), runs at most 15 minutes, and uses the unchanged producer from the private application repository
(pinned commit).

To disable: Settings → Pages → Unpublish site, and Actions → "Publish one KINX observation" → Disable workflow
(or archive/delete this repository).
