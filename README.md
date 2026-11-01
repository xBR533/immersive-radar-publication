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

## IR-023 scheduled pilot (installed only after a separate owner GO)

`publish-kinx-scheduled.yml` runs every 5 minutes. It is inert until the variable `PUBLICATION_ENABLED` is exactly
`true` **and** an owner-committed `pilot/authorization.json` covers the current time (at most 48 h). Each run acquires at
most one NOAA object (≤ 64 MiB), reserved in the append-only ledger `pilot/ledger/` before any byte is requested.
Cumulative objects, source bytes and repository bytes are hard-capped. The workflow stops itself on expiry, after 3
consecutive failed runs, or when a budget is reached. **To stop it now, see `docs/IR023_PILOT_RUNBOOK.md`** (kill switch,
disable, cancel, credential response). Still not a live feed, not an operational or warning product.
