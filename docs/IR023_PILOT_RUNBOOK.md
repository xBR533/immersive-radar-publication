# IR-023 scheduled publication pilot — owner runbook

Workflow: `.github/workflows/publish-kinx-scheduled.yml` · Controls: `ops/pilot.py` · Budgets: `pilot/authorization.json` (owner-committed) and `pilot/ledger/<pilotId>/` (append-only, written by the workflow).

Everything below works from the GitHub web UI or `gh` on any machine. None of it needs Claude.

## Stop now

**For an emergency shutdown, do all three steps.** The kill switch blocks the next authorized work, but it can't
interrupt a runner that is already working. Only cancelling a run does that.

| Control | What it stops | What it does NOT stop |
|---|---|---|
| Kill switch `PUBLICATION_ENABLED` ≠ `true` | Every job that starts after the change: the guard refuses, `acquire` and `stage` re-check it, the `deploy` job is skipped, and `preflight` re-checks it immediately before deploy-pages | A job already past its check, e.g. an `acquire` that is downloading or a `deploy-pages` call already in progress. Job environment values are fixed when the job starts. |
| Disable workflow | Creation of new scheduled runs | Runs already queued or running |
| Cancel run | The run in progress (each job is cancelled; nothing further is deployed) | Work already pushed to `main` or already deployed (both are verified, valid publications) |

1. **Kill switch:**
   ```bash
   gh variable set PUBLICATION_ENABLED --body false -R xBR533/immersive-radar-publication
   ```
   Web: repository → Settings → Secrets and variables → Actions → Variables → `PUBLICATION_ENABLED` → `false`.
   Any value other than exactly `true` is "off".
2. **Disable the workflow:**
   ```bash
   gh workflow disable publish-kinx-scheduled.yml -R xBR533/immersive-radar-publication
   ```
   Web: Actions → "Publish newer KINX observations (IR-023 scheduled pilot)" → ⋯ → Disable workflow.
3. **Cancel anything running or queued:**
   ```bash
   gh run list -R xBR533/immersive-radar-publication --workflow publish-kinx-scheduled.yml --limit 5
   ```
   ```bash
   gh run cancel <run-id> -R xBR533/immersive-radar-publication
   ```
   A run cancelled mid-acquisition keeps its reservation charged in the ledger. Its partial files are never committed or deployed.
4. **Confirm:** after the next 5-minute mark, `gh run list …` shows no new or in-progress run, and
   `gh workflow list --all -R xBR533/immersive-radar-publication` shows the workflow `disabled_manually`.

Worst case after step 1 alone: one run that had already passed its checks can finish one acquisition (≤ 64 MiB)
and one deployment, within its job timeouts (acquire 8 min, stage 4 min, deploy 6 min).

The site keeps serving the last valid publication, and the headset keeps its saved scans and shows STALE or offline
truthfully. **Unpublishing Pages is also safe against the pilot.** If the live index is absent, the pilot refuses to
republish it unless the authorization explicitly sets `allowInitialPublication: true`, and the Gate 3 authorization
keeps that `false`.

## Credential compromise

| Suspected | Do |
|---|---|
| Signing key (`IR019B_SIGNING_KEY`) | Stop now (above). Then `gh secret delete IR019B_SIGNING_KEY -R xBR533/immersive-radar-publication`. If fabricated data may be live: `gh api -X DELETE repos/xBR533/immersive-radar-publication/pages` (unpublish). The app trusts only key `888a89015b6da4a7`, so a replacement key needs a new app build (separately authorized). |
| Producer deploy key | `gh repo deploy-key delete 168309141 -R xBR533/immersive-radar` and `gh secret delete PRODUCER_READ_DEPLOY_KEY -R xBR533/immersive-radar-publication`. It is read-only, so the exposure is the private source code. |
| Workflow/token abuse | Turn off Actions for the repository: `gh api -X PUT repos/xBR533/immersive-radar-publication/actions/permissions -F enabled=false` (Settings → Actions → General → Disable actions). |

## What stops by itself

| Condition | Effect | Resumes? |
|---|---|---|
| `PUBLICATION_ENABLED` ≠ `true` | no acquisition, no deployment | only when the owner sets it back to `true` |
| now ≥ `expiresUtc` | no acquisition or deployment; the isolated `stop` job disables the workflow | **never** (time only moves forward; a failed disable call changes nothing) |
| 3 consecutive failed executions in the window | no acquisition; workflow disabled | never automatically: refusals don't count as healthy runs, so the streak stays |
| Failure accounting unknown (API down, odd run state) | run fails, no acquisition | the outage runs count toward the 3-failure stop |
| Live index unreachable, unverifiable, absent (without allowance) or newer than `main` | that run deploys nothing and fails | counts toward the 3-failure stop |
| Deployed index not served by both origin and the plain URL within 150 s | the run fails (deployment accepted ≠ publicly visible) | later runs recheck the plain URL for ≤ 150 s without redeploying; each run fails until the plain URL serves an authenticated matching index, and 3 in a row stop the pilot |
| Source objects/bytes or repository bytes would exceed the ledger budget | no acquisition; already-committed work may still be deployed once; workflow disabled | never: the ledger only grows |
| Authorization or ledger missing/invalid | no acquisition; workflow disabled | only after a new owner commit |

**Restarting** after any automatic stop means committing a new `pilot/authorization.json` with a new `pilotId` and window. That is a new owner authorization: a new budget and a new failure window. Never rewrite or force-push `main`.

## Gate 3 procedure (proposed, not executed; needs a separate owner GO)

Read-only preflight:
1. Billing: Settings → Billing → confirm the Actions spending limit is $0 (or acceptable), that there's no billing block, and how artifact storage is treated for this public repository.
2. `git ls-remote https://github.com/xBR533/immersive-radar-publication.git main` still equals the reviewed base (`9b0046f…` at Gate 2), and `git ls-remote https://github.com/xBR533/immersive-radar.git main` contains `PRODUCER_COMMIT` `2033127…`.
3. `gh variable list -R xBR533/immersive-radar-publication` shows no `PUBLICATION_ENABLED`.

Owner-executed steps:
1. Retire the IR-019B one-shot publisher: `gh workflow disable publish-kinx.yml -R xBR533/immersive-radar-publication`.
2. Fast-forward publication `main` to the reviewed Gate 2 branch, then immediately `gh workflow disable publish-kinx-scheduled.yml -R xBR533/immersive-radar-publication`. Even left enabled, runs are inert: with no variable set, the guard refuses.
3. Measure the baseline: `git clone --mirror https://github.com/xBR533/immersive-radar-publication.git b && git -C b count-objects -v`. Set `repositoryBaselineBytes` ≥ `(size + size-pack) × 1024`.
4. Commit `pilot/authorization.json` (copy of `pilot/authorization.example.json` with the real window ≤ 48 h, `pilotId`, baseline).
5. At the start time: `gh variable set PUBLICATION_ENABLED --body true -R …` and `gh workflow enable publish-kinx-scheduled.yml -R …`.
6. First hour: watch the first runs. Expect a guard pass, then a `reserve` commit followed by a `publish` commit, a deploy, and the probe reporting the index visible. Abort with "Stop now" on anything unexpected.
7. During the pilot: check `gh run list` a few times a day. The ledger (`pilot/ledger/…`) is the budget record.
8. At expiry the pilot stops itself. Then set the kill switch to `false`, confirm the workflow is disabled, and collect the evidence: run list and conclusions, ledger totals, per-publication freshness (ledger `freshness` + probe summaries), repository size, and a final `publish.py verify` of the live site.

A successful pilot does not enable indefinite publication. Continued operation needs Gate 4 (headset) and Gate 5 (independent review, git-free storage, production key custody).
