# S3 Cleaner — Architecture & Design

This document covers the internal design decisions, data flow details, and the rationale behind each safety control.

---

## System Context

```
┌─────────────┐         ┌──────────────┐         ┌────────────────┐
│  Operator   │────────▶│  s3-cleaner  │────────▶│  AWS Services  │
│  (human)    │         │  (Go CLI)    │         │                │
│             │◀────────│              │◀────────│  STS           │
│  Reviews    │  CSV +  │  plan/apply  │  API    │  S3            │
│  manifest   │  audit  │              │  calls  │  Athena        │
└─────────────┘         └──────────────┘         │  Glue          │
                                                  └────────────────┘
```

The tool is a **batch CLI** — no server, no daemon, no state between runs. All state is in files (manifest CSV, hash sidecar, summary JSON, audit log) and the S3 bucket itself.

---

## Module Responsibilities

### main.go (43 lines)
Entry point. Routes `plan` / `apply` / `help` subcommands. Sets UTC log timestamps.

### config.go (91 lines)
AWS plumbing shared by plan and apply:

- **awsConfig**: Loads AWS SDK config, calls STS GetCallerIdentity, hard-fails if account != `113612240996`. Every plan and every apply starts with this check.
- **versioning**: Wraps GetBucketVersioning, maps SDK enum to one of three strings: `"Enabled"`, `"Suspended"`, `"Disabled"`. The string is stored in the manifest and re-checked at apply time.
- **verifyRegion**: HeadBucket and compare `BucketRegion` header. Guards against operating on a bucket in the wrong region (which can silently succeed for some API calls but produce inconsistent state).
- **checkObjectLock**: GetObjectLockConfiguration. If Object Lock is enabled, refuse to proceed — deleting locked objects fails at the API level, and partial failures in a batch are confusing.
- **parsePrefixes**: Splits comma-separated prefix string, trims whitespace, drops empties.

### plan.go (229 lines)
Athena query → CSV manifest pipeline:

1. Validates all inputs (bucket, output URI, database/table names, cutoff)
2. Runs all AWS pre-checks (identity, versioning, region, Object Lock)
3. Builds the Athena SQL with parameterized WHERE clauses
4. Starts the query and polls with 3s ticker until success/failure
5. Paginates GetQueryResults, validates each row, writes 11-column CSV
6. Generates SHA-256 sidecar and PlanSummary JSON

**SQL injection protection**: Database and table names are validated against `^[A-Za-z0-9_]+$` before interpolation into the query string.

**Prefix exclusion SQL**: Uses `substr("key", 1, N) <> 'prefix'` with `utf8.RuneCountInString` for correct multi-byte handling. This is a Presto/Trino function that works correctly with Athena.

### manifest.go (239 lines)
Two parallel manifest readers with identical validation logic:

- **readManifest**: Loads all candidates into a `[]Candidate` slice. Used by dry-run where the full set is needed in memory for preview.
- **streamManifest**: Callback-based, O(1) memory. Used by execute for the validation pass and delete pass. Supports a limit parameter.

Both enforce:
- CSV header must contain all 11 required columns
- Bucket must match the requested bucket
- Delete markers are rejected
- Modified date must be strictly before cutoff
- Versioning state must be `Enabled`, `Disabled`, or `Suspended`
- Protected prefix check on every key
- Duplicate `(key, version_id)` detection via `seen` map

**Why two readers?** The dry-run needs the full slice to truncate by limit and print a summary. Execute needs O(1) memory for million-row manifests. The `TestStreamManifestAgreesWithReadManifest` test walks 8 scenarios and asserts both produce identical results.

### apply.go (274 lines)
Two modes:

**Dry-run** (default): readManifest → print preview. No AWS API calls except manifest integrity check.

**Execute** (`--execute --confirm-bucket`):
1. AWS pre-checks (identity, versioning, region, Object Lock)
2. **Suspended guard**: refuses if versioning = Suspended
3. **Validation pass**: streams manifest, checks every row's versioning state matches current bucket state
4. **Delete pass**: streams manifest again, buffers batches of 1000, calls deleteBatch per batch
5. Writes audit log (JSON-lines) with per-object results and final summary

**deleteBatch**: Builds `[]ObjectIdentifier`, sets `VersionId` for versioned buckets (requires non-empty), omits it for Disabled. Uses `ExpectedBucketOwner` as an extra guard. Retries transient errors with exponential backoff (3 attempts, 500ms base, 30s cap).

### retry.go (32 lines)
Generic `retryWithBackoff(ctx, maxAttempts, baseDelay, fn)`. Backoff formula: `baseDelay * 2^attempt`, capped at 30s. Respects context cancellation between attempts.

---

## Data Flow: Plan to Apply

```
PLAN                          HUMAN                         APPLY
────                          ─────                         ─────

Athena query
    │
    ▼
CSV manifest ──────────────▶ Review CSV ──────────────────▶ Verify SHA-256
    │                        Check keys                        │
    ├── .sha256              Check counts                      │
    │                        Check cutoff                      ▼
    └── .summary.json ─────────────────────────────────────▶ Verify cutoff
                                                               │
                                                               ▼
                                                          Validate rows
                                                               │
                                                          ┌────┴────┐
                                                          │ Dry-run │
                                                          │  or     │
                                                          │ Execute │
                                                          └─────────┘
```

The SHA-256 hash is the tamper seal. If the manifest is modified after plan (even a single byte), apply refuses to proceed. The cutoff verification prevents the operator from accidentally using `--cutoff 2025-01-01` at apply time when the manifest was generated with `--cutoff 2023-01-01`.

---

## Delete Semantics by Versioning State

```
                    ┌───────────────────┐
                    │  Bucket State?    │
                    └────────┬──────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
         ┌────────┐    ┌─────────┐    ┌───────────┐
         │Disabled│    │ Enabled │    │ Suspended │
         └───┬────┘    └────┬────┘    └─────┬─────┘
             │              │               │
             ▼              ▼               ▼
       Key-only       Key+VersionId     REFUSED
       delete         delete            (safety)
             │              │
             │         ┌────┴────┐
             │         │ VersionId│
             │         │ = "null"?│
             │         └────┬────┘
             │         yes  │  no
             │         ┌────┴──────┐
             │         │ Pass      │ Pass
             │         │ "null"    │ real ID
             │         │ to API    │ to API
             │         └───────────┘
             │
             ▼
       No VersionId
       in API call
       (creates delete
       marker if versioning
       gets enabled later —
       but that's the correct
       behavior for current
       Disabled state)
```

---

## Retry Strategy

```
Attempt 1  ──fail──▶  sleep 500ms
Attempt 2  ──fail──▶  sleep 1000ms
Attempt 3  ──fail──▶  return error

Backoff = min(baseDelay * 2^attempt, 30s)
Context cancellation checked between attempts.
```

The retry wraps only the `DeleteObjects` API call. Plan-phase Athena calls are not retried (Athena has its own retry/queue semantics). The retry is for transient S3 errors like throttling or 500s, not permanent errors like AccessDenied.

---

## Memory Model

```
Plan:    Athena pagination ──▶ CSV write (streaming, O(1) memory)
Dry-run: CSV read ──▶ []Candidate slice (bounded by --limit, typically small)
Execute: CSV stream ──▶ batch[1000] ──▶ delete ──▶ flush ──▶ next batch (O(1))
```

At production scale (hundreds of millions of objects), the execute path never holds more than 1000 candidates in memory at once. The `seen` map for duplicate detection does grow with row count, but each entry is a small string (~50-200 bytes), so even 100M entries would use ~10-20 GB — which is large but bounded by the manifest size.

---

## Resolved Issues from Original Implementation

The tool went through a comprehensive bug-fix cycle. Key fixes:

| Issue | Original | Fixed |
|-------|----------|-------|
| Cutoff date hardcoded | Error messages showed `2023-01-01` regardless of `--cutoff` | Uses actual `--cutoff` value |
| No Athena timeout | Query could hang indefinitely | Configurable `--athena-timeout` (default 30m) |
| No retry on delete | Single attempt, transient errors = failure | 3 attempts with exponential backoff |
| No SHA-256 integrity | Manifest could be modified between plan and apply | SHA-256 sidecar generated and verified |
| No region check | Could operate on wrong-region bucket | HeadBucket verification |
| No Object Lock check | Would fail confusingly on locked objects | Pre-check blocks execution |
| Missing manifest columns | 8 columns vs spec's 11 | Added snapshot_date, versioning_state, query_id |
| No streaming reader | `readManifest` loaded everything into memory | `streamManifest` for O(1) execute path |
| No audit log | No record of what was deleted | JSON-lines audit with per-object results |
| No cutoff verification | Apply could use different cutoff than plan | Summary JSON comparison |
| No suspended guard | Would attempt deletion on suspended buckets | Explicit refusal |
| Null VersionID confusion | `"null"` could be confused with empty `""` | Distinct handling, tested |
