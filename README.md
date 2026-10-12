# S3 Inventory Cleanup Tool

A Go CLI that uses **S3 Inventory + Athena** to identify and safely delete eligible S3 objects/versions older than a configurable cutoff date. Designed for production-scale buckets with hundreds of millions of objects.

---

## Table of Contents

- [Features](#features)
- [Architecture Overview](#architecture-overview)
- [Data Flow Diagram](#data-flow-diagram)
- [Execution Flow](#execution-flow)
- [Safety Controls](#safety-controls)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [File Structure](#file-structure)
- [Versioning Behavior](#versioning-behavior)
- [Manifest Format](#manifest-format)
- [Audit Log](#audit-log)
- [IAM Permissions](#iam-permissions)
- [Testing](#testing)
- [Production Checklist](#production-checklist)
- [Troubleshooting](#troubleshooting)

---

## Features

- **Plan / Apply / Execute** three-stage workflow — never deletes without explicit confirmation
- **S3 Inventory + Athena** as scalable metadata source (no `ListObjectsV2` at scale)
- **Streaming CSV processing** — O(1) memory for manifests with millions of rows
- **SHA-256 manifest integrity** — detects tampering between plan and apply
- **Versioned & non-versioned buckets** — exact VersionId deletion for versioned, key-only for non-versioned
- **Suspended bucket guard** — refuses apply for Suspended versioning state
- **Object Lock detection** — blocks execution on Object Lock-enabled buckets
- **Bounded retry with exponential backoff** — handles transient S3 errors
- **JSON-lines audit log** — per-object delete results for compliance
- **Protected prefix exclusions** — configurable prefix-based protection
- **AWS account identity guard** — STS verification prevents wrong-account operations
- **Bucket region verification** — HeadBucket check prevents cross-region misuse
- **Cutoff verification** — apply validates cutoff matches plan summary

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        S3 Inventory Cleanup Tool                     │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────┐    ┌───────────┐    ┌──────────┐    ┌──────────────┐   │
│  │ config  │    │  plan     │    │  apply   │    │  manifest    │   │
│  │         │    │           │    │          │    │              │   │
│  │ AWS cfg │    │ Athena    │    │ Dry-run  │    │ CSV parse    │   │
│  │ STS     │    │ query     │    │ Execute  │    │ Stream read  │   │
│  │ Region  │    │ CSV write │    │ Batched  │    │ SHA-256      │   │
│  │ Obj Lock│    │ Summary   │    │ delete   │    │ Validation   │   │
│  │ Version │    │ SHA-256   │    │ Audit    │    │ Dedup check  │   │
│  └─────────┘    └───────────┘    └──────────┘    └──────────────┘   │
│                                                                      │
│  ┌─────────┐    ┌───────────┐                                       │
│  │  retry  │    │   main    │                                       │
│  │         │    │           │                                       │
│  │ Exp.    │    │ CLI entry │                                       │
│  │ backoff │    │ plan/apply│                                       │
│  └─────────┘    └───────────┘                                       │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Diagram

```
                    ┌─────────────────┐
                    │   S3 Inventory  │
                    │  (Parquet data) │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Athena Table   │
                    │  (Glue Catalog) │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        ┌──────────┐  ┌───────────┐  ┌──────────────┐
        │ STS      │  │ Athena    │  │ S3 Bucket    │
        │ Identity │  │ Query     │  │ Versioning   │
        │ Check    │  │ Results   │  │ Region Check │
        └──────┬───┘  └─────┬─────┘  └──────┬───────┘
               │            │               │
               └────────────┼───────────────┘
                            │
                            ▼
                   ┌────────────────┐
                   │   PLAN PHASE   │
                   │                │
                   │ Write:         │
                   │  manifest.csv  │
                   │  .sha256       │
                   │  .summary.json │
                   └───────┬────────┘
                           │
                    Human Review
                           │
                           ▼
                   ┌────────────────┐
                   │  APPLY PHASE   │
                   │                │
                   │ 1. SHA-256     │
                   │    verify      │
                   │ 2. Cutoff      │
                   │    verify      │
                   │ 3. Dry-run or  │
                   │    Execute     │
                   └───────┬────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
     ┌────────────────┐       ┌────────────────┐
     │   DRY RUN      │       │   EXECUTE      │
     │                │       │                │
     │ Print preview  │       │ Validation pass│
     │ No API calls   │       │ Delete pass    │
     │ Show totals    │       │ Audit log      │
     └────────────────┘       └────────────────┘
```

---

## Execution Flow

### Plan Command Flow

```
START
  │
  ├── Parse flags (--bucket, --region, --database, --table, --cutoff, ...)
  │
  ├── Validate inputs
  │     ├── Bucket name required
  │     ├── Athena output S3 URI required
  │     ├── Database/table names alphanumeric+underscore only (SQL injection guard)
  │     └── Cutoff parsed as RFC3339 UTC
  │
  ├── AWS setup
  │     ├── Load AWS config for region
  │     ├── STS GetCallerIdentity → verify account 113612240996
  │     ├── GetBucketVersioning → record state (Enabled/Suspended/Disabled)
  │     ├── HeadBucket → verify region matches --region
  │     └── GetObjectLockConfiguration → block if Object Lock enabled
  │
  ├── Build Athena query
  │     ├── SELECT bucket, key, size, last_modified_date, version_id,
  │     │          is_latest, is_delete_marker, storage_class
  │     ├── WHERE bucket = '...'
  │     │     AND is_delete_marker = false
  │     │     AND last_modified_date < TIMESTAMP '...'
  │     │     AND substr("key", 1, N) <> 'prefix'  (per excluded prefix)
  │     └── ORDER BY last_modified_date ASC
  │
  ├── Execute Athena query
  │     ├── StartQueryExecution
  │     └── Poll GetQueryExecution every 3s (timeout: --athena-timeout, default 30m)
  │
  ├── Stream results → CSV
  │     ├── Paginate GetQueryResults (1000 rows/page)
  │     ├── Skip Athena header row
  │     ├── Validate each row (bucket match, no delete markers, valid size)
  │     ├── Append: snapshot_date, versioning_state, query_id (3 extra columns)
  │     └── Count rows and sum bytes
  │
  ├── Write sidecar files
  │     ├── manifest.csv.sha256 (SHA-256 hex + filename)
  │     └── manifest.csv.summary.json (PlanSummary with all metadata)
  │
  └── Print summary (candidates, bytes, MiB/GiB/TiB)
      └── "PLAN ONLY — no deletions performed"
```

### Apply Command Flow

```
START
  │
  ├── Parse flags (--bucket, --manifest, --execute, --confirm-bucket, --limit, --cutoff, ...)
  │
  ├── Validate inputs
  │     ├── Bucket name required
  │     ├── --execute requires --confirm-bucket = --bucket
  │     └── --cutoff parsed as RFC3339 UTC
  │
  ├── Verify cutoff matches plan
  │     ├── Read <manifest>.summary.json
  │     └── Compare Cutoff field → reject on mismatch
  │
  ├── Verify manifest integrity
  │     ├── Read <manifest>.sha256
  │     ├── Compute SHA-256 of manifest
  │     └── Compare → reject on mismatch (tamper detection)
  │
  ├─── DRY RUN (default, no --execute)
  │     │
  │     ├── readManifest() — load into memory (fine for small --limit)
  │     │     ├── Validate CSV header (11 required columns)
  │     │     ├── Per-row: bucket match, no delete markers, cutoff check,
  │     │     │            valid versioning state, protected prefix check,
  │     │     │            duplicate key/version detection
  │     │     └── Apply --limit
  │     │
  │     ├── Print each candidate (key, version_id, size, modified)
  │     └── "DRY RUN — no deletions performed"
  │
  └─── EXECUTE (--execute --confirm-bucket)
        │
        ├── AWS setup (same as plan: STS, versioning, region, Object Lock)
        │
        ├── SUSPENDED GUARD → refuse if versioning = Suspended
        │
        ├── PASS 1: Validation stream
        │     └── streamManifest() — verify every row's versioning state
        │           matches current bucket state
        │
        ├── PASS 2: Delete stream
        │     ├── streamManifest() — O(1) memory
        │     ├── Buffer up to 1000 candidates per batch
        │     ├── deleteBatch() per batch:
        │     │     ├── Enabled/Suspended: require non-empty VersionID
        │     │     ├── Disabled: key-only delete (no VersionId)
        │     │     ├── Set ExpectedBucketOwner = accountID
        │     │     ├── retryWithBackoff(3 attempts, 500ms base, 30s cap)
        │     │     └── Write audit entry per object (OK/ERROR)
        │     └── Flush remaining batch
        │
        ├── Write SUMMARY audit entry (deleted count, failed count, manifest hash)
        └── Exit nonzero if any deletions failed
```

---

## Safety Controls

| # | Control | Enforcement |
|---|---------|-------------|
| 1 | **Account identity** | STS GetCallerIdentity verifies account `113612240996` |
| 2 | **Bucket confirmation** | `--execute` requires `--confirm-bucket` exact match |
| 3 | **No implicit deletion** | `plan` never deletes; `apply` defaults to dry-run |
| 4 | **Manifest integrity** | SHA-256 hash sidecar verified before any processing |
| 5 | **Cutoff verification** | Apply rejects if `--cutoff` differs from plan summary |
| 6 | **Region verification** | HeadBucket confirms bucket is in expected region |
| 7 | **Object Lock check** | Blocks execution on Object Lock-enabled buckets |
| 8 | **Versioning state guard** | Rejects apply if bucket state changed since plan |
| 9 | **Suspended guard** | Refuses execute for Suspended versioning state |
| 10 | **Version-specific delete** | Versioned buckets require exact VersionId per candidate |
| 11 | **Delete markers excluded** | Query and validation both reject delete markers |
| 12 | **Protected prefixes** | Configurable prefix exclusion in both plan and apply |
| 13 | **Bounded retry** | 3 attempts, exponential backoff, 30s cap, context-aware |
| 14 | **Batch limit** | DeleteObjects batches capped at 1000 (S3 API limit) |
| 15 | **Audit trail** | JSON-lines log with per-object results |
| 16 | **Failure exit code** | Nonzero exit if any deletions failed |
| 17 | **SQL injection guard** | Database/table names validated with `^[A-Za-z0-9_]+$` |

---

## Prerequisites

- **Go 1.22+**
- **AWS CLI** configured with credentials that have the required IAM permissions
- **S3 Inventory** configured on the target bucket (Parquet format, all versions)
- **Athena** table pointing at the Inventory data (Glue Data Catalog)
- **Athena workgroup** with query results S3 location

---

## Installation

```bash
git clone https://github.com/mohitpant05/awsTool-S3-Cleanup.git
cd awsTool-S3-Cleanup

go mod tidy
go build -o s3-cleaner .
```

Binary is at `./s3-cleaner`. No external dependencies beyond the AWS SDK.

---

## Quick Start

### 1. Plan (generate candidate manifest)

```bash
./s3-cleaner plan \
  --bucket my-bucket \
  --region us-east-1 \
  --database s3_inventory_db \
  --table my_bucket_inventory \
  --athena-output s3://my-bucket/athena-query-results/ \
  --cutoff 2023-01-01T00:00:00Z \
  --manifest candidate-manifest.csv \
  --snapshot-date 2024-10-11 \
  --exclude-prefixes "reports/,backups/"
```

**Output files:**
- `candidate-manifest.csv` — one row per eligible object/version
- `candidate-manifest.csv.sha256` — integrity hash
- `candidate-manifest.csv.summary.json` — plan metadata (account, bucket, cutoff, counts)

### 2. Review the manifest

Open `candidate-manifest.csv` and verify:
- Object keys are expected (no critical data)
- Protected prefixes are excluded
- Version IDs look correct
- Row count and byte total match expectations

Review `candidate-manifest.csv.summary.json` for plan metadata.

### 3. Dry run (preview deletions)

```bash
./s3-cleaner apply \
  --bucket my-bucket \
  --region us-east-1 \
  --manifest candidate-manifest.csv \
  --cutoff 2023-01-01T00:00:00Z \
  --limit 10
```

Shows exactly what would be deleted. No S3 API calls.

### 4. Execute (delete approved candidates)

```bash
./s3-cleaner apply \
  --bucket my-bucket \
  --region us-east-1 \
  --manifest candidate-manifest.csv \
  --cutoff 2023-01-01T00:00:00Z \
  --limit 10 \
  --execute \
  --confirm-bucket my-bucket
```

- Start with `--limit 10` for a small canary
- Increase `--limit` after confirming results
- Use `--limit 0` for all candidates after full review

**Output:** `candidate-manifest.csv.audit.jsonl` — per-object results

---

## CLI Reference

### `s3-cleaner plan`

| Flag | Default | Description |
|------|---------|-------------|
| `--bucket` | *(required)* | Source S3 bucket name |
| `--region` | `us-east-1` | AWS region |
| `--database` | `s3_inventory_db` | Athena/Glue database name |
| `--table` | `tookan_test1_full_snapshot` | Athena/Glue table name |
| `--athena-output` | *(required)* | Writable S3 URI for Athena query results |
| `--cutoff` | `2023-01-01T00:00:00Z` | Exclusive cutoff date (RFC3339 UTC) |
| `--manifest` | `candidate-manifest.csv` | Local path for output CSV |
| `--workgroup` | `primary` | Athena workgroup |
| `--exclude-prefixes` | `reports/,athena-clean-test/,...` | Comma-separated protected prefixes |
| `--athena-timeout` | `30m` | Maximum wait for Athena query |
| `--snapshot-date` | *(empty)* | Inventory snapshot date (YYYY-MM-DD) for traceability |

### `s3-cleaner apply`

| Flag | Default | Description |
|------|---------|-------------|
| `--bucket` | *(required)* | Target S3 bucket name |
| `--region` | `us-east-1` | AWS region |
| `--manifest` | `candidate-manifest.csv` | Path to reviewed manifest |
| `--execute` | `false` | Enable actual deletion |
| `--confirm-bucket` | *(empty)* | Must match `--bucket` for `--execute` |
| `--limit` | `10` | Max candidates to process; `0` = all |
| `--cutoff` | `2023-01-01T00:00:00Z` | Must match plan cutoff (verified against summary) |
| `--exclude-prefixes` | `reports/,athena-clean-test/,...` | Protected prefixes (use same as plan) |

---

## File Structure

```
s3-cleaner/
├── main.go             CLI entry point, command routing
├── config.go           AWS config, STS identity, versioning, region, Object Lock
├── plan.go             Athena query, CSV/summary/hash generation
├── apply.go            Dry-run preview and execute deletion with audit
├── manifest.go         CSV parsing (readManifest + streamManifest), SHA-256, date parsing
├── retry.go            Bounded exponential backoff retry
├── config_test.go      Tests: prefix parsing, SQL generation, identifier regex
├── manifest_test.go    Tests: manifest validation, streaming, hash, null VersionID
├── retry_test.go       Tests: retry success/exhaust/context cancellation
├── go.mod              Module definition (Go 1.22, AWS SDK Go v2)
├── go.sum              Dependency checksums
├── agent.md            Implementation specification and context
└── README.md           This file
```

### Generated files (by plan/apply)

```
candidate-manifest.csv              Candidate manifest (plan output)
candidate-manifest.csv.sha256       SHA-256 hash sidecar
candidate-manifest.csv.summary.json Plan metadata (JSON)
candidate-manifest.csv.audit.jsonl  Per-object delete audit log (apply --execute output)
```

---

## Versioning Behavior

### Disabled buckets
- Objects have no version history
- Delete by key only (no VersionId in DeleteObjects)
- Deletion is irreversible unless backups exist
- Manifest `version_id` column may be empty

### Enabled buckets
- Delete exact `(Key, VersionId)` pairs
- Can remove noncurrent versions without affecting current version
- `VersionId = "null"` is valid — represents objects that existed before versioning was enabled
- Empty VersionId on an Enabled bucket is rejected as a safety measure
- Delete markers are excluded from candidates

### Suspended buckets
- **Apply is refused** for Suspended versioning state
- Suspension creates a complex mix of null versions and real version IDs
- Agent.md spec requires explicit testing before enabling suspended-bucket deletion
- Plan still works for Suspended buckets (read-only)

### Mixed scenario (old objects without versioning, new with versioning)

When versioning is enabled on a bucket that already contains objects:
- Pre-versioning objects get `VersionId = "null"` (literal string)
- New objects get real version IDs
- The tool handles both: `"null"` is a valid VersionId passed to DeleteObjects
- Deduplication uses `key + "\x00" + version_id`, so `"null"` and `""` are distinct

---

## Manifest Format

11-column CSV with header:

```
bucket,object_key,size_bytes,last_modified_date,version_id,is_latest,is_delete_marker,storage_class,inventory_snapshot_date,versioning_status_at_plan_time,query_execution_id
```

| Column | Type | Description |
|--------|------|-------------|
| `bucket` | string | Source bucket name |
| `object_key` | string | Full S3 object key (preserved exactly) |
| `size_bytes` | int64 | Object size in bytes |
| `last_modified_date` | timestamp | Last modified (UTC) |
| `version_id` | string | Version ID (empty for non-versioned, "null" for pre-versioning) |
| `is_latest` | boolean | Whether this is the current version |
| `is_delete_marker` | boolean | Always `false` (markers are excluded) |
| `storage_class` | string | S3 storage class |
| `inventory_snapshot_date` | string | Inventory snapshot date |
| `versioning_status_at_plan_time` | string | `Enabled`, `Disabled`, or `Suspended` |
| `query_execution_id` | string | Athena query execution ID |

### Plan Summary JSON

```json
{
  "timestamp": "2024-10-12T07:30:00Z",
  "account": "113612240996",
  "bucket": "my-bucket",
  "region": "us-east-1",
  "versioning": "Disabled",
  "cutoff": "2023-01-01T00:00:00Z",
  "snapshot_date": "2024-10-11",
  "query_execution_id": "abc-123-def",
  "database": "s3_inventory_db",
  "table": "my_bucket_inventory",
  "candidate_count": 11689,
  "candidate_bytes": 238321553,
  "manifest_path": "candidate-manifest.csv",
  "manifest_sha256": "a1b2c3d4..."
}
```

---

## Audit Log

Apply `--execute` produces `<manifest>.audit.jsonl` with one JSON line per object:

```json
{"ts":"2024-10-12T08:00:01Z","action":"DELETE","bucket":"my-bucket","key":"old/file.txt","version_id":"v123","result":"OK","size_bytes":1024}
{"ts":"2024-10-12T08:00:01Z","action":"DELETE","bucket":"my-bucket","key":"old/fail.txt","version_id":"v456","result":"ERROR","error":"AccessDenied: ...","size_bytes":2048}
```

Final line is a summary:

```json
{"ts":"2024-10-12T08:00:02Z","action":"SUMMARY","deleted":100,"failed":1,"manifest_hash":"a1b2c3d4..."}
```

---

## IAM Permissions

### Plan (read-only)

```json
{
  "Effect": "Allow",
  "Action": [
    "sts:GetCallerIdentity",
    "s3:GetBucketVersioning",
    "s3:GetBucketLocation",
    "s3:GetObjectLockConfiguration",
    "athena:StartQueryExecution",
    "athena:GetQueryExecution",
    "athena:GetQueryResults",
    "glue:GetTable",
    "glue:GetDatabase",
    "s3:GetObject",
    "s3:PutObject"
  ],
  "Resource": [
    "arn:aws:s3:::my-bucket",
    "arn:aws:s3:::my-bucket/*",
    "arn:aws:athena:us-east-1:113612240996:workgroup/primary",
    "arn:aws:glue:us-east-1:113612240996:catalog",
    "arn:aws:glue:us-east-1:113612240996:database/s3_inventory_db",
    "arn:aws:glue:us-east-1:113612240996:table/s3_inventory_db/*"
  ]
}
```

### Apply --execute (destructive)

```json
{
  "Effect": "Allow",
  "Action": [
    "sts:GetCallerIdentity",
    "s3:GetBucketVersioning",
    "s3:GetBucketLocation",
    "s3:GetObjectLockConfiguration",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion"
  ],
  "Resource": [
    "arn:aws:s3:::my-bucket",
    "arn:aws:s3:::my-bucket/*"
  ]
}
```

Use separate IAM roles for plan vs. execute when possible.

---

## Testing

```bash
# Run all tests
go test ./... -v

# Run specific test
go test -run TestReadManifestNullVersionIDAccepted -v

# Test coverage
go test -cover ./...
```

### Test coverage (27 tests)

| Area | Tests | What's covered |
|------|-------|----------------|
| **Manifest parsing** | 16 | Header validation, cutoff, bucket mismatch, delete markers, protected prefixes, duplicates, negative size, missing columns, null VersionID |
| **Streaming** | 2 | Stream/read agreement, limit enforcement |
| **Integrity** | 2 | SHA-256 hash generation, hash verification with tamper detection |
| **Date parsing** | 3 | 5 timestamp formats, date-only, invalid input |
| **Config** | 5 | Prefix parsing, SQL clause generation (ASCII + Unicode + quote escaping), identifier regex |
| **Retry** | 3 | Transient failure recovery, attempt exhaustion, context cancellation |
| **Null VersionID** | 3 | "null" accepted for Enabled, "null" vs "" distinct, empty rejected for versioned |

---

## Production Checklist

Before running against a production bucket:

- [ ] Confirm production bucket name, region, and versioning status
- [ ] Verify S3 Inventory is enabled with all versions and required fields
- [ ] Confirm Athena table schema matches Inventory output
- [ ] Review Object Lock, legal holds, lifecycle rules, replication
- [ ] Set appropriate cutoff date and verify with stakeholders
- [ ] Configure protected prefixes for all paths that must not be deleted
- [ ] Update `accountID` constant in `config.go` if using a different account
- [ ] Run plan and review the full manifest CSV
- [ ] Have a second person verify the manifest and summary
- [ ] Start with `--limit 10` canary and verify results
- [ ] Check CloudWatch storage metrics after canary (allow 24-48h lag)
- [ ] Gradually increase `--limit` after each verification
- [ ] Keep all audit logs and manifests for compliance
- [ ] Candidate bytes are estimates, not guaranteed bill savings

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `wrong AWS account` | Credentials point to different account | Check `aws sts get-caller-identity`, switch profile |
| `manifest hash mismatch` | Manifest modified after plan | Re-run `plan` to generate fresh manifest |
| `apply --cutoff does not match plan cutoff` | Different cutoff used in plan vs apply | Use same `--cutoff` value for both commands |
| `bucket has versioning Suspended` | Bucket versioning was suspended | Either re-enable versioning or disable it; tool refuses Suspended state for safety |
| `missing version_id for versioned bucket` | Manifest has empty VersionID for Enabled bucket | Re-run plan; object may predate versioning (should have "null" not empty) |
| `bucket has Object Lock enabled` | Object Lock prevents deletion | Review retention/legal-hold requirements before proceeding |
| `versioning state changed` | Bucket versioning changed between plan and apply | Re-run `plan` with current bucket state |
| `Athena query FAILED` | Bad SQL, missing table, permissions | Check Athena console for error details, verify table exists |
| `context deadline exceeded` | Athena query took longer than `--athena-timeout` | Increase `--athena-timeout` (default 30m), check table partitioning |
| `N delete attempts failed` | S3 returned per-object errors | Check audit.jsonl for error codes; common: AccessDenied, NoSuchVersion |
