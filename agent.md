# S3 Storage Cleanup — Agent Context & Implementation Brief

**Purpose:** This document is the canonical handoff for any AI agent or engineer continuing the S3 storage-reduction project. It captures the goal, known environment, completed investigation, intended behavior, implementation requirements, safety constraints, and unresolved work.

**Status:** Investigation and a small test-bucket Inventory/Athena proof of concept have been performed. The production cleanup tool is **not yet validated or production-ready**. Do not assume an executable or compile-tested Go project has already been delivered.

---

## 1. Executive summary

The task is to reduce S3 storage costs by identifying and safely deleting eligible objects or object versions older than a configurable cutoff date. The wider environment includes a production-scale S3 bucket reportedly around **49–57 TB** and hundreds of millions of objects, but the exact production bucket name and its current configuration must be confirmed before any production action.

The intended tool is a Go CLI that:
1. Uses S3 Inventory as its scalable metadata source.
2. Uses Athena to query Inventory data and identify eligible candidates.
3. Generates a reviewable `candidate-manifest.csv` and a summary of potential reclaimable bytes.
4. Supports a no-delete planning stage and an apply dry-run.
5. Deletes only entries explicitly present in a reviewed manifest when an explicit execution flag and bucket confirmation are supplied.
6. Handles versioned, unversioned, and versioning-suspended buckets safely.
7. Produces auditable logs and a result report.

**Primary safety principle:** `--execute` must only execute deletions represented by the reviewed manifest. It must never run a broad, implicit “delete everything older than date X” operation directly against a production bucket.

---

## 2. User goal and chosen deletion policy

The user selected this policy:

> Reclaim maximum eligible storage by deleting explicitly approved object versions older than the cutoff, including noncurrent versions, with safe delete-marker handling.

Interpretation:
- The cutoff is configurable; a prior example used `2023-01-01`.
- Include all object versions that meet the cutoff policy, not only the latest version.
- For versioned buckets, target exact `VersionId` values so the intended version is removed rather than creating a delete marker or affecting another version.
- Do not delete delete markers by default. Delete-marker cleanup should be a separate, explicit, reviewed mode because removing a marker can reveal an older version.
- For unversioned buckets, delete the object key only when the candidate has been reviewed and the tool has verified the bucket's versioning state.
- The policy must be translated into a precise, documented date rule before production: S3 Inventory `LastModifiedDate` is a timestamp; define whether “older than cutoff” means strictly `< 2023-01-01T00:00:00Z` and use UTC consistently.
- Candidate bytes are an estimate of storage potentially addressed by deletion, not a guarantee of bill reduction.

---

## 3. Environment and facts currently known

### AWS/account context
- Expected AWS account ID: `113612240996`. The tool should verify the active identity with STS before planning and before applying.
- Test bucket: `tookan-test1`.
- `tookan-test1` is in **us-east-1**. `GetBucketLocation` returning `null` is normal for us-east-1; `HeadBucket`'s `BucketRegion` response can be used to confirm region.
- User confirmed that versioning on `tookan-test1` is disabled.
- Do not infer that production bucket versioning is disabled. Check it at runtime.
- The production-scale bucket was described at different points as approximately 49 TB and approximately 57 TB, with hundreds of millions of objects. Those are approximate historical figures and need to be refreshed against current AWS metrics.
- The actual production bucket name has not been supplied in this context. Never guess it.

### Inventory proof of concept
An Inventory configuration was created for `tookan-test1`:
- Configuration ID: `tookan-test1-inventory`
- Enabled: true
- Destination: same bucket (`tookan-test1`)
- Prefix: `reports`
- Format: Parquet
- Frequency: Daily
- Included object versions: `All`
- No Inventory filter was configured.
- Requested optional fields included Size, LastModifiedDate, StorageClass, ReplicationStatus, EncryptionStatus, IntelligentTieringAccessTier, ObjectAccessControlList, ObjectOwner, and LifecycleExpirationDate.
- Inventory output keys had a double slash after `reports`, e.g.:
  `s3://tookan-test1/reports//tookan-test1/tookan-test1-inventory/...`
- That double slash complicated the direct Athena table setup. A Parquet file was copied to a clean test prefix for an independent Athena proof of concept.
- Three daily Parquet report files were observed:
  - 9 October: 617,789 bytes
  - 10 October: 618,486 bytes
  - 11 October: 618,623 bytes
  Each manifest referenced one Parquet data file.
- Hive symlink files existed for daily partitions.
- The original Hive table `s3_inventory_db.tookan_test1_inventory` returned zero rows and did not match the actual manifest schema. Treat it as a failed/incorrect table setup, not as proof that Inventory was empty.
- A clean copied Parquet snapshot was queried through:
  `s3_inventory_db.tookan_test1_full_snapshot`
  with location:
  `s3://tookan-test1/athena-clean-test/full-snapshot/`
- This test table represented the 11 October snapshot only; it was not a union of all three daily reports.
- Athena returned:
  - `23,602` rows / objects
  - Sum of reported size: `1,733,868,827` bytes (about 1.61 GiB)
  - Objects older than `2023-01-01`: `11,689`
  - Sum of their reported size: `238,321,553` bytes (about 227.3 MiB)
  - Oldest object date: `2022-08-16`
  - Newest object date: `2026-10-10`
- CloudWatch metrics for `tookan-test1` showed `StandardStorage` average around `1,734,165,280` bytes and `AllStorageTypes` average around `23,593` (the metric unit/meaning must be confirmed; do not automatically assume this number is bytes).
- The Inventory object count (`23,602`) and the observed CloudWatch value (`23,593`) differ slightly. They are broadly close but **not an exact reconciliation**. Check metric name, unit, period, timing, and snapshot age before declaring completeness.
- This proof of concept used a small test bucket, not the production bucket. The 227.3 MiB candidate estimate must not be extrapolated to production.

---

## 4. Architecture and data flow

Recommended flow:

1. **Validate configuration**
   - Confirm bucket name, AWS account, region, cutoff, Athena database/table, workgroup, query-results bucket/prefix, manifest path, and protected prefixes.
   - Refuse to proceed if required settings are missing or account/bucket identity is unexpected.

2. **Validate source metadata**
   - Verify the Inventory configuration is enabled, covers the intended bucket/prefix scope, includes all required fields and all versions where applicable, and has completed at least one successful report after the desired cutoff/reference point.
   - Read the Inventory manifest and confirm its listed files are available.
   - Check report age and completeness.
   - Do not treat a daily report as real-time. Changes after the snapshot are not reflected.

3. **Plan with Athena**
   - Query the correct Inventory table/partition(s).
   - For the selected “maximum eligible storage” policy, include both latest and noncurrent object versions that are older than the cutoff, excluding delete markers by default.
   - Do not filter to `is_latest = true`, because that would omit noncurrent versions.
   - Avoid unioning multiple full daily snapshots without deduplication and a clearly defined snapshot-selection rule.
   - Paginate Athena query results; do not assume one result page contains all rows.
   - Correctly skip Athena's first header row in `GetQueryResults`.
   - Produce a manifest and summary only after the query succeeds completely.

4. **Review**
   - The operator inspects the CSV and summary.
   - The operator verifies protected paths, data ownership, cutoff semantics, version IDs, estimated bytes, object counts, and exceptions.
   - Any change to the manifest after review should invalidate its approval/checksum.

5. **Apply dry-run**
   - Read the manifest and show the exact delete operations that would be sent.
   - No S3 delete API call should occur in dry-run mode.
   - Report candidate count, estimated bytes, bucket versioning state, and any skipped/invalid rows.

6. **Execute**
   - Require `--execute` plus an explicit `--confirm-bucket <bucket-name>` matching the manifest and runtime target.
   - Recheck AWS account identity, bucket region, and versioning state.
   - Delete only manifest entries, in batches of at most 1,000 where `DeleteObjects` is used.
   - Use exact VersionId values for versioned buckets.
   - Do not silently reinterpret a missing version ID as a current-object delete when the bucket is versioned.
   - Record every attempted item and every per-object error.
   - Use bounded retries with backoff for transient errors; do not retry permanent authorization or validation failures indefinitely.
   - Produce an apply report and exit nonzero if any deletions failed.

7. **Verify**
   - Check delete API responses and failures.
   - Re-run Inventory/Athena only after the next completed Inventory report to assess remaining candidates; Inventory is not an immediate post-delete verification source.
   - Use S3/CloudWatch storage metrics over an appropriate delay to assess actual storage change. Explain that billing/metrics may lag and bytes removed from a manifest are not identical to immediate billed savings.

---

## 5. `candidate-manifest.csv` requirements

The planning command should generate `candidate-manifest.csv`. This is the review artifact, not a command that itself deletes anything.

Recommended columns:
- `bucket`
- `object_key`
- `size_bytes`
- `last_modified_date`
- `version_id`
- `is_latest`
- `is_delete_marker`
- `storage_class`
- `inventory_snapshot_date`
- `versioning_status_at_plan_time`
- `source_inventory_manifest` or another source/report identifier
- optional `candidate_reason`

Requirements:
- Use CSV-safe quoting for keys containing commas, quotes, or newlines.
- Do not trim or normalize object keys; spaces, Unicode, `%`, `+`, and unusual characters may be valid parts of keys.
- Keep sizes as integer bytes.
- Include exact version IDs without alteration.
- Include a separate summary file or printed summary with row count and sum of eligible bytes.
- Consider adding a SHA-256 hash of the manifest to the approval record. At apply time, verify the approved hash so a changed file cannot accidentally be applied.
- Include snapshot timestamp and query execution ID for traceability.
- A CSV may be large. The implementation should document expected disk use and should not load all rows into memory if production-scale row counts make that unsafe. Streaming CSV output is preferred.

---

## 6. Precise deletion semantics

### Unversioned bucket
- There is no retained version history through S3 Versioning.
- A delete by key removes the current object and is generally not reversible unless backups/replicas exist.
- The planner must ensure that the object in the manifest is still the same eligible object at apply time, as much as feasible. Inventory can be stale. A conservative implementation can re-check metadata before deleting; note that metadata checks do not fully eliminate races.
- Do not apply if the bucket's versioning state differs from the state recorded in the manifest.

### Versioning enabled
- Delete exact object versions using `(Key, VersionId)`.
- This can remove noncurrent versions without deleting the current version.
- A version delete is irreversible through S3 Versioning once that version is permanently removed; verify backup/retention requirements first.
- Delete markers are not ordinary data versions and must be excluded from the default candidate set.

### Versioning suspended
- This state must be treated separately from both enabled and disabled.
- S3 may have a special `null` version as well as version IDs from before suspension.
- The tool must handle `VersionId = "null"` correctly when explicitly represented, and must never confuse a missing CSV field with the literal string `"null"`.
- The implementation should validate bucket state and the exact deletion request behavior before enabling production execution on suspended buckets. If not fully implemented and tested, refuse apply for suspended buckets.

### Delete markers
- Default behavior: do not include or delete delete markers.
- Removing a delete marker can cause an older object version to become visible again.
- If storage optimization later requires marker cleanup, implement a separate mode that identifies markers with no retained object versions (and/or follows a separately approved rule), generates a distinct manifest, and requires independent review. Do not casually mix marker cleanup with object-version deletion.

### Legal/compliance retention
Before production use, check Object Lock, legal holds, retention periods, replication, lifecycle rules, backup/DR obligations, and internal retention requirements. If these cannot be verified, block or scope out protected objects rather than assume deletion is allowed.

---

## 7. CLI interface proposal

Illustrative interface; exact flags can change during implementation.

### Plan
```bash
s3-cleaner plan \
  --bucket tookan-test1 \
  --region us-east-1 \
  --database s3_inventory_db \
  --table tookan_test1_full_snapshot \
  --cutoff 2023-01-01 \
  --output candidate-manifest.csv
```

Expected result:
- Confirms AWS account and bucket identity.
- Queries Athena using the configured table and cutoff.
- Writes candidate manifest and summary.
- Does not call S3 deletion APIs.

### Apply dry-run
```bash
s3-cleaner apply \
  --manifest candidate-manifest.csv
```

Expected result:
- Reads and validates manifest.
- Prints planned operations and totals.
- Makes no deletion API calls.

### Apply execute
```bash
s3-cleaner apply \
  --manifest candidate-manifest.csv \
  --execute \
  --confirm-bucket tookan-test1
```

Expected result:
- Requires explicit execution flag and exact bucket confirmation.
- Verifies identity and bucket state.
- Deletes only approved manifest entries.
- Produces an audit log/report.

Do not use the illustrative commands against production until the tool is implemented, compiled, tested, and independently reviewed. Flag names above describe the desired UX, not proof that a working binary already exists.

---

## 8. Athena query logic

The exact SQL must match the actual Inventory table schema. Do not copy SQL blindly from the failed initial table setup.

For an Inventory schema with fields analogous to:
- `bucket`
- `key`
- `size`
- `last_modified_date`
- `version_id`
- `is_latest`
- `is_delete_marker`

The conceptual filter is:

```sql
WHERE last_modified_date < TIMESTAMP '2023-01-01 00:00:00'
  AND is_delete_marker = false
```

The actual Athena type for `last_modified_date` and boolean fields must be confirmed with `DESCRIBE` and sample rows. If Inventory represents dates as strings or uses different column names/types, cast/quote accordingly. Partition pruning and table layout should be designed for the real Inventory schema.

Important:
- Do **not** add `is_latest = true` for the selected policy; noncurrent versions are eligible.
- Do not combine daily full snapshots without deduplication and explicit choice of snapshot.
- If the selected table contains only one snapshot, clearly state that in the report.
- Inventory `LastModifiedDate` is not a substitute for a business-level deletion/retention timestamp unless that is the chosen policy.
- Use a UTC cutoff rule and document boundary behavior.

---

## 9. Safety controls and hard requirements

These are release blockers, not optional nice-to-haves:

1. **Identity guard:** verify expected AWS account `113612240996` via STS; fail closed on mismatch unless explicitly overridden for a test environment.
2. **Bucket guard:** exact target bucket confirmation for execute mode.
3. **No implicit deletion:** `plan` never deletes; `apply` defaults to dry-run.
4. **Explicit execute:** destructive operations require `--execute`.
5. **Manifest-only execution:** no new broad Athena query during apply; delete only rows in the reviewed manifest.
6. **Manifest integrity:** record/verify SHA-256 or equivalent approval fingerprint.
7. **Bucket-state guard:** verify region and versioning status; fail if state differs from planning metadata.
8. **Version-specific deletion:** for versioned buckets, require exact VersionId for every candidate except a carefully designed/tested `null`-version case.
9. **Delete markers excluded by default.**
10. **Protected prefixes:** support configurable exclusions, including at minimum the test/Inventory/query-output locations:
    - `reports/`
    - `athena-clean-test/`
    - `athena-test-data/`
    - `athena-query-results/`
    Prefixes must be configurable and reviewed. Do not rely on prefix protection alone.
11. **No deletion of Inventory reports or Athena outputs** used to generate the manifest or audit evidence.
12. **Batching and retries:** obey S3 `DeleteObjects` batch limit of 1,000, inspect per-object errors, use bounded retries.
13. **Auditability:** log timestamps, AWS account, bucket, manifest hash, object key, version ID, request/result, errors, and summary. Protect logs because keys may contain sensitive information.
14. **Concurrency/race considerations:** Inventory is a snapshot and can be stale; detect or mitigate objects changed after the snapshot.
15. **Retention controls:** check Object Lock/legal holds and internal retention policy before production deletion.
16. **Failure behavior:** partial failures must be explicit; never report success if any delete failed.
17. **Idempotency:** retries must not accidentally delete a different version or silently act on a changed object.
18. **Cost control:** query only needed fields/partitions, avoid scanning multiple snapshots unnecessarily, and estimate Athena query costs before running at scale.

---

## 10. IAM permissions to consider

Use least privilege and separate planning from execution if feasible.

Planning may need:
- STS `GetCallerIdentity`
- S3 bucket location, versioning, Inventory configuration/list/read permissions
- S3 `GetObject` for Inventory manifests and data files as needed
- Athena query execution/status/results permissions
- Glue Data Catalog read permissions for database/table metadata
- S3 write access to Athena query results / manifest output location if the application writes there

Execution may need:
- STS `GetCallerIdentity`
- S3 bucket location and versioning read permissions
- S3 `DeleteObject` for unversioned keys
- S3 `DeleteObjectVersion` for explicit version deletions
- Potentially `s3:ListBucketVersions` or metadata read permissions for verification and safe validation

Exact permissions depend on whether Athena runs in the console/workgroup or via the Go SDK, where manifests/results are stored, and whether the tool performs pre-delete checks. Avoid broad `s3:*` in the production execution role. Ensure permissions are limited to the intended bucket and paths.

---

## 11. Testing plan before production

### Unit tests
- CSV quoting and unusual keys.
- Integer size summation and overflow safety.
- Cutoff boundary: just before, exactly at, and after cutoff.
- Missing VersionId vs literal `"null"`.
- `is_delete_marker` true/false handling.
- Header-row skipping in Athena results.
- Athena result pagination.
- Manifest hash mismatch.
- Protected-prefix matching (including prefix-boundary cases).

### Integration tests on a dedicated disposable bucket
Create objects and versions representing:
- Old and new unversioned objects.
- Versioned object with old noncurrent version and new current version.
- Object with multiple old noncurrent versions.
- Delete marker with previous versions.
- Suspended bucket with a null version, if this state is supported.
- Protected-prefix objects.
- Object modified after Inventory snapshot.
- Simulated per-object deletion failure.
Verify:
- `plan` only emits eligible candidates.
- Dry-run makes no delete calls.
- `--execute` deletes exactly manifest entries and no others.
- Current/newer versions remain intact when deleting old noncurrent versions.
- Delete markers are not deleted by default.
- Errors are logged and the process exits nonzero on partial failure.
- Running the same manifest again does not cause unintended deletion.

### Production canary
- Start with a dedicated test bucket or a tightly scoped production prefix approved by the data owner.
- Review CSV manually and have a second person approve the manifest hash.
- Start with a small candidate subset.
- Verify the effect before increasing batch size.
- Keep logs and manifest for audit/rollback planning (note: manifest does not itself restore deleted objects).

---

## 12. Operational/reporting requirements

At plan time report:
- AWS account ID, region, bucket, versioning status.
- Inventory configuration and snapshot/report timestamp.
- Athena database/table/workgroup/query execution ID.
- Cutoff rule and timezone.
- Total candidate rows.
- Total candidate bytes and GiB/TiB conversion.
- Breakdown by storage class, latest/noncurrent, and possibly age bands.
- Count/bytes skipped by protected prefixes or invalid metadata.
- Warning when the Inventory snapshot is stale or does not cover the target scope.

At apply time report:
- Manifest hash and approval reference.
- Number of rows read, valid candidates, skipped rows.
- Planned deletions.
- Successful deletions and failed deletions.
- Estimated bytes represented by successfully deleted candidates.
- Error details per key/version.
- Final exit status and timestamp.

Be precise: “candidate bytes” is not equal to guaranteed bill savings. Noncurrent versions can consume billable storage; deleting a version can reclaim its stored bytes, but replication, storage class minimums, incomplete multipart uploads, metrics lag, and other factors affect actual savings.

---

## 13. Known unknowns and questions to resolve

1. What is the exact production bucket name and region?
2. Is production versioning enabled, suspended, or disabled?
3. Is Inventory enabled on production, and does it include all versions?
4. Is the report fresh enough and complete for the desired cleanup scope?
5. What is the correct Athena database/table schema for production?
6. What are the S3 Object Lock, legal-hold, lifecycle, replication, and retention requirements?
7. What is the approved cutoff date and does it apply to `LastModifiedDate` or a business-level deletion timestamp?
8. Which prefixes must always be protected?
9. Is deletion of old current versions allowed, or should the policy be restricted to noncurrent versions unless separately approved?
10. Who approves the candidate manifest and how is approval recorded?
11. Where should audit logs and the generated CSV be stored, and how long should they be retained?
12. What is the actual production object count/size today? Prior estimates of 49–57 TB and hundreds of millions of objects are approximate and need confirmation.

Do not fill these unknowns by guessing. Ask the operator or inspect AWS configuration using read-only commands.

---

## 14. Immediate next steps for the next agent

1. **Do not assume the Go project already exists or compiles.** Earlier file-generation attempts failed and no verified working binary/source artifact is established in this context.
2. Inspect current project files if supplied; otherwise create the Go module and source cleanly.
3. Implement a safe plan path first and test it against `tookan-test1`/the clean test Athena table.
4. Verify the Inventory schema with Athena `DESCRIBE` and sample records.
5. Ensure the plan output is a complete, paginated, streaming CSV with correct version IDs and an explicit snapshot ID.
6. Implement dry-run apply with no deletion API calls and test that property.
7. Implement exact-manifest execution only after integration tests pass.
8. Handle versioning enabled/disabled; either fully test suspended-version behavior or explicitly refuse apply for suspended buckets.
9. Test delete-marker exclusion and protected-prefix matching.
10. Only after validation, run a small approved canary. Do not jump directly to the production-scale bucket.

---

## 15. Current answer to the `--execute` question

`plan` generates the CSV and performs no deletion. `apply` without `--execute` previews the manifest actions and must not call S3 deletion APIs. `apply --execute --confirm-bucket <exact-name>` is intended to submit actual S3 delete requests for the manifest entries, after identity, bucket, versioning, and manifest checks.

The exact command interface is a design proposal unless the next agent inspects or creates the real implementation. Do not tell an operator that execution is safe merely because the flags exist; first compile, test, and review the implementation.

---

## 16. Reference command snippets for read-only investigation

Confirm identity:
```bash
aws sts get-caller-identity
```

Confirm bucket region:
```bash
aws s3api head-bucket --bucket tookan-test1
```

Confirm versioning:
```bash
aws s3api get-bucket-versioning --bucket tookan-test1 --region us-east-1
```

Inspect Inventory configurations:
```bash
aws s3api list-bucket-inventory-configurations \
  --bucket tookan-test1 \
  --region us-east-1
```

Inspect Athena table schema:
```sql
DESCRIBE s3_inventory_db.tookan_test1_full_snapshot;
```

Inspect sample records:
```sql
SELECT *
FROM s3_inventory_db.tookan_test1_full_snapshot
LIMIT 10;
```

These examples use the test bucket and test table only. Replace names only after confirming the actual target and its configuration. Read-only inspection commands are not deletion commands.

---

## 17. Handoff instruction to the next agent

Act as a cautious senior AWS storage/Go engineer. Preserve all safety requirements in this brief. Start by verifying the current files and the real Inventory/Athena schema. Separate facts from assumptions. Do not invent bucket names, paths, schemas, or successful test results. Do not perform destructive operations. First deliver a compilable, tested planning/dry-run implementation; then explain any remaining gaps before adding execution support. Require explicit human approval before any production deletion.
