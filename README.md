# S3 Inventory Cleanup Tool (Go)

## Behavior
- `plan` queries Athena, writes a CSV candidate manifest, and reports candidate object-version count and summed bytes. It never deletes.
- `apply` is dry-run by default. `--execute --confirm-bucket BUCKET` is required to delete.
- Works with versioning Disabled, Enabled, or Suspended. For Enabled/Suspended buckets it deletes exact version IDs; the literal `null` version ID is preserved for suspended buckets.
- Queries all object versions older than the cutoff, including noncurrent versions, but excludes delete markers. Delete markers are preserved so an older hidden version cannot unexpectedly become visible.
- Verifies AWS account `113612240996` and verifies bucket versioning state has not changed since planning.

## Build
```bash
go mod tidy
go build -o s3-cleaner .
```

## Plan (test bucket)
```bash
./s3-cleaner plan \
  --bucket tookan-test1 --region us-east-1 \
  --database s3_inventory_db --table tookan_test1_full_snapshot \
  --athena-output s3://tookan-test1/athena-query-results/ \
  --cutoff 2023-01-01T00:00:00Z --manifest candidate-manifest.csv
```

## Dry run
```bash
./s3-cleaner apply --bucket tookan-test1 --region us-east-1 \
  --manifest candidate-manifest.csv --limit 10
```

## Execute approved sample
```bash
./s3-cleaner apply --bucket tookan-test1 --region us-east-1 \
  --manifest candidate-manifest.csv --limit 10 \
  --execute --confirm-bucket tookan-test1
```

`--limit 0` means all candidates. Only use after full manifest review. Use the same `--exclude-prefixes` in plan and apply. Defaults protect `reports/`, `athena-clean-test/`, `athena-test-data/`, and `athena-query-results/`.

## Important limitations before production
- The apply-side cutoff check is currently fixed to 2023-01-01. Parameterize it before using other cutoffs.
- This starter has no persistent audit log, exponential retry/backoff, resumable checkpointing, or per-object revalidation immediately before delete.
- A report table must represent one complete Inventory snapshot. Do not union daily snapshots without choosing a point-in-time report and deduplicating appropriately.
- Sum of Inventory `size` is an estimate of reclaimable object bytes; CloudWatch/storage metrics update asynchronously and may not drop by exactly that amount.
- Validate IAM, table schema, Inventory completeness, excluded prefixes and the manifest before production use.
