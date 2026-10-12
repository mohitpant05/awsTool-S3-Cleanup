package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	st "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func apply(args []string) error {
	f := flag.NewFlagSet("apply", flag.ContinueOnError)
	bucket := f.String("bucket", "", "target bucket")
	region := f.String("region", "us-east-1", "region")
	manifest := f.String("manifest", "candidate-manifest.csv", "reviewed manifest")
	execute := f.Bool("execute", false, "perform deletion")
	confirm := f.String("confirm-bucket", "", "must match bucket for execution")
	limit := f.Int("limit", 10, "max candidates; 0 means all")
	cutoff := f.String("cutoff", "2023-01-01T00:00:00Z", "must match plan cutoff (RFC3339)")
	excludes := f.String("exclude-prefixes", "reports/,athena-clean-test/,athena-test-data/,athena-query-results/", "protected prefixes")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *bucket == "" || *limit < 0 {
		return errors.New("apply requires --bucket and --limit >= 0")
	}
	if *execute && *confirm != *bucket {
		return errors.New("--execute requires --confirm-bucket to exactly match --bucket")
	}
	cutoffTime, err := time.Parse(time.RFC3339, *cutoff)
	if err != nil {
		return fmt.Errorf("invalid --cutoff: %w", err)
	}
	hashPath := *manifest + ".sha256"
	if err := verifyManifestHash(*manifest, hashPath); err != nil {
		return fmt.Errorf("manifest integrity check failed: %w", err)
	}
	log.Printf("Manifest integrity verified (SHA-256)")

	if !*execute {
		// Dry-run needs the full set in memory to print a preview summary, and
		// --limit is typically small for a preview, so readManifest's one-shot
		// slice load is fine here.
		cs, err := readManifest(*manifest, *bucket, parsePrefixes(*excludes), cutoffTime)
		if err != nil {
			return err
		}
		if *limit > 0 && len(cs) > *limit {
			cs = cs[:*limit]
		}
		var bytes int64
		for _, c := range cs {
			bytes += c.Size
		}
		log.Printf("Selected candidates: %d | summed bytes: %d (%.2f MiB)", len(cs), bytes, float64(bytes)/(1<<20))
		for _, c := range cs {
			log.Printf("CANDIDATE key=%q version_id=%q latest=%s size=%d modified=%s", c.Key, c.VersionID, c.Latest, c.Size, c.Modified)
		}
		log.Printf("DRY RUN — no deletions performed")
		return nil
	}

	ctx := context.Background()
	cfg, err := awsConfig(ctx, *region)
	if err != nil {
		return err
	}
	client := s3.NewFromConfig(cfg)
	state, err := versioning(ctx, client, *bucket)
	if err != nil {
		return err
	}
	if err := verifyRegion(ctx, client, *bucket, *region); err != nil {
		return err
	}
	if err := checkObjectLock(ctx, client, *bucket); err != nil {
		return err
	}

	// Execute path streams the manifest row-by-row instead of loading it all
	// into memory, so a manifest with millions of candidates can't OOM the
	// process. Candidates are buffered only up to one DeleteObjects batch at
	// a time and flushed as soon as the batch is full.
	//
	// The manifest is streamed twice: a validation-only pass first confirms
	// every candidate still matches the bucket's current versioning state
	// (mirrors the previous "all candidates checked before any delete"
	// guarantee), then a second pass performs the actual batched deletes.
	// This keeps memory flat at both stages instead of buffering the whole
	// manifest once to validate and again to delete.
	validateErr := streamManifest(*manifest, *bucket, parsePrefixes(*excludes), cutoffTime, *limit, func(c Candidate) error {
		if c.Versioning != state {
			return fmt.Errorf("versioning state changed (manifest %s, bucket %s); rerun plan", c.Versioning, state)
		}
		return nil
	})
	if validateErr != nil {
		return validateErr
	}

	var total, deleted, failed int
	var batch []Candidate
	streamErr := streamManifest(*manifest, *bucket, parsePrefixes(*excludes), cutoffTime, *limit, func(c Candidate) error {
		total++
		batch = append(batch, c)
		if len(batch) >= 1000 {
			d, fl, err := deleteBatch(ctx, client, *bucket, state, batch)
			deleted += d
			failed += fl
			batch = batch[:0]
			if err != nil {
				return err
			}
		}
		return nil
	})
	if streamErr != nil {
		return streamErr
	}
	if len(batch) > 0 {
		d, fl, err := deleteBatch(ctx, client, *bucket, state, batch)
		deleted += d
		failed += fl
		if err != nil {
			return err
		}
	}
	if total == 0 {
		return nil
	}
	log.Printf("APPLY COMPLETE deleted=%d failed=%d", deleted, failed)
	if failed > 0 {
		return fmt.Errorf("%d delete attempts failed; inspect logs", failed)
	}
	return nil
}

// deleteBatch issues a single DeleteObjects call (with retry) for one batch
// of candidates and reports how many succeeded and failed.
func deleteBatch(ctx context.Context, client *s3.Client, bucket, state string, batch []Candidate) (deleted int, failed int, err error) {
	ids := make([]st.ObjectIdentifier, 0, len(batch))
	for _, c := range batch {
		o := st.ObjectIdentifier{Key: aws.String(c.Key)}
		if state == "Enabled" || state == "Suspended" {
			if c.VersionID == "" {
				return 0, 0, fmt.Errorf("missing version_id for versioned bucket key %q; refusing key-only delete", c.Key)
			}
			o.VersionId = aws.String(c.VersionID)
		}
		ids = append(ids, o)
	}
	var out *s3.DeleteObjectsOutput
	retryErr := retryWithBackoff(ctx, 3, 500*time.Millisecond, func() error {
		var e error
		out, e = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(accountID), Delete: &st.Delete{Objects: ids, Quiet: aws.Bool(false)}})
		return e
	})
	if retryErr != nil {
		log.Printf("BATCH ERROR after retries: %v", retryErr)
		return 0, len(batch), nil
	}
	deleted = len(out.Deleted)
	for _, x := range out.Errors {
		failed++
		log.Printf("DELETE ERROR key=%q version_id=%q code=%s message=%s", aws.ToString(x.Key), aws.ToString(x.VersionId), aws.ToString(x.Code), aws.ToString(x.Message))
	}
	log.Printf("Processed batch of %d (deleted=%d failed=%d)", len(batch), deleted, failed)
	return deleted, failed, nil
}
