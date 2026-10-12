package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	at "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	st "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const accountID = "113612240996"
const manifestHeader = "bucket,object_key,size,last_modified_date,version_id,is_latest,is_delete_marker,versioning_status"

var identifier = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

type Candidate struct {
	Bucket, Key                                           string
	Size                                                  int64
	Modified, VersionID, Latest, DeleteMarker, Versioning string
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "plan":
		err = plan(os.Args[2:])
	case "apply":
		err = apply(os.Args[2:])
	case "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}
func usage() {
	fmt.Fprintln(os.Stderr, `S3 Inventory cleanup (dry-run first)
plan: query Athena, write candidate manifest, estimate reclaimable bytes; never deletes
apply: validate manifest and dry-run; deletion requires --execute --confirm-bucket
Example plan:
 go run . plan --bucket tookan-test1 --region us-east-1 --database s3_inventory_db --table tookan_test1_full_snapshot --athena-output s3://tookan-test1/athena-query-results/ --cutoff 2023-01-01T00:00:00Z --manifest candidates.csv
Example dry run:
 go run . apply --bucket tookan-test1 --region us-east-1 --manifest candidates.csv --limit 10
Example execution after review:
 go run . apply --bucket tookan-test1 --region us-east-1 --manifest candidates.csv --limit 10 --execute --confirm-bucket tookan-test1
Versioned buckets: deletes exact object versions. Suspended buckets support VersionId "null". Delete markers are preserved to avoid revealing old versions.`)
}

func awsConfig(ctx context.Context, region string) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return cfg, err
	}
	ident, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return cfg, fmt.Errorf("STS identity check: %w", err)
	}
	if aws.ToString(ident.Account) != accountID {
		return cfg, fmt.Errorf("wrong AWS account %q; expected %q", aws.ToString(ident.Account), accountID)
	}
	log.Printf("AWS account verified: %s", aws.ToString(ident.Account))
	return cfg, nil
}
func versioning(ctx context.Context, c *s3.Client, bucket string) (string, error) {
	out, err := c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		return "", err
	}
	switch out.Status {
	case st.BucketVersioningStatusEnabled:
		return "Enabled", nil
	case st.BucketVersioningStatusSuspended:
		return "Suspended", nil
	default:
		return "Disabled", nil
	}
}
func parsePrefixes(s string) []string {
	var r []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			r = append(r, p)
		}
	}
	return r
}
func plan(args []string) error {
	f := flag.NewFlagSet("plan", flag.ContinueOnError)
	bucket := f.String("bucket", "", "source bucket")
	region := f.String("region", "us-east-1", "AWS region")
	db := f.String("database", "s3_inventory_db", "Athena database")
	table := f.String("table", "tookan_test1_full_snapshot", "Inventory table")
	output := f.String("athena-output", "", "writable Athena query results S3 URI")
	cutoff := f.String("cutoff", "2023-01-01T00:00:00Z", "exclusive cutoff, RFC3339 UTC")
	manifest := f.String("manifest", "candidate-manifest.csv", "local manifest path")
	wg := f.String("workgroup", "primary", "Athena workgroup")
	excludes := f.String("exclude-prefixes", "reports/,athena-clean-test/,athena-test-data/,athena-query-results/", "comma-separated protected prefixes")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *bucket == "" || *output == "" || !strings.HasPrefix(*output, "s3://") {
		return errors.New("plan requires --bucket and --athena-output s3://...")
	}
	if !identifier.MatchString(*db) || !identifier.MatchString(*table) {
		return errors.New("database/table must contain only letters, digits, underscores")
	}
	ct, err := time.Parse(time.RFC3339, *cutoff)
	if err != nil {
		return fmt.Errorf("bad cutoff: %w", err)
	}
	ctx := context.Background()
	cfg, err := awsConfig(ctx, *region)
	if err != nil {
		return err
	}
	sc := s3.NewFromConfig(cfg)
	state, err := versioning(ctx, sc, *bucket)
	if err != nil {
		return fmt.Errorf("GetBucketVersioning: %w", err)
	}
	log.Printf("Bucket versioning: %s", state)
	where := []string{fmt.Sprintf("bucket = '%s'", strings.ReplaceAll(*bucket, "'", "''")), "is_delete_marker = false", fmt.Sprintf("last_modified_date < TIMESTAMP '%s'", ct.UTC().Format("2006-01-02 15:04:05"))}
	for _, p := range parsePrefixes(*excludes) {
		where = append(where, fmt.Sprintf(`substr("key", 1, %d) <> '%s'`, len(p), strings.ReplaceAll(p, "'", "''")))
	}
	query := fmt.Sprintf(`SELECT bucket, "key" AS object_key, size, last_modified_date, version_id, is_latest, is_delete_marker FROM %s.%s WHERE %s ORDER BY last_modified_date ASC`, *db, *table, strings.Join(where, " AND "))
	ac := athena.NewFromConfig(cfg)
	started, err := ac.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{QueryString: aws.String(query), QueryExecutionContext: &at.QueryExecutionContext{Database: aws.String(*db)}, WorkGroup: aws.String(*wg), ResultConfiguration: &at.ResultConfiguration{OutputLocation: aws.String(*output)}})
	if err != nil {
		return fmt.Errorf("start Athena query: %w", err)
	}
	qid := aws.ToString(started.QueryExecutionId)
	log.Printf("Athena query ID: %s", qid)
	if err = wait(ctx, ac, qid); err != nil {
		return err
	}
	file, err := os.Create(*manifest)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	if err = w.Write(strings.Split(manifestHeader, ",")); err != nil {
		return err
	}
	var rows, bytes int64
	var token *string
	first := true
	for {
		page, e := ac.GetQueryResults(ctx, &athena.GetQueryResultsInput{QueryExecutionId: aws.String(qid), NextToken: token, MaxResults: aws.Int32(1000)})
		if e != nil {
			return e
		}
		for i, row := range page.ResultSet.Rows {
			if first && i == 0 {
				continue
			}
			vals := make([]string, 7)
			for j := 0; j < len(vals) && j < len(row.Data); j++ {
				if row.Data[j].VarCharValue != nil {
					vals[j] = aws.ToString(row.Data[j].VarCharValue)
				}
			}
			if vals[0] != *bucket || vals[1] == "" {
				return fmt.Errorf("unexpected Athena row: %#v", vals)
			}
			sz, e := strconv.ParseInt(vals[2], 10, 64)
			if e != nil || sz < 0 {
				return fmt.Errorf("invalid size for key %q", vals[1])
			}
			if strings.EqualFold(vals[6], "true") {
				return fmt.Errorf("delete marker returned in candidate query for %q", vals[1])
			}
			if e = w.Write(append(vals, state)); e != nil {
				return e
			}
			rows++
			bytes += sz
		}
		first = false
		token = page.NextToken
		if token == nil || *token == "" {
			break
		}
	}
	w.Flush()
	if err = w.Error(); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	log.Printf("PLAN ONLY — no deletions performed")
	log.Printf("Candidates (object versions): %d", rows)
	log.Printf("Summed candidate size: %d bytes | %.2f MiB | %.2f GiB | %.6f TiB", bytes, float64(bytes)/(1<<20), float64(bytes)/(1<<30), float64(bytes)/(1<<40))
	log.Printf("Manifest: %s", *manifest)
	log.Printf("Delete markers excluded/preserved; bytes are an estimate, not a guarantee of immediate metric decrease")
	return nil
}
func wait(ctx context.Context, c *athena.Client, id string) error {
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for {
		r, e := c.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: aws.String(id)})
		if e != nil {
			return e
		}
		if r.QueryExecution == nil || r.QueryExecution.Status == nil {
			return errors.New("Athena response has no status")
		}
		s := r.QueryExecution.Status
		switch s.State {
		case at.QueryExecutionStateSucceeded:
			return nil
		case at.QueryExecutionStateFailed, at.QueryExecutionStateCancelled:
			return fmt.Errorf("Athena query %s: %s", s.State, aws.ToString(s.StateChangeReason))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
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
	if !*execute {
		log.Printf("DRY RUN — no deletions performed")
		return nil
	}
	if len(cs) == 0 {
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
	for _, c := range cs {
		if c.Versioning != state {
			return fmt.Errorf("versioning state changed (manifest %s, bucket %s); rerun plan", c.Versioning, state)
		}
	}
	var deleted, failed int
	for start := 0; start < len(cs); start += 1000 {
		end := start + 1000
		if end > len(cs) {
			end = len(cs)
		}
		batch := cs[start:end]
		ids := make([]st.ObjectIdentifier, 0, len(batch))
		for _, c := range batch {
			o := st.ObjectIdentifier{Key: aws.String(c.Key)}
			if state == "Enabled" || state == "Suspended" {
				if c.VersionID == "" {
					return fmt.Errorf("missing version_id for versioned bucket key %q; refusing key-only delete", c.Key)
				}
				o.VersionId = aws.String(c.VersionID)
			}
			ids = append(ids, o)
		}
		out, e := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(*bucket), ExpectedBucketOwner: aws.String(accountID), Delete: &st.Delete{Objects: ids, Quiet: aws.Bool(false)}})
		if e != nil {
			failed += len(batch)
			log.Printf("BATCH ERROR: %v", e)
			continue
		}
		deleted += len(out.Deleted)
		for _, x := range out.Errors {
			failed++
			log.Printf("DELETE ERROR key=%q version_id=%q code=%s message=%s", aws.ToString(x.Key), aws.ToString(x.VersionId), aws.ToString(x.Code), aws.ToString(x.Message))
		}
		log.Printf("Processed %d/%d", end, len(cs))
	}
	log.Printf("APPLY COMPLETE deleted=%d failed=%d", deleted, failed)
	if failed > 0 {
		return fmt.Errorf("%d delete attempts failed; inspect logs", failed)
	}
	return nil
}
func readManifest(path, bucket string, protected []string, cutoff time.Time) ([]Candidate, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	r := csv.NewReader(f)
	h, e := r.Read()
	if e != nil {
		return nil, e
	}
	cols := map[string]int{}
	for i, s := range h {
		cols[strings.ToLower(strings.TrimSpace(s))] = i
	}
	for _, n := range strings.Split(manifestHeader, ",") {
		if _, ok := cols[n]; !ok {
			return nil, fmt.Errorf("manifest missing column %s", n)
		}
	}
	cutoff = cutoff.UTC()
	seen := map[string]bool{}
	var result []Candidate
	for {
		row, e := r.Read()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		get := func(n string) string {
			i := cols[n]
			if i >= len(row) {
				return ""
			}
			return strings.TrimSpace(row[i])
		}
		c := Candidate{Bucket: get("bucket"), Key: get("object_key"), Modified: get("last_modified_date"), VersionID: get("version_id"), Latest: strings.ToLower(get("is_latest")), DeleteMarker: strings.ToLower(get("is_delete_marker")), Versioning: get("versioning_status")}
		if c.Bucket != bucket {
			return nil, fmt.Errorf("manifest bucket %q != requested bucket %q", c.Bucket, bucket)
		}
		if c.Key == "" || c.DeleteMarker == "true" {
			return nil, fmt.Errorf("invalid key or delete marker candidate: %q", c.Key)
		}
		c.Size, e = strconv.ParseInt(get("size"), 10, 64)
		if e != nil || c.Size < 0 {
			return nil, fmt.Errorf("invalid size for %q", c.Key)
		}
		d, e := parseDate(c.Modified)
		if e != nil {
			return nil, e
		}
		if !d.Before(cutoff) {
			return nil, fmt.Errorf("%q is not before cutoff 2023-01-01", c.Key)
		}
		if c.Versioning != "Enabled" && c.Versioning != "Suspended" && c.Versioning != "Disabled" {
			return nil, fmt.Errorf("invalid versioning state %q", c.Versioning)
		}
		for _, p := range protected {
			if strings.HasPrefix(c.Key, p) {
				return nil, fmt.Errorf("%q matches protected prefix %q", c.Key, p)
			}
		}
		identity := c.Key + "\x00" + c.VersionID
		if seen[identity] {
			return nil, fmt.Errorf("duplicate key/version %q / %q", c.Key, c.VersionID)
		}
		seen[identity] = true
		result = append(result, c)
	}
	return result, nil
}
func parseDate(s string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.000000", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05", time.RFC3339Nano} {
		if t, e := time.Parse(layout, strings.TrimSpace(s)); e == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported Inventory date %q", s)
}
