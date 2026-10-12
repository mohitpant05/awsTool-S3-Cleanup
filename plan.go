package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	at "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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
