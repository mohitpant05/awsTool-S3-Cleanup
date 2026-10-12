package main

import (
	"fmt"
	"log"
	"os"
)

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
