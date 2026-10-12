package main

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const manifestHeader = "bucket,object_key,size_bytes,last_modified_date,version_id,is_latest,is_delete_marker,storage_class,inventory_snapshot_date,versioning_status_at_plan_time,query_execution_id"

type Candidate struct {
	Bucket, Key                                                         string
	Size                                                                int64
	Modified, VersionID, Latest, DeleteMarker, StorageClass, Versioning string
	SnapshotDate, QueryID                                               string
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
		c := Candidate{
			Bucket:       get("bucket"),
			Key:          get("object_key"),
			Modified:     get("last_modified_date"),
			VersionID:    get("version_id"),
			Latest:       strings.ToLower(get("is_latest")),
			DeleteMarker: strings.ToLower(get("is_delete_marker")),
			StorageClass: get("storage_class"),
			SnapshotDate: get("inventory_snapshot_date"),
			Versioning:   get("versioning_status_at_plan_time"),
			QueryID:      get("query_execution_id"),
		}
		if c.Bucket != bucket {
			return nil, fmt.Errorf("manifest bucket %q != requested bucket %q", c.Bucket, bucket)
		}
		if c.Key == "" || c.DeleteMarker == "true" {
			return nil, fmt.Errorf("invalid key or delete marker candidate: %q", c.Key)
		}
		c.Size, e = strconv.ParseInt(get("size_bytes"), 10, 64)
		if e != nil || c.Size < 0 {
			return nil, fmt.Errorf("invalid size for %q", c.Key)
		}
		d, e := parseDate(c.Modified)
		if e != nil {
			return nil, e
		}
		if !d.Before(cutoff) {
			return nil, fmt.Errorf("%q modified %s is not before cutoff %s", c.Key, c.Modified, cutoff.Format("2006-01-02T15:04:05Z"))
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
func hashManifest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func verifyManifestHash(manifestPath, hashPath string) error {
	expected, err := os.ReadFile(hashPath)
	if err != nil {
		return fmt.Errorf("read hash file: %w", err)
	}
	fields := strings.Fields(strings.TrimSpace(string(expected)))
	if len(fields) == 0 {
		return errors.New("empty hash file")
	}
	actual, err := hashManifest(manifestPath)
	if err != nil {
		return err
	}
	if actual != fields[0] {
		return fmt.Errorf("manifest hash mismatch: expected %s, got %s", fields[0], actual)
	}
	return nil
}

func parseDate(s string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.000000", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05", "2006-01-02", time.RFC3339Nano} {
		if t, e := time.Parse(layout, strings.TrimSpace(s)); e == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported Inventory date %q", s)
}
