package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

func writeTempCSV(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "manifest-*.csv")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return f.Name()
}

func TestReadManifestCutoffErrorShowsActualDate(t *testing.T) {
	csv := manifestHeader + "\n" +
		"test-bucket,key1,100,2024-06-15 00:00:00,,true,false,STANDARD,2026-10-11,Disabled,qid\n"
	f := writeTempCSV(t, csv)
	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := readManifest(f, "test-bucket", nil, cutoff)
	if err == nil {
		t.Fatal("expected error for object after cutoff")
	}
	if strings.Contains(err.Error(), "2023-01-01") {
		t.Errorf("error still shows hardcoded 2023-01-01: %s", err.Error())
	}
	if !strings.Contains(err.Error(), "2024-01-01") {
		t.Errorf("error should show actual cutoff 2024-01-01: %s", err.Error())
	}
}

func TestReadManifestAcceptsBeforeCutoff(t *testing.T) {
	csv := manifestHeader + "\n" +
		"test-bucket,key1,100,2022-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n"
	f := writeTempCSV(t, csv)
	cutoff := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	cands, err := readManifest(f, "test-bucket", nil, cutoff)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cands) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(cands))
	}
}

func TestParseDateFormats(t *testing.T) {
	cases := []string{
		"2022-08-16 10:20:30.123456",
		"2022-08-16 10:20:30.123",
		"2022-08-16 10:20:30",
		"2022-08-16",
		"2022-08-16T10:20:30Z",
	}
	for _, s := range cases {
		if _, err := parseDate(s); err != nil {
			t.Errorf("parseDate(%q) failed: %v", s, err)
		}
	}
}

func TestParseDateOnly(t *testing.T) {
	d, err := parseDate("2022-08-16")
	if err != nil {
		t.Fatalf("parseDate should handle date-only: %v", err)
	}
	if d.Year() != 2022 || d.Month() != 8 || d.Day() != 16 {
		t.Errorf("wrong date: %v", d)
	}
}

func TestParseDateInvalid(t *testing.T) {
	_, err := parseDate("not-a-date")
	if err == nil {
		t.Fatal("expected error for invalid date")
	}
}

func TestHashManifest(t *testing.T) {
	content := manifestHeader + "\ntest-bucket,k,1,2022-01-01 00:00:00,,true,false,STANDARD,2026-10-11,Disabled,qid\n"
	f := writeTempCSV(t, content)
	hash, err := hashManifest(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(hash) != 64 {
		t.Errorf("expected 64 hex chars, got %d: %s", len(hash), hash)
	}
	hash2, _ := hashManifest(f)
	if hash != hash2 {
		t.Error("same file should produce same hash")
	}
}

// TestStreamManifestAgreesWithReadManifest guards against streamManifest
// (the OOM-safe fast path used by apply --execute) drifting from
// readManifest (the authoritative, slice-based validator used by dry-run).
// Both must accept or reject each input identically and, on success,
// produce the same candidates in the same order.
func TestStreamManifestAgreesWithReadManifest(t *testing.T) {
	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	protected := []string{"reports/"}
	cases := []struct {
		name string
		csv  string
	}{
		{"valid multi-row", manifestHeader + "\n" +
			"test-bucket,key1,100,2022-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n" +
			"test-bucket,key2,200,2022-07-15 00:00:00,v2,true,false,STANDARD,2026-10-11,Disabled,qid\n"},
		{"wrong bucket", manifestHeader + "\n" +
			"other-bucket,key1,100,2022-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n"},
		{"delete marker", manifestHeader + "\n" +
			"test-bucket,key1,100,2022-06-15 00:00:00,v1,true,true,STANDARD,2026-10-11,Disabled,qid\n"},
		{"after cutoff", manifestHeader + "\n" +
			"test-bucket,key1,100,2024-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n"},
		{"invalid versioning state", manifestHeader + "\n" +
			"test-bucket,key1,100,2022-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Weird,qid\n"},
		{"protected prefix", manifestHeader + "\n" +
			"test-bucket,reports/key1,100,2022-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n"},
		{"duplicate key/version", manifestHeader + "\n" +
			"test-bucket,key1,100,2022-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n" +
			"test-bucket,key1,100,2022-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n"},
		{"invalid size", manifestHeader + "\n" +
			"test-bucket,key1,not-a-number,2022-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := writeTempCSV(t, tc.csv)

			readResult, readErr := readManifest(f, "test-bucket", protected, cutoff)

			var streamed []Candidate
			streamErr := streamManifest(f, "test-bucket", protected, cutoff, 0, func(c Candidate) error {
				streamed = append(streamed, c)
				return nil
			})

			if (readErr == nil) != (streamErr == nil) {
				t.Fatalf("disagreement on error: readManifest err=%v, streamManifest err=%v", readErr, streamErr)
			}
			if readErr != nil {
				return
			}
			if len(readResult) != len(streamed) {
				t.Fatalf("disagreement on count: readManifest=%d, streamManifest=%d", len(readResult), len(streamed))
			}
			for i := range readResult {
				if readResult[i] != streamed[i] {
					t.Errorf("disagreement at index %d: readManifest=%+v, streamManifest=%+v", i, readResult[i], streamed[i])
				}
			}
		})
	}
}

// TestStreamManifestRespectsLimit verifies streamManifest stops calling fn
// once limit candidates have been produced, without reading past that point
// into memory.
func TestStreamManifestRespectsLimit(t *testing.T) {
	csv := manifestHeader + "\n" +
		"test-bucket,key1,100,2022-06-15 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n" +
		"test-bucket,key2,100,2022-06-15 00:00:00,v2,true,false,STANDARD,2026-10-11,Disabled,qid\n" +
		"test-bucket,key3,100,2022-06-15 00:00:00,v3,true,false,STANDARD,2026-10-11,Disabled,qid\n"
	f := writeTempCSV(t, csv)
	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	var got []Candidate
	err := streamManifest(f, "test-bucket", nil, cutoff, 2, func(c Candidate) error {
		got = append(got, c)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected limit of 2 candidates, got %d", len(got))
	}
}

func TestVerifyManifestHash(t *testing.T) {
	content := manifestHeader + "\ntest-bucket,k,1,2022-01-01 00:00:00,,true,false,STANDARD,2026-10-11,Disabled,qid\n"
	csvPath := writeTempCSV(t, content)
	hash, _ := hashManifest(csvPath)
	hashPath := csvPath + ".sha256"
	os.WriteFile(hashPath, []byte(hash+"  "+csvPath+"\n"), 0644)

	if err := verifyManifestHash(csvPath, hashPath); err != nil {
		t.Fatalf("valid hash should verify: %v", err)
	}

	os.WriteFile(hashPath, []byte("badhash  "+csvPath+"\n"), 0644)
	if err := verifyManifestHash(csvPath, hashPath); err == nil {
		t.Fatal("tampered hash should fail verification")
	}
}

func TestReadManifestDuplicateKeyVersion(t *testing.T) {
	row := "test-bucket,dup-key,50,2022-06-01 00:00:00,v1,false,false,STANDARD,2026-10-11,Disabled,qid"
	csv := manifestHeader + "\n" + row + "\n" + row + "\n"
	f := writeTempCSV(t, csv)
	_, err := readManifest(f, "test-bucket", nil, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestReadManifestDeleteMarkerRejected(t *testing.T) {
	csv := manifestHeader + "\n" +
		"test-bucket,dm-key,0,2022-01-01 00:00:00,,true,true,STANDARD,2026-10-11,Disabled,qid\n"
	f := writeTempCSV(t, csv)
	_, err := readManifest(f, "test-bucket", nil, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "delete marker") {
		t.Fatalf("expected delete marker error, got %v", err)
	}
}

func TestReadManifestProtectedPrefix(t *testing.T) {
	csv := manifestHeader + "\n" +
		"test-bucket,reports/file.csv,100,2022-01-01 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n"
	f := writeTempCSV(t, csv)
	_, err := readManifest(f, "test-bucket", []string{"reports/"}, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("expected protected prefix error, got %v", err)
	}
}

func TestReadManifestBucketMismatch(t *testing.T) {
	csv := manifestHeader + "\n" +
		"wrong-bucket,k,1,2022-01-01 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n"
	f := writeTempCSV(t, csv)
	_, err := readManifest(f, "test-bucket", nil, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "wrong-bucket") {
		t.Fatalf("expected bucket mismatch error, got %v", err)
	}
}

func TestReadManifestWrongVersioningState(t *testing.T) {
	csv := manifestHeader + "\n" +
		"test-bucket,k,1,2022-01-01 00:00:00,v1,true,false,STANDARD,2026-10-11,InvalidState,qid\n"
	f := writeTempCSV(t, csv)
	_, err := readManifest(f, "test-bucket", nil, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "invalid versioning") {
		t.Fatalf("expected versioning error, got %v", err)
	}
}

func TestReadManifestNegativeSize(t *testing.T) {
	csv := manifestHeader + "\n" +
		"test-bucket,k,-5,2022-01-01 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled,qid\n"
	f := writeTempCSV(t, csv)
	_, err := readManifest(f, "test-bucket", nil, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "invalid size") {
		t.Fatalf("expected negative size error, got %v", err)
	}
}

func TestReadManifestMissingColumn(t *testing.T) {
	headerCols := strings.Split(manifestHeader, ",")
	truncatedHeader := strings.Join(headerCols[:len(headerCols)-1], ",")
	csv := truncatedHeader + "\n" +
		"test-bucket,k,1,2022-01-01 00:00:00,v1,true,false,STANDARD,2026-10-11,Disabled\n"
	f := writeTempCSV(t, csv)
	_, err := readManifest(f, "test-bucket", nil, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "missing column") {
		t.Fatalf("expected missing column error, got %v", err)
	}
}
