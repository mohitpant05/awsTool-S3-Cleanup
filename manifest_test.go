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
		"test-bucket,key1,100,2024-06-15 00:00:00,,true,false,Disabled\n"
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
		"test-bucket,key1,100,2022-06-15 00:00:00,v1,true,false,Disabled\n"
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
