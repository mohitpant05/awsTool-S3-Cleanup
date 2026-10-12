package main

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPrefixExcludeClauseUsesRuneCount(t *testing.T) {
	prefix := "données/"
	byteLen := len(prefix)
	runeLen := utf8.RuneCountInString(prefix)
	if byteLen == runeLen {
		t.Skip("test only meaningful for multi-byte prefixes")
	}
	sql := prefixExcludeClause(prefix)
	wantSubstr := fmt.Sprintf(`substr("key", 1, %d)`, runeLen)
	badSubstr := fmt.Sprintf(`substr("key", 1, %d)`, byteLen)
	if !strings.Contains(sql, wantSubstr) {
		t.Errorf("expected SQL to use rune count %d: %s", runeLen, sql)
	}
	if strings.Contains(sql, badSubstr) {
		t.Errorf("SQL uses byte length %d instead of rune length %d: %s", byteLen, runeLen, sql)
	}
}

func TestPrefixExcludeClauseEscapesQuotes(t *testing.T) {
	sql := prefixExcludeClause("o'brien/")
	if !strings.Contains(sql, "o''brien/") {
		t.Errorf("expected escaped quote in SQL: %s", sql)
	}
}

func TestPrefixExcludeClauseASCII(t *testing.T) {
	sql := prefixExcludeClause("reports/")
	if !strings.Contains(sql, `substr("key", 1, 8)`) {
		t.Errorf("expected rune count 8 for ascii prefix: %s", sql)
	}
}

func TestParsePrefixesEdgeCases(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a,b,c", []string{"a", "b", "c"}},
		{" a , b ,, c ", []string{"a", "b", "c"}},
		{",,,", nil},
		{"only/", []string{"only/"}},
	}
	for _, c := range cases {
		got := parsePrefixes(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("parsePrefixes(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestIdentifierRegex(t *testing.T) {
	valid := []string{"s3_inventory_db", "table1", "ABC_123", "_leading"}
	for _, v := range valid {
		if !identifier.MatchString(v) {
			t.Errorf("identifier regex rejected valid input %q", v)
		}
	}
	invalid := []string{"bad-name", "bad name", "bad.name", "bad;drop table", ""}
	for _, v := range invalid {
		if identifier.MatchString(v) {
			t.Errorf("identifier regex accepted invalid input %q", v)
		}
	}
}
