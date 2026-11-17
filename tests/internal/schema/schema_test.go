package schema_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"feedvalve/internal/schema"
)

// writeFile writes body to a temporary file and returns its path.
func writeFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const valid = `{"destinations": [
  {"table": "MD_TOKEN_QUOTE", "key": "TOKEN", "columns": [
    {"column": "LAST_TRADED_PRICE", "field": "lastTradedPrice", "type": "decimal", "scale": 2},
    {"column": "VOLUME_TRADED_TODAY", "field": "volumeTradedToday", "type": "number"},
    {"column": "EFFECTIVE_TS", "field": "logTime", "type": "date"},
    {"column": "INDCTR", "field": "indicator", "type": "char"},
    {"column": "LST_UPD_TM", "type": "sysdate"}]},
  {"table": "feed.MD_SECURITY_MASTER", "key": "TOKEN", "columns": [
    {"column": "EXPIRY", "field": "expiryDate", "type": "date"},
    {"column": "EFFECTIVE_TS", "field": "logTime", "type": "date"}]}
]}`

func TestLoadValid(t *testing.T) {
	s, err := schema.Load(writeFile(t, valid))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []schema.Dest{
		{Table: "MD_TOKEN_QUOTE", Key: "TOKEN", Columns: []schema.Column{
			{Name: "LAST_TRADED_PRICE", Field: "lastTradedPrice", Type: schema.Decimal, Scale: 2},
			{Name: "VOLUME_TRADED_TODAY", Field: "volumeTradedToday", Type: schema.Number},
			{Name: "EFFECTIVE_TS", Field: "logTime", Type: schema.Date},
			{Name: "INDCTR", Field: "indicator", Type: schema.Char},
			{Name: "LST_UPD_TM", Type: schema.Sysdate},
		}},
		{Table: "feed.MD_SECURITY_MASTER", Key: "TOKEN", Columns: []schema.Column{
			{Name: "EXPIRY", Field: "expiryDate", Type: schema.Date},
			{Name: "EFFECTIVE_TS", Field: "logTime", Type: schema.Date},
		}},
	}
	if !reflect.DeepEqual(s.Dests, want) {
		t.Errorf("Dests = %+v\nwant   %+v", s.Dests, want)
	}
}

func TestRefs(t *testing.T) {
	s, err := schema.Load(writeFile(t, valid))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	tests := []struct {
		field string
		want  []schema.Ref
	}{
		{"lastTradedPrice", []schema.Ref{{Dest: 0, Slot: 0}}},
		{"volumeTradedToday", []schema.Ref{{Dest: 0, Slot: 1}}},
		{"logTime", []schema.Ref{{Dest: 0, Slot: 2}, {Dest: 1, Slot: 1}}},
		{"expiryDate", []schema.Ref{{Dest: 1, Slot: 0}}},
		{"indicator", []schema.Ref{{Dest: 0, Slot: 3}}},
		{"", nil}, // a sysdate column holds no field
		{"noSuchField", nil},
	}
	for _, tt := range tests {
		if got := s.Refs(tt.field); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Refs(%q) = %v, want %v", tt.field, got, tt.want)
		}
	}
}

func TestTemplateLoads(t *testing.T) {
	if _, err := schema.Load(filepath.Join("..", "..", "..", "schema.json.template")); err != nil {
		t.Fatalf("schema.json.template does not load: %v", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := schema.Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("Load of a missing file succeeded")
	}
}

// dest builds a one-destination file with the given table, key and column objects.
func dest(table, key string, cols ...string) string {
	return fmt.Sprintf(`{"destinations": [{"table": %q, "key": %q, "columns": [%s]}]}`, table, key, strings.Join(cols, ","))
}

// col builds one column object.
func col(name, field, typ string, scale int) string {
	if scale == 0 {
		return fmt.Sprintf(`{"column": %q, "field": %q, "type": %q}`, name, field, typ)
	}
	return fmt.Sprintf(`{"column": %q, "field": %q, "type": %q, "scale": %d}`, name, field, typ, scale)
}

func TestLoadErrors(t *testing.T) {
	ok := col("C1", "f1", "number", 0)
	many := make([]string, 65)
	for i := range many {
		many[i] = col(fmt.Sprintf("C%d", i), fmt.Sprintf("f%d", i), "number", 0)
	}
	four := `{"destinations": [` + strings.Repeat(`{"table": "T", "key": "K", "columns": []},`, 3) + `{"table": "T", "key": "K", "columns": []}]}`

	tests := []struct {
		name    string
		body    string
		wantErr []string
	}{
		{"malformed JSON", `{`, []string{"schema"}},
		{"unknown key", `{"destinations": [], "bogus": 1}`, []string{"bogus"}},
		{"no destinations", `{"destinations": []}`, []string{"destinations"}},
		{"four destinations", four, []string{"destinations"}},
		{"empty table", dest("", "K", ok), []string{"destinations[0].table"}},
		{"injected table", dest("T; DROP TABLE x", "K", ok), []string{"destinations[0].table"}},
		{"empty key", dest("T", "", ok), []string{"destinations[0].key"}},
		{"invalid key", dest("T", "1K", ok), []string{"destinations[0].key"}},
		{"too-long identifier", dest(strings.Repeat("T", 129), "K", ok), []string{"destinations[0].table"}},
		{"invalid column", dest("T", "K", col("A-B", "f", "number", 0)), []string{"destinations[0].columns[0].column"}},
		{"duplicate column", dest("T", "K", ok, col("C1", "f2", "number", 0)), []string{"duplicate column"}},
		{"column is the key", dest("T", "K", col("K", "f", "number", 0)), []string{"key column"}},
		{"empty field", dest("T", "K", col("C", "", "number", 0)), []string{"destinations[0].columns[0].field"}},
		{"duplicate field", dest("T", "K", ok, col("C2", "f1", "number", 0)), []string{"duplicate field"}},
		{"unknown type", dest("T", "K", col("C", "f", "varchar", 0)), []string{"type"}},
		{"timestamp is gone", dest("T", "K", col("C", "f", "timestamp", 0)), []string{"type"}},
		{"sysdate with a field", dest("T", "K", col("C", "f", "sysdate", 0)), []string{"sysdate"}},
		{"char without a field", dest("T", "K", col("C", "", "char", 0)), []string{"destinations[0].columns[0].field"}},
		{"decimal without scale", dest("T", "K", col("C", "f", "decimal", 0)), []string{"scale"}},
		{"decimal scale too big", dest("T", "K", col("C", "f", "decimal", 19)), []string{"scale"}},
		{"scale on a number", dest("T", "K", col("C", "f", "number", 2)), []string{"scale"}},
		{"too many columns", dest("T", "K", many...), []string{"64"}},
		{"all faults reported", dest("", "", col("", "", "x", 0)), []string{"table", "key", "column", "field", "type"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := schema.Load(writeFile(t, tt.body))
			if err == nil {
				t.Fatal("Load succeeded, want an error")
			}
			for _, w := range tt.wantErr {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not mention %q", err, w)
				}
			}
		})
	}
}
