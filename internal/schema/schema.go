// Package schema loads the column-mapping file: for each destination table,
// its name, its key column, and which mirror field each other column holds.
// The mirror itself only knows numbered columns; this file gives them names
// and database columns, so tables and columns change by editing the file,
// not the code.
//
// A destination's position in the file is its number (Contribution.Dest) and
// its place in every transaction's fixed write order (HLD §8.3). A column's
// position in its list is its mirror slot (Contribution.Cols index).
//
// Names in the file are turned into SQL text, so every table, key and column
// name must be a plain Oracle identifier, optionally schema-qualified.
//
// Rows are only updated, never inserted: each table's rows are pre-loaded,
// and the key column (the token) finds them.
package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"feedvalve/internal/mirror"
)

// Type says how a column's int64 value is stored.
type Type uint8

const (
	Number  Type = iota + 1 // the value as it is
	Decimal                 // a scaled integer: value / 10^Scale
	Date                    // seconds since 1 Jan 1980 as the exchange sends them, stored as that wall-clock time
	Char                    // one character, given as its byte code
	Sysdate                 // no value: set to the database's SYSDATE on every write
)

// maxScale is the largest decimal scale: 10^18 still fits in an int64.
const maxScale = 18

// maxIdentLen is Oracle's identifier length limit (12.2 and later).
const maxIdentLen = 128

// identPart is one part of a plain Oracle identifier.
var identPart = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_$#]*$`)

// typeNames maps the file's type names onto Types.
var typeNames = map[string]Type{"number": Number, "decimal": Decimal, "date": Date, "char": Char, "sysdate": Sysdate}

// Column is one database column and the mirror field it holds.
type Column struct {
	Name  string // database column
	Field string // mirror field name; empty for Sysdate
	Type  Type   // how the value is stored
	Scale int    // decimal places, for Decimal only
}

// Dest is one destination table. Columns[i] is held in mirror slot i.
type Dest struct {
	Table   string   // table name, optionally schema-qualified
	Key     string   // the single numeric key column; holds the row key
	Columns []Column // the other columns, in slot order
}

// Ref is one place a mirror field is written to.
type Ref struct {
	Dest uint8 // destination number
	Slot int   // mirror slot within it
}

// Schema is the loaded mapping. Read-only once loaded, so safe to share.
type Schema struct {
	Dests []Dest           // the destinations, in write order
	refs  map[string][]Ref // field name → every place it is written
}

// file, destFile and colFile are the JSON layout of the mapping file.
type file struct {
	Destinations []destFile `json:"destinations"`
}

type destFile struct {
	Table   string    `json:"table"`
	Key     string    `json:"key"`
	Columns []colFile `json:"columns"`
}

type colFile struct {
	Column string `json:"column"`
	Field  string `json:"field"`
	Type   string `json:"type"`
	Scale  int    `json:"scale"`
}

// Load reads and checks the mapping file at path. Unknown keys are errors,
// and every problem found is reported together.
func Load(path string) (*Schema, error) {
	var (
		b   []byte        // raw file contents
		f   file          // the decoded file
		dec *json.Decoder // strict decoder
		s   *Schema       // the result
		err error         // result of each step
	)

	b, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	dec = json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("decode schema %s: %w", path, err)
	}
	s, err = build(&f)
	if err != nil {
		return nil, fmt.Errorf("invalid schema %s: %w", path, err)
	}
	return s, nil
}

// build checks the decoded file and turns it into a Schema.
func build(f *file) (*Schema, error) {
	var (
		errs []error         // every problem found
		s    *Schema         // the result
		d    *Dest           // destination being built
		t    Type            // a column's type
		ok   bool            // whether the type name is known
		cols map[string]bool // column names seen in this destination
		flds map[string]bool // field names seen in this destination
	)

	s = &Schema{refs: make(map[string][]Ref)}
	if n := len(f.Destinations); n < 1 || n > mirror.Dests {
		errs = append(errs, fmt.Errorf("destinations must number 1..%d, got %d", mirror.Dests, n))
	}
	for i, df := range f.Destinations {
		cols = make(map[string]bool, len(df.Columns))
		flds = make(map[string]bool, len(df.Columns))
		s.Dests = append(s.Dests, Dest{Table: df.Table, Key: df.Key})
		d = &s.Dests[len(s.Dests)-1]
		if !validIdent(df.Table, true) {
			errs = append(errs, fmt.Errorf("destinations[%d].table %q is not a plain Oracle identifier", i, df.Table))
		}
		if !validIdent(df.Key, false) {
			errs = append(errs, fmt.Errorf("destinations[%d].key %q is not a plain Oracle identifier", i, df.Key))
		}
		if len(df.Columns) > mirror.MaxCols {
			errs = append(errs, fmt.Errorf("destinations[%d] has %d columns, at most %d fit a mirror row", i, len(df.Columns), mirror.MaxCols))
		}
		for j, cf := range df.Columns {
			switch {
			case !validIdent(cf.Column, false):
				errs = append(errs, fmt.Errorf("destinations[%d].columns[%d].column %q is not a plain Oracle identifier", i, j, cf.Column))
			case strings.EqualFold(cf.Column, df.Key):
				errs = append(errs, fmt.Errorf("destinations[%d].columns[%d] %q is the key column", i, j, cf.Column))
			case cols[strings.ToUpper(cf.Column)]:
				errs = append(errs, fmt.Errorf("destinations[%d].columns[%d]: duplicate column %q", i, j, cf.Column))
			}
			cols[strings.ToUpper(cf.Column)] = true
			t, ok = typeNames[cf.Type]
			switch {
			case t == Sysdate && cf.Field != "":
				errs = append(errs, fmt.Errorf("destinations[%d].columns[%d] is sysdate and takes no field", i, j))
			case t == Sysdate:
			case cf.Field == "":
				errs = append(errs, fmt.Errorf("destinations[%d].columns[%d].field is required", i, j))
			case flds[cf.Field]:
				errs = append(errs, fmt.Errorf("destinations[%d].columns[%d]: duplicate field %q", i, j, cf.Field))
			}
			flds[cf.Field] = true
			switch {
			case !ok:
				errs = append(errs, fmt.Errorf("destinations[%d].columns[%d].type %q must be number, decimal, date, char or sysdate", i, j, cf.Type))
			case t == Decimal && (cf.Scale < 1 || cf.Scale > maxScale):
				errs = append(errs, fmt.Errorf("destinations[%d].columns[%d].scale must be 1..%d for a decimal, got %d", i, j, maxScale, cf.Scale))
			case t != Decimal && cf.Scale != 0:
				errs = append(errs, fmt.Errorf("destinations[%d].columns[%d].scale is only for decimals", i, j))
			}
			d.Columns = append(d.Columns, Column{Name: cf.Column, Field: cf.Field, Type: t, Scale: cf.Scale})
			if cf.Field != "" {
				s.refs[cf.Field] = append(s.refs[cf.Field], Ref{Dest: uint8(i), Slot: j})
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return s, nil
}

// validIdent reports whether name is a plain Oracle identifier, or, if
// qualified is allowed, two of them joined by a dot.
func validIdent(name string, qualified bool) bool {
	var (
		parts []string // the dot-separated parts
	)

	parts = strings.Split(name, ".")
	if len(parts) > 2 || (len(parts) == 2 && !qualified) {
		return false
	}
	for _, p := range parts {
		if len(p) > maxIdentLen || !identPart.MatchString(p) {
			return false
		}
	}
	return true
}

// Refs returns every (destination, slot) that mirror field field is written
// to, in destination order, or nil if no column holds it. Mappers call it
// once at startup.
func (s *Schema) Refs(field string) []Ref {
	return s.refs[field]
}
