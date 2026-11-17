package mirror_test

import (
	"reflect"
	"testing"

	"feedvalve/internal/mirror"
)

func TestContributionSet(t *testing.T) {
	tests := []struct {
		name     string
		puts     [][2]int64 // (column, value) in call order
		wantMask uint64
		wantCols map[int]int64
	}{
		{"nothing set", nil, 0, map[int]int64{}},
		{"first column", [][2]int64{{0, 7}}, 1, map[int]int64{0: 7}},
		{"last column", [][2]int64{{mirror.MaxCols - 1, -9}}, 1 << 63, map[int]int64{63: -9}},
		{"several columns", [][2]int64{{2, 20}, {5, 50}}, 1<<2 | 1<<5, map[int]int64{2: 20, 5: 50}},
		{"same column twice, last value wins", [][2]int64{{3, 1}, {3, 2}}, 1 << 3, map[int]int64{3: 2}},
		{"zero is a value, not unset", [][2]int64{{4, 0}}, 1 << 4, map[int]int64{4: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c mirror.Contribution
			for _, p := range tt.puts {
				c.Set(int(p[0]), p[1])
			}
			if c.Mask != tt.wantMask {
				t.Errorf("Mask = %#x, want %#x", c.Mask, tt.wantMask)
			}
			for col, want := range tt.wantCols {
				if c.Cols[col] != want {
					t.Errorf("Cols[%d] = %d, want %d", col, c.Cols[col], want)
				}
			}
		})
	}
}

// at builds a contribution with the given arrival position.
func at(seq uint64, index, record uint16) mirror.Contribution {
	return mirror.Contribution{Seq: seq, Index: index, Record: record}
}

func TestContributionStamp(t *testing.T) {
	tests := []struct {
		name           string
		earlier, later mirror.Contribution
	}{
		{"later datagram", at(1, 0, 0), at(2, 0, 0)},
		{"later sub-packet, same datagram", at(5, 0, 0), at(5, 1, 0)},
		{"later record, same sub-packet", at(5, 3, 0), at(5, 3, 1)},
		{"datagram outranks sub-packet and record", at(5, 255, 255), at(6, 0, 0)},
		{"sub-packet outranks record", at(5, 0, 255), at(5, 1, 0)},
		{"large arrival stamps", at(1<<47, 0, 0), at(1<<47+1, 0, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if a, b := tt.earlier.Stamp(), tt.later.Stamp(); a >= b {
				t.Errorf("Stamp %#x is not before %#x", a, b)
			}
		})
	}
	if a, b := at(9, 2, 1), at(9, 2, 1); a.Stamp() != b.Stamp() {
		t.Errorf("same position stamps differ: %#x, %#x", a.Stamp(), b.Stamp())
	}
}

// A slice, pointer or string in Contribution would alias reused buffers and
// make queue buffers GC-scanned (ADR-006).
func TestContributionIsPointerFree(t *testing.T) {
	assertPointerFree(t, "Contribution", reflect.TypeOf(mirror.Contribution{}))
}

// assertPointerFree fails t if type ty, named name, holds anything but
// scalars, structs and fixed-size arrays.
func assertPointerFree(t *testing.T, name string, ty reflect.Type) {
	t.Helper()
	var walk func(path string, ty reflect.Type)
	walk = func(path string, ty reflect.Type) {
		switch ty.Kind() {
		case reflect.Struct:
			for i := range ty.NumField() {
				walk(path+"."+ty.Field(i).Name, ty.Field(i).Type)
			}
		case reflect.Array:
			walk(path+"[]", ty.Elem())
		case reflect.Pointer, reflect.Slice, reflect.String, reflect.Map,
			reflect.Chan, reflect.Func, reflect.Interface, reflect.UnsafePointer:
			t.Errorf("%s is a %s; %s must hold only scalars and arrays", path, ty.Kind(), name)
		}
	}
	walk(name, ty)
}
