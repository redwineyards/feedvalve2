package stats_test

import (
	"reflect"
	"sync"
	"testing"

	"feedvalve/internal/stats"
)

func TestTranscodeCounts(t *testing.T) {
	tests := []struct {
		name string
		adds []uint16
		want map[uint16]uint64 // every transcode Each must visit, with its count
	}{
		{"empty", nil, map[uint16]uint64{}},
		{"one code", []uint16{7208, 7208, 7208}, map[uint16]uint64{7208: 3}},
		{"several codes", []uint16{7305, 6541, 7208, 6541}, map[uint16]uint64{6541: 2, 7208: 1, 7305: 1}},
		{"edges of the range", []uint16{0, 65535, 65535}, map[uint16]uint64{0: 1, 65535: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c stats.TranscodeCounts
			for _, tc := range tt.adds {
				c.Add(tc)
			}
			for tc, n := range tt.want {
				if got := c.Get(tc); got != n {
					t.Errorf("Get(%d) = %d, want %d", tc, got, n)
				}
			}
			if got := c.Get(1); tt.want[1] == 0 && got != 0 {
				t.Errorf("Get(1) = %d for a code never added, want 0", got)
			}

			got := map[uint16]uint64{}
			var order []uint16
			c.Each(func(tc uint16, n uint64) {
				got[tc] = n
				order = append(order, tc)
			})
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Each visited %v, want %v", got, tt.want)
			}
			for i := 1; i < len(order); i++ {
				if order[i-1] >= order[i] {
					t.Errorf("Each order %v is not ascending", order)
					break
				}
			}
		})
	}
}

func TestTranscodeCountsConcurrentAdd(t *testing.T) {
	const goroutines, perG = 8, 1000
	var c stats.TranscodeCounts
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perG {
				c.Add(7208)
			}
		}()
	}
	wg.Wait()
	if got := c.Get(7208); got != goroutines*perG {
		t.Errorf("Get(7208) = %d, want %d", got, goroutines*perG)
	}
}
