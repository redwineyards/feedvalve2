package decode_test

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	"feedvalve/internal/decode"
)

// fill returns an n-byte array-like slice of distinct printable bytes from start.
func fill(n int, start byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = start + byte(i)
	}
	return b
}

// secSample is a 7305 record whose every field is distinct, so a misplaced
// read shows up. Bit fields are left false; TestDecodeSecurityUpdateBits
// covers them.
func secSample() decode.SecurityUpdate {
	s := decode.SecurityUpdate{
		LogTime: 1451610000, Token: 424242, PermittedToTrade: 1, IssuedCapital: 12345.5,
		WarningQuantity: 1001, FreezeQuantity: 1002,
		IssueRate: 1003, IssueStartDate: 1004, InterestPaymentDate: 1005, IssueMaturityDate: 1006,
		MarginPercentage: 1007, MinimumLotQuantity: 1008, BoardLotQuantity: 1009, TickSize: 1010,
		ListingDate: 1011, ExpulsionDate: 1012, ReAdmissionDate: 1013, RecordDate: 1014,
		LowPriceRange: 1015, HighPriceRange: 1016, ExpiryDate: 1017,
		NoDeliveryStartDate: 1018, NoDeliveryEndDate: 1019,
		BookClosureStartDate: 1020, BookClosureEndDate: 1021, ExerciseStartDate: 1022, ExerciseEndDate: 1023,
		Multiplier: 1024, UnderlyingToken: 1025, IntrinsicValue: 1026, ExtrinsicValue: 1027,
		LocalUpdateDateTime: 1028, DeleteFlag: 'N', BasePrice: 1029,
	}
	copy(s.SecInfo[:], fill(30, 'A'))
	copy(s.CreditRating[:], fill(12, 'a'))
	copy(s.Name[:], fill(25, 'B'))
	copy(s.AssetInstrument[:], fill(6, 'C'))
	copy(s.AssetName[:], fill(10, 'D'))
	copy(s.Remark[:], fill(25, 'E'))
	for i := range s.Eligibility {
		s.Eligibility[i] = decode.MarketEligibility{Eligible: i%2 == 0, Status: int16(i + 1)}
	}
	return s
}

// secMessage encodes s as a 7305 message of length size, writing each field
// at the absolute offset of bible §4.3 rather than through any cursor.
func secMessage(size int, s decode.SecurityUpdate) []byte {
	m := make([]byte, max(size, 294))
	i16 := func(off int, v int16) { binary.BigEndian.PutUint16(m[off:], uint16(v)) }
	i32 := func(off int, v int32) { binary.BigEndian.PutUint32(m[off:], uint32(v)) }

	binary.BigEndian.PutUint32(m[4:], uint32(s.LogTime))
	binary.BigEndian.PutUint16(m[10:], 7305)
	binary.BigEndian.PutUint16(m[38:], uint16(size))
	i32(40, s.Token)
	copy(m[44:74], s.SecInfo[:])
	i16(74, s.PermittedToTrade)
	binary.BigEndian.PutUint64(m[76:], math.Float64bits(s.IssuedCapital))
	i32(84, s.WarningQuantity)
	i32(88, s.FreezeQuantity)
	copy(m[92:104], s.CreditRating[:])
	for i, e := range s.Eligibility {
		off := 104 + 3*i // stride 3, not 4
		if e.Eligible {
			m[off] = 0x80
		}
		i16(off+1, e.Status)
	}
	i16(116, s.IssueRate)
	i32(118, s.IssueStartDate)
	i32(122, s.InterestPaymentDate)
	i32(126, s.IssueMaturityDate)
	i32(130, s.MarginPercentage)
	i32(134, s.MinimumLotQuantity)
	i32(138, s.BoardLotQuantity)
	i32(142, s.TickSize)
	copy(m[146:171], s.Name[:])
	m[171] = 0xEE // reserved: must be ignored
	i32(172, s.ListingDate)
	i32(176, s.ExpulsionDate)
	i32(180, s.ReAdmissionDate)
	i32(184, s.RecordDate)
	i32(188, s.LowPriceRange)
	i32(192, s.HighPriceRange)
	i32(196, s.ExpiryDate)
	i32(200, s.NoDeliveryStartDate)
	i32(204, s.NoDeliveryEndDate)
	for bit, set := range []bool{s.ParticipateInMarketIndex, s.AON, s.MinimumFill} {
		if set {
			m[208] |= 0x80 >> bit
		}
	}
	m[209] = 0xEE // reserved CHAR: must be ignored
	i32(210, s.BookClosureStartDate)
	i32(214, s.BookClosureEndDate)
	i32(218, s.ExerciseStartDate)
	i32(222, s.ExerciseEndDate)
	i32(226, s.Multiplier)
	copy(m[230:236], s.AssetInstrument[:])
	copy(m[236:246], s.AssetName[:])
	i32(246, s.UnderlyingToken)
	i32(250, s.IntrinsicValue)
	i32(254, s.ExtrinsicValue)
	for bit, set := range []bool{s.Dividend, s.Rights, s.Bonus, s.Interest, s.AGM, s.EGM} {
		if set {
			m[258] |= 0x80 >> bit
		}
	}
	if s.ExerciseStyle {
		m[258] |= 0x01
	}
	for bit, set := range []bool{s.ExAllowed, s.ExRejectionAllowed, s.PlAllowed, s.IsThisAsset, s.IsCorporateAdjusted} {
		if set {
			m[259] |= 0x80 >> bit
		}
	}
	i32(260, s.LocalUpdateDateTime)
	m[264] = s.DeleteFlag
	copy(m[265:290], s.Remark[:])
	i32(290, s.BasePrice)
	return m[:size]
}

func TestDecodeSecurityUpdate(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		wantErr error
	}{
		{"exact length", 294, nil},
		{"one byte short", 293, decode.ErrBadLength},
		{"one byte long", 295, decode.ErrBadLength},
		{"empty", 0, decode.ErrBadLength},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got decode.SecurityUpdate
			err := decode.DecodeSecurityUpdate(secMessage(tt.size, secSample()), &got)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && got != secSample() {
				t.Errorf("decoded\n got  %+v\n want %+v", got, secSample())
			}
		})
	}
}

// Each bit flag is decoded from its own bit, big-endian (bible §4.14).
func TestDecodeSecurityUpdateBits(t *testing.T) {
	flags := []struct {
		name string
		set  func(s *decode.SecurityUpdate)
	}{
		{"ParticipateInMarketIndex", func(s *decode.SecurityUpdate) { s.ParticipateInMarketIndex = true }},
		{"AON", func(s *decode.SecurityUpdate) { s.AON = true }},
		{"MinimumFill", func(s *decode.SecurityUpdate) { s.MinimumFill = true }},
		{"Dividend", func(s *decode.SecurityUpdate) { s.Dividend = true }},
		{"Rights", func(s *decode.SecurityUpdate) { s.Rights = true }},
		{"Bonus", func(s *decode.SecurityUpdate) { s.Bonus = true }},
		{"Interest", func(s *decode.SecurityUpdate) { s.Interest = true }},
		{"AGM", func(s *decode.SecurityUpdate) { s.AGM = true }},
		{"EGM", func(s *decode.SecurityUpdate) { s.EGM = true }},
		{"ExerciseStyle", func(s *decode.SecurityUpdate) { s.ExerciseStyle = true }},
		{"ExAllowed", func(s *decode.SecurityUpdate) { s.ExAllowed = true }},
		{"ExRejectionAllowed", func(s *decode.SecurityUpdate) { s.ExRejectionAllowed = true }},
		{"PlAllowed", func(s *decode.SecurityUpdate) { s.PlAllowed = true }},
		{"IsThisAsset", func(s *decode.SecurityUpdate) { s.IsThisAsset = true }},
		{"IsCorporateAdjusted", func(s *decode.SecurityUpdate) { s.IsCorporateAdjusted = true }},
		{"Eligibility[3]", func(s *decode.SecurityUpdate) { s.Eligibility[3].Eligible = true }},
	}
	for _, f := range flags {
		t.Run(f.name, func(t *testing.T) {
			want := secSample()
			f.set(&want)
			var got decode.SecurityUpdate
			if err := decode.DecodeSecurityUpdate(secMessage(294, want), &got); err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Errorf("decoded\n got  %+v\n want %+v", got, want)
			}
		})
	}
}

func TestDecodeSecurityUpdateAllocatesNothing(t *testing.T) {
	m := secMessage(294, secSample())
	var out decode.SecurityUpdate
	allocs := testing.AllocsPerRun(100, func() {
		if err := decode.DecodeSecurityUpdate(m, &out); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("DecodeSecurityUpdate allocates %.0f times per call, want 0", allocs)
	}
}

// SecurityUpdate is copied by value off reused buffers (ADR-006).
func TestSecurityUpdateIsPointerFree(t *testing.T) {
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
			t.Errorf("%s is a %s; SecurityUpdate must hold only scalars and arrays", path, ty.Kind())
		}
	}
	walk("SecurityUpdate", reflect.TypeOf(decode.SecurityUpdate{}))
}
