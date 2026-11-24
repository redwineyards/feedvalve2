package decode

import "encoding/binary"

// SecurityUpdateLen is the declared length of a 7305 message
// (MS_SECURITY_UPDATE_INFO), header included (bible §4.3).
const SecurityUpdateLen = 294

// secTokenOff is where the body starts, right after BCAST_HEADER.
const secTokenOff = headerLen

// secBodyLen is the body's length; the bible verifies there is no padding
// anywhere, so the fields sum to exactly this.
const secBodyLen = 4 + 30 + 2 + 8 + 4 + 4 + 12 + 4*3 + 2 + 4*4 + 4*3 + 25 + 1 +
	4*9 + 2 + 4*5 + 6 + 10 + 4*3 + 2 + 4 + 1 + 25 + 4

// The body must reconcile with the declared length; if it ever stops doing
// so, one of these arrays gets a negative size and the build fails.
var (
	_ [SecurityUpdateLen - (headerLen + secBodyLen)]struct{}
	_ [(headerLen + secBodyLen) - SecurityUpdateLen]struct{}
)

// MarketEligibility is one ST_SEC_ELIGIBILITY_PER_MARKET entry: 3 bytes on
// the wire, array stride 3 (bible §4.14).
type MarketEligibility struct {
	Eligible bool  // may trade in that market
	Status   int16 // 1 Pre-open, 2 Open, 3 Suspended, 4 Pre-open extended, 5 Stock Open With Market
}

// SecurityUpdate is one 7305 record (MS_SECURITY_UPDATE_INFO), plus the
// header's LogTime, exactly as sent. Prices are scaled integers (bible §2.4),
// dates are seconds since 1980 (bible §2.5), and CHAR fields are the raw
// blank-padded bytes (bible §2.6). It holds no pointers or slices (ADR-006).
type SecurityUpdate struct {
	LogTime int32 // header: message generation time

	Token               int32
	SecInfo             [30]byte // SEC_INFO, spec Ch.5; its layout is not in the bible, so kept raw
	PermittedToTrade    int16    // 0 listed but not permitted, 1 permitted
	IssuedCapital       float64
	WarningQuantity     int32
	FreezeQuantity      int32
	CreditRating        [12]byte
	Eligibility         [4]MarketEligibility // per market
	IssueRate           int16
	IssueStartDate      int32
	InterestPaymentDate int32
	IssueMaturityDate   int32
	MarginPercentage    int32
	MinimumLotQuantity  int32
	BoardLotQuantity    int32
	TickSize            int32
	Name                [25]byte
	ListingDate         int32
	ExpulsionDate       int32
	ReAdmissionDate     int32
	RecordDate          int32
	LowPriceRange       int32 // operating range, scaled
	HighPriceRange      int32 // operating range, scaled
	ExpiryDate          int32
	NoDeliveryStartDate int32
	NoDeliveryEndDate   int32

	ParticipateInMarketIndex bool // ST_ELIGIBILITY_INDICATORS
	AON                      bool
	MinimumFill              bool

	BookClosureStartDate int32
	BookClosureEndDate   int32
	ExerciseStartDate    int32
	ExerciseEndDate      int32
	Multiplier           int32
	AssetInstrument      [6]byte
	AssetName            [10]byte
	UnderlyingToken      int32
	IntrinsicValue       int32
	ExtrinsicValue       int32

	Dividend            bool // ST_PURPOSE
	Rights              bool
	Bonus               bool
	Interest            bool
	AGM                 bool
	EGM                 bool
	ExerciseStyle       bool
	ExAllowed           bool
	ExRejectionAllowed  bool
	PlAllowed           bool
	IsThisAsset         bool
	IsCorporateAdjusted bool

	LocalUpdateDateTime int32
	DeleteFlag          byte // 'N' active, 'Y' deleted
	Remark              [25]byte
	BasePrice           int32 // scaled
}

// DecodeSecurityUpdate parses the 7305 message m, from the header onward,
// into out. A message that is not exactly SecurityUpdateLen bytes returns
// ErrBadLength and leaves out unchanged.
func DecodeSecurityUpdate(m []byte, out *SecurityUpdate) error {
	var (
		r   reader // cursor over the body
		ind byte   // a bit-field byte
	)

	if len(m) != SecurityUpdateLen {
		return ErrBadLength
	}
	out.LogTime = int32(binary.BigEndian.Uint32(m[logTimeOff:]))
	r = reader{b: m, off: secTokenOff}

	out.Token = r.i32()
	r.bytes(out.SecInfo[:])
	out.PermittedToTrade = r.i16()
	out.IssuedCapital = r.f64()
	out.WarningQuantity = r.i32()
	out.FreezeQuantity = r.i32()
	r.bytes(out.CreditRating[:])
	for i := range out.Eligibility {
		ind = r.u8()
		out.Eligibility[i] = MarketEligibility{Eligible: ind&0x80 != 0, Status: r.i16()}
	}
	out.IssueRate = r.i16()
	out.IssueStartDate = r.i32()
	out.InterestPaymentDate = r.i32()
	out.IssueMaturityDate = r.i32()
	out.MarginPercentage = r.i32()
	out.MinimumLotQuantity = r.i32()
	out.BoardLotQuantity = r.i32()
	out.TickSize = r.i32()
	r.bytes(out.Name[:])
	r.pad("Reserved after Name", 1)
	out.ListingDate = r.i32()
	out.ExpulsionDate = r.i32()
	out.ReAdmissionDate = r.i32()
	out.RecordDate = r.i32()
	out.LowPriceRange = r.i32()
	out.HighPriceRange = r.i32()
	out.ExpiryDate = r.i32()
	out.NoDeliveryStartDate = r.i32()
	out.NoDeliveryEndDate = r.i32()
	ind = r.u8()
	r.pad("ST_ELIGIBILITY_INDICATORS reserved byte", 1)
	out.ParticipateInMarketIndex = ind&0x80 != 0
	out.AON = ind&0x40 != 0
	out.MinimumFill = ind&0x20 != 0
	out.BookClosureStartDate = r.i32()
	out.BookClosureEndDate = r.i32()
	out.ExerciseStartDate = r.i32()
	out.ExerciseEndDate = r.i32()
	out.Multiplier = r.i32()
	r.bytes(out.AssetInstrument[:])
	r.bytes(out.AssetName[:])
	out.UnderlyingToken = r.i32()
	out.IntrinsicValue = r.i32()
	out.ExtrinsicValue = r.i32()
	ind = r.u8()
	out.Dividend = ind&0x80 != 0
	out.Rights = ind&0x40 != 0
	out.Bonus = ind&0x20 != 0
	out.Interest = ind&0x10 != 0
	out.AGM = ind&0x08 != 0
	out.EGM = ind&0x04 != 0
	out.ExerciseStyle = ind&0x01 != 0
	ind = r.u8()
	out.ExAllowed = ind&0x80 != 0
	out.ExRejectionAllowed = ind&0x40 != 0
	out.PlAllowed = ind&0x20 != 0
	out.IsThisAsset = ind&0x10 != 0
	out.IsCorporateAdjusted = ind&0x08 != 0
	out.LocalUpdateDateTime = r.i32()
	out.DeleteFlag = r.u8()
	r.bytes(out.Remark[:])
	out.BasePrice = r.i32()
	return nil
}
