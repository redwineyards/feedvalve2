package db_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"feedvalve/internal/config"
	"feedvalve/internal/db"
	"feedvalve/internal/mirror"
	"feedvalve/tests/internal/fakedb"
)

// quote returns a quote whose every field is distinct and derived from base.
func quote(base int64) mirror.Quote {
	q := mirror.Quote{
		LastTradedPrice: base + 1, LastTradeQuantity: base + 2, LastTradeTime: base + 3,
		AverageTradePrice: base + 4, VolumeTradedToday: base + 5, NetChangeIndicator: '+',
		PctChangePrevClose: base + 7, ClosingPrice: base + 8, OpenPrice: base + 9,
		HighPrice: base + 10, LowPrice: base + 11, TotalBuyQuantity: base + 12, TotalSellQuantity: base + 13,
	}
	for i := range 5 {
		q.BuyPrice[i] = base + 20 + int64(i)
		q.BuyQuantity[i] = base + 30 + int64(i)
		q.SellPrice[i] = base + 40 + int64(i)
		q.SellQuantity[i] = base + 50 + int64(i)
	}
	return q
}

// batchOf returns a batch holding one quote row per token, each with quote(token*1000).
func batchOf(tokens ...int32) *mirror.Batch {
	b := mirror.NewBatch(0, len(tokens))
	for _, tok := range tokens {
		b.Rows = append(b.Rows, mirror.BatchRow{Token: tok, Quote: quote(int64(tok) * 1000), HasQuote: true})
	}
	return b
}

// wantSQL is the statement for quote rows.
const wantSQL = "UPDATE ABC_TRADE_QUOTE SET " +
	"ABC_LST_TRD_PRC = :1, ABC_LST_TRD_QTY = :2, " +
	"ABC_LST_TRD_TM = NVL(DATE '1980-01-01' + NUMTODSINTERVAL(NULLIF(:3, 0), 'SECOND'), ABC_LST_TRD_TM), " +
	"ABC_AVRG_TRD_PRC = :4, ABC_TOT_QTY_TRDD = :5, ABC_INDCTR = CHR(:6), ABC_CHNG_PRVS_CLS = :7, " +
	"ABC_CLS_PRC = :8, ABC_OPN_PRC = :9, ABC_HGH_PRC = :10, ABC_LOW_PRC = :11, " +
	"ABC_TOT_BID_QTY = :12, ABC_TOT_OFFR_QTY = :13, " +
	"ABC_BST1_BID_PRC = :14, ABC_BST1_BID_QTY = :15, ABC_BST2_BID_PRC = :16, ABC_BST2_BID_QTY = :17, " +
	"ABC_BST3_BID_PRC = :18, ABC_BST3_BID_QTY = :19, ABC_BST4_BID_PRC = :20, ABC_BST4_BID_QTY = :21, " +
	"ABC_BST5_BID_PRC = :22, ABC_BST5_BID_QTY = :23, " +
	"ABC_BST1_OFFR_PRC = :24, ABC_BST1_OFFR_QTY = :25, ABC_BST2_OFFR_PRC = :26, ABC_BST2_OFFR_QTY = :27, " +
	"ABC_BST3_OFFR_PRC = :28, ABC_BST3_OFFR_QTY = :29, ABC_BST4_OFFR_PRC = :30, ABC_BST4_OFFR_QTY = :31, " +
	"ABC_BST5_OFFR_PRC = :32, ABC_BST5_OFFR_QTY = :33, " +
	"ABC_LST_UPD_TM = SYSDATE WHERE ABC_TOKEN_NO = :34"

// wantArgs is the bind arrays for quotes from tokens, in bind order.
func wantArgs(tokens ...int32) []any {
	var args []any
	col := func(f func(q *mirror.Quote) int64) {
		vals := []int64{}
		for _, tok := range tokens {
			q := quote(int64(tok) * 1000)
			vals = append(vals, f(&q))
		}
		args = append(args, vals)
	}
	col(func(q *mirror.Quote) int64 { return q.LastTradedPrice })
	col(func(q *mirror.Quote) int64 { return q.LastTradeQuantity })
	col(func(q *mirror.Quote) int64 { return q.LastTradeTime })
	col(func(q *mirror.Quote) int64 { return q.AverageTradePrice })
	col(func(q *mirror.Quote) int64 { return q.VolumeTradedToday })
	col(func(q *mirror.Quote) int64 { return q.NetChangeIndicator })
	col(func(q *mirror.Quote) int64 { return q.PctChangePrevClose })
	col(func(q *mirror.Quote) int64 { return q.ClosingPrice })
	col(func(q *mirror.Quote) int64 { return q.OpenPrice })
	col(func(q *mirror.Quote) int64 { return q.HighPrice })
	col(func(q *mirror.Quote) int64 { return q.LowPrice })
	col(func(q *mirror.Quote) int64 { return q.TotalBuyQuantity })
	col(func(q *mirror.Quote) int64 { return q.TotalSellQuantity })
	for i := range 5 {
		col(func(q *mirror.Quote) int64 { return q.BuyPrice[i] })
		col(func(q *mirror.Quote) int64 { return q.BuyQuantity[i] })
	}
	for i := range 5 {
		col(func(q *mirror.Quote) int64 { return q.SellPrice[i] })
		col(func(q *mirror.Quote) int64 { return q.SellQuantity[i] })
	}
	keys := []int64{}
	for _, tok := range tokens {
		keys = append(keys, int64(tok))
	}
	return append(args, keys)
}

func TestCommitOneArrayBoundUpdate(t *testing.T) {
	conn, f := fakedb.Open(t)
	commit := db.NewCommit(conn, time.Second, new(db.Stats))

	if err := commit(batchOf(7, 9)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(f.Execs()) != 1 {
		t.Fatalf("ran %d statements, want 1: %+v", len(f.Execs()), f.Execs())
	}
	if f.Execs()[0].SQL != wantSQL {
		t.Errorf("SQL =\n%s\nwant\n%s", f.Execs()[0].SQL, wantSQL)
	}
	if want := wantArgs(7, 9); !reflect.DeepEqual(f.Execs()[0].Args, want) {
		t.Errorf("args = %v, want %v (values in bind order, then tokens)", f.Execs()[0].Args, want)
	}
	if f.Commits() != 1 || f.Rollbacks() != 0 {
		t.Errorf("(commits, rollbacks) = (%d, %d), want (1, 0)", f.Commits(), f.Rollbacks())
	}
}

func TestCommitEmptyBatch(t *testing.T) {
	conn, f := fakedb.Open(t)
	commit := db.NewCommit(conn, time.Second, new(db.Stats))
	if err := commit(batchOf()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(f.Execs()) != 0 {
		t.Errorf("ran %d statements for an empty batch, want 0", len(f.Execs()))
	}
}

func TestCommitCountsUnmatchedTokens(t *testing.T) {
	conn, f := fakedb.Open(t)
	f.Missing = 1
	st := new(db.Stats)
	commit := db.NewCommit(conn, time.Second, st)
	if err := commit(batchOf(1, 2)); err != nil {
		t.Fatalf("commit: %v (an unmatched token is not an error)", err)
	}
	if n := st.Unmatched.Load(); n != 1 {
		t.Errorf("Unmatched = %d, want 1", n)
	}
	if f.Commits() != 1 {
		t.Errorf("commits = %d, want 1", f.Commits())
	}
}

func TestCommitFailureRollsBack(t *testing.T) {
	conn, f := fakedb.Open(t)
	f.FailAt = 1
	commit := db.NewCommit(conn, time.Second, new(db.Stats))
	err := commit(batchOf(1))
	if err == nil {
		t.Fatal("commit succeeded, want the driver's error")
	}
	for _, want := range []string{"ABC_TRADE_QUOTE", "ORA-00001"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if f.Commits() != 0 || f.Rollbacks() != 1 {
		t.Errorf("(commits, rollbacks) = (%d, %d), want (0, 1)", f.Commits(), f.Rollbacks())
	}
}

func TestCommitTimesOut(t *testing.T) {
	conn, f := fakedb.Open(t)
	f.Block = true
	commit := db.NewCommit(conn, 50*time.Millisecond, new(db.Stats))
	start := time.Now()
	err := commit(batchOf(1))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want a deadline error", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("commit took %v, want it bounded by the 50ms timeout", d)
	}
}

// TestUpdateIntegration needs a real Oracle instance holding ABC_TRADE_QUOTE
// and GHI_CNT_MSTR (create_tables.sql) and a user that may insert and delete
// in them. It seeds rows with negative tokens, which no exchange contract
// uses, and deletes them afterwards. Set FEEDVALVE_TEST_CONFIG and FEEDVALVE_DB_PASSWORD to run it.
func TestUpdateIntegration(t *testing.T) {
	path, password := os.Getenv("FEEDVALVE_TEST_CONFIG"), os.Getenv("FEEDVALVE_DB_PASSWORD")
	if path == "" || password == "" {
		t.Skip("FEEDVALVE_TEST_CONFIG and FEEDVALVE_DB_PASSWORD not set")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	target := db.Target{Host: cfg.DB.Host, Port: cfg.DB.Port, Service: cfg.DB.Service, User: cfg.DB.User}
	conn, err := db.Open(context.Background(), target, password, time.Duration(cfg.DB.ConnectTimeoutMs)*time.Millisecond)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	cleanup := func() {
		conn.Exec("DELETE FROM ABC_TRADE_QUOTE WHERE ABC_TOKEN_NO IN (-1, -2)")
		conn.Exec("DELETE FROM GHI_CNT_MSTR WHERE GHI_TOKEN_NO = -1")
	}
	cleanup()
	defer cleanup()
	seed := "INSERT INTO ABC_TRADE_QUOTE (ABC_XCHNG_CD, ABC_PRDCT_TYP, ABC_INDSTK, ABC_UNDRLYNG, ABC_EXPRY_DT, " +
		"ABC_EXER_TYP, ABC_OPT_TYP, ABC_STRK_PRC, ABC_TOKEN_NO) " +
		"SELECT 'TST', 'F', 'S', 'FVTEST', DATE '2026-12-31', 'E', '*', 0, -LEVEL FROM dual CONNECT BY LEVEL <= 2"
	if _, err := conn.Exec(seed); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	seedGHI := "INSERT INTO GHI_CNT_MSTR (GHI_XCHNG_CD, GHI_PRDCT_TYP, GHI_UNDRLYNG, GHI_EXPRY_DT, " +
		"GHI_EXER_TYP, GHI_OPT_TYP, GHI_STRK_PRC, GHI_TOKEN_NO) " +
		"VALUES ('TST', 'F', 'FVTEST', DATE '2026-12-31', 'E', '*', 0, -1)"
	if _, err := conn.Exec(seedGHI); err != nil {
		t.Fatalf("seed GHI row: %v", err)
	}

	// 01-Jan-2026 01:02:03, exchange wall-clock, as seconds since 1980.
	secs := int64(time.Date(2026, 1, 1, 1, 2, 3, 0, time.UTC).Sub(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)).Seconds())
	b := batchOf(-1, -2, -3) // -3 has no row: never inserted
	b.Rows[0].Quote.LastTradeTime = secs
	b.Rows[1].Quote.LastTradeTime = 0 // no trade yet: the column keeps its NULL
	b.Rows[0].Master, b.Rows[0].HasMaster = masterOf(-1000, "SPLIT 1:2"), true
	st := new(db.Stats)
	if err := db.NewCommit(conn, 10*time.Second, st)(b); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := st.Unmatched.Load(); n != 1 {
		t.Errorf("Unmatched = %d, want 1", n)
	}
	q := "SELECT ABC_LST_TRD_PRC, TO_CHAR(ABC_LST_TRD_TM, 'DD-Mon-YYYY HH24:MI:SS'), ABC_INDCTR, " +
		"CASE WHEN ABC_LST_UPD_TM IS NULL THEN 0 ELSE 1 END FROM ABC_TRADE_QUOTE WHERE ABC_TOKEN_NO = :1"
	for _, c := range []struct {
		token int32
		ltt   sql.NullString
	}{{-1, sql.NullString{String: "01-Jan-2026 01:02:03", Valid: true}}, {-2, sql.NullString{}}} {
		var ltp, upd int64
		var ltt sql.NullString
		var ind string
		if err := conn.QueryRow(q, c.token).Scan(&ltp, &ltt, &ind, &upd); err != nil {
			t.Fatal(err)
		}
		if want := quote(int64(c.token) * 1000).LastTradedPrice; ltp != want || ltt != c.ltt || ind != "+" || upd != 1 {
			t.Errorf("token %d: (ltp, ltt, ind, updated) = (%d, %v, %q, %d), want (%d, %v, \"+\", 1)",
				c.token, ltp, ltt, ind, upd, want, c.ltt)
		}
	}

	var abcBase, mult, ghiBase, lot int64
	var flag, remark string
	if err := conn.QueryRow("SELECT ABC_BASE_PRC, ABC_MULTPLR FROM ABC_TRADE_QUOTE WHERE ABC_TOKEN_NO = -1").Scan(&abcBase, &mult); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow("SELECT GHI_BASE_PRICE, GHI_MIN_LOT_QTY, GHI_RECORD_DELETE_FLG, RTRIM(GHI_REMARKS) FROM GHI_CNT_MSTR WHERE GHI_TOKEN_NO = -1").Scan(&ghiBase, &lot, &flag, &remark); err != nil {
		t.Fatal(err)
	}
	if want := masterOf(-1000, ""); abcBase != want.BasePrice || mult != want.Multiplier || ghiBase != want.BasePrice ||
		lot != want.MinimumLotQuantity || flag != "N" || remark != "SPLIT 1:2" {
		t.Errorf("master columns = (abc base %d, multiplier %d, ghi base %d, min lot %d, flag %q, remark %q), want (%d, %d, %d, %d, \"N\", \"SPLIT 1:2\")",
			abcBase, mult, ghiBase, lot, flag, remark, want.BasePrice, want.Multiplier, want.BasePrice, want.MinimumLotQuantity)
	}
}

// masterOf returns a master whose every field is distinct and derived from
// base, with remark as its blank-padded remark.
func masterOf(base int64, remark string) mirror.Master {
	m := mirror.Master{
		HighPriceRange: base + 1, LowPriceRange: base + 2, BasePrice: base + 3, Multiplier: base + 4,
		MinimumLotQuantity: base + 5, BoardLotQuantity: base + 6, TickSize: base + 7, FreezeQuantity: base + 8,
		DeleteFlag: 'N',
	}
	for i := range m.Remark {
		m.Remark[i] = ' '
	}
	copy(m.Remark[:], remark)
	return m
}

// masterRow is a batch row for token carrying only a changed master part.
func masterRow(tok int32, remark string) mirror.BatchRow {
	return mirror.BatchRow{Token: tok, Master: masterOf(int64(tok)*1000, remark), HasMaster: true}
}

const (
	wantABCMasterSQL = "UPDATE ABC_TRADE_QUOTE SET ABC_HGH_PRC_RNG = :1, ABC_LOW_PRC_RNG = :2, " +
		"ABC_BASE_PRC = :3, ABC_MULTPLR = :4 WHERE ABC_TOKEN_NO = :5"
	wantGHISQL = "UPDATE GHI_CNT_MSTR SET GHI_MIN_LOT_QTY = :1, GHI_BOARD_LOT_QTY = :2, GHI_TCK_SZ = :3, " +
		"GHI_FREEZE_QTY = :4, GHI_BASE_PRICE = :5, GHI_RECORD_DELETE_FLG = CHR(:6), GHI_REMARKS = :7 " +
		"WHERE GHI_TOKEN_NO = :8"
)

func TestCommitMasterRows(t *testing.T) {
	conn, f := fakedb.Open(t)
	commit := db.NewCommit(conn, time.Second, new(db.Stats))
	b := mirror.NewBatch(0, 2)
	b.Rows = append(b.Rows, masterRow(7, "SPLIT 1:2"), masterRow(9, ""))
	if err := commit(b); err != nil {
		t.Fatalf("commit: %v", err)
	}
	execs := f.Execs()
	if len(execs) != 2 {
		t.Fatalf("ran %d statements, want 2 (ABC master, GHI): %+v", len(execs), execs)
	}
	if execs[0].SQL != wantABCMasterSQL {
		t.Errorf("first SQL =\n%s\nwant\n%s", execs[0].SQL, wantABCMasterSQL)
	}
	if want := []any{[]int64{7001, 9001}, []int64{7002, 9002}, []int64{7003, 9003}, []int64{7004, 9004}, []int64{7, 9}}; !reflect.DeepEqual(execs[0].Args, want) {
		t.Errorf("ABC master args = %v, want %v", execs[0].Args, want)
	}
	if execs[1].SQL != wantGHISQL {
		t.Errorf("second SQL =\n%s\nwant\n%s", execs[1].SQL, wantGHISQL)
	}
	want := []any{
		[]int64{7005, 9005}, []int64{7006, 9006}, []int64{7007, 9007}, []int64{7008, 9008}, []int64{7003, 9003},
		[]int64{'N', 'N'}, []string{"SPLIT 1:2", ""}, []int64{7, 9},
	}
	if !reflect.DeepEqual(execs[1].Args, want) {
		t.Errorf("GHI args = %v, want %v (remarks right-trimmed)", execs[1].Args, want)
	}
	if f.Commits() != 1 {
		t.Errorf("commits = %d, want 1", f.Commits())
	}
}

// Each statement runs only over the rows whose part changed, in the fixed
// order quote, ABC master, GHI, all in one transaction.
func TestCommitPartsAndOrder(t *testing.T) {
	both := masterRow(3, "")
	both.Quote, both.HasQuote = quote(3000), true
	tests := []struct {
		name     string
		rows     []mirror.BatchRow
		wantSQL  []string
		wantKeys []any
	}{
		{"quotes only", batchOf(1).Rows, []string{wantSQL}, []any{[]int64{1}}},
		{"masters only", []mirror.BatchRow{masterRow(2, "")}, []string{wantABCMasterSQL, wantGHISQL}, []any{[]int64{2}, []int64{2}}},
		{"mixed", append(batchOf(1).Rows, masterRow(2, ""), both),
			[]string{wantSQL, wantABCMasterSQL, wantGHISQL}, []any{[]int64{1, 3}, []int64{2, 3}, []int64{2, 3}}},
		{"nothing changed", []mirror.BatchRow{{Token: 4}}, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, f := fakedb.Open(t)
			b := mirror.NewBatch(0, len(tt.rows))
			b.Rows = append(b.Rows, tt.rows...)
			if err := db.NewCommit(conn, time.Second, new(db.Stats))(b); err != nil {
				t.Fatalf("commit: %v", err)
			}
			var gotSQL []string
			var gotKeys []any
			for _, e := range f.Execs() {
				gotSQL = append(gotSQL, e.SQL)
				gotKeys = append(gotKeys, e.Args[len(e.Args)-1])
			}
			if !reflect.DeepEqual(gotSQL, tt.wantSQL) {
				t.Errorf("statements = %q, want %q", gotSQL, tt.wantSQL)
			}
			if !reflect.DeepEqual(gotKeys, tt.wantKeys) {
				t.Errorf("tokens per statement = %v, want %v", gotKeys, tt.wantKeys)
			}
		})
	}
}

func TestCommitMasterFailureRollsBack(t *testing.T) {
	conn, f := fakedb.Open(t)
	f.FailAt = 2 // the GHI statement
	b := mirror.NewBatch(0, 1)
	b.Rows = append(b.Rows, masterRow(1, ""))
	err := db.NewCommit(conn, time.Second, new(db.Stats))(b)
	if err == nil || !strings.Contains(err.Error(), "GHI_CNT_MSTR") {
		t.Fatalf("err = %v, want one naming GHI_CNT_MSTR", err)
	}
	if f.Commits() != 0 || f.Rollbacks() != 1 {
		t.Errorf("(commits, rollbacks) = (%d, %d), want (0, 1)", f.Commits(), f.Rollbacks())
	}
}

func TestCommitCountsUnmatchedPerStatement(t *testing.T) {
	conn, f := fakedb.Open(t)
	f.Missing = 1
	st := new(db.Stats)
	b := mirror.NewBatch(0, 1)
	b.Rows = append(b.Rows, masterRow(1, ""))
	if err := db.NewCommit(conn, time.Second, st)(b); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := st.Unmatched.Load(); n != 2 {
		t.Errorf("Unmatched = %d, want 2 (one per statement)", n)
	}
}
