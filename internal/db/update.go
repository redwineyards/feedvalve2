package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"feedvalve/internal/mirror"
)

// Stats counts what the commits found. Safe to read concurrently.
type Stats struct {
	Unmatched atomic.Uint64 // rows whose token matched no table row; not written, not retried
}

// updateSQL writes one quote to its ABC_TRADE_QUOTE row. Every value is bound
// as an integer and converted in the SQL; binds are positional, each used
// once, in the order quoteArgs fills them, and the token comes last. Dates are
// the exchange's own wall-clock with no timezone shift (bible §2.5); a last
// trade time of 0 means no trade yet and leaves the column as it was.
// ABC_CHNG_PRVS_CLS takes the % change from the closing price, in hundredths
// of a percent, as split computes it.
const updateSQL = "UPDATE ABC_TRADE_QUOTE SET " +
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

// abcMasterSQL writes a row's 7305 columns in ABC_TRADE_QUOTE; they do not
// overlap updateSQL's. Binds as abcMasterArgs fills them, token last.
const abcMasterSQL = "UPDATE ABC_TRADE_QUOTE SET ABC_HGH_PRC_RNG = :1, ABC_LOW_PRC_RNG = :2, " +
	"ABC_BASE_PRC = :3, ABC_MULTPLR = :4 WHERE ABC_TOKEN_NO = :5"

// ghiSQL writes a row's 7305 columns in GHI_CNT_MSTR. Binds as ghiArgs fills
// them, token last; the remark is the one text bind.
const ghiSQL = "UPDATE GHI_CNT_MSTR SET GHI_MIN_LOT_QTY = :1, GHI_BOARD_LOT_QTY = :2, GHI_TCK_SZ = :3, " +
	"GHI_FREEZE_QTY = :4, GHI_BASE_PRICE = :5, GHI_RECORD_DELETE_FLG = CHR(:6), GHI_REMARKS = :7 " +
	"WHERE GHI_TOKEN_NO = :8"

// valueBinds is the number of quote values updateSQL binds before the token.
const valueBinds = 33

// statement is one UPDATE of a commit and the rows it covers.
type statement struct {
	table string // for errors and logs
	query string // the UPDATE
	args  []any  // its bind arrays
	rows  int    // how many rows it covers
}

// NewCommit returns the commit for the writers. It writes a batch as one
// transaction: an array-bound UPDATE of ABC_TRADE_QUOTE for the rows whose
// quote changed, then one of ABC_TRADE_QUOTE and one of GHI_CNT_MSTR for the
// rows whose master changed, always in that order. Rows are only updated,
// never inserted: the tables' rows are pre-loaded, and a token that matches
// no row is counted in st. A commit that takes longer than timeout is
// abandoned and rolled back.
func NewCommit(conn *sql.DB, timeout time.Duration, st *Stats) func(b *mirror.Batch) error {
	return func(b *mirror.Batch) error {
		return commit(conn, timeout, st, b)
	}
}

// commit writes b in one transaction, or rolls it back and returns why not.
func commit(conn *sql.DB, timeout time.Duration, st *Stats, b *mirror.Batch) error {
	var (
		quotes  []int              // positions in b.Rows of the rows whose quote changed
		masters []int              // positions in b.Rows of the rows whose master changed
		stmts   []statement        // the statements to run, in order
		done    []int64            // rows each statement updated
		ctx     context.Context    // bounds the whole transaction
		cancel  context.CancelFunc // releases ctx
		tx      *sql.Tx            // the batch's transaction
		res     sql.Result         // one statement's outcome
		n       int64              // rows it updated
		err     error              // result of each step
	)

	for i := range b.Rows {
		if b.Rows[i].HasQuote {
			quotes = append(quotes, i)
		}
		if b.Rows[i].HasMaster {
			masters = append(masters, i)
		}
	}
	if len(quotes) > 0 {
		stmts = append(stmts, statement{"ABC_TRADE_QUOTE", updateSQL, quoteArgs(b.Rows, quotes), len(quotes)})
	}
	if len(masters) > 0 {
		stmts = append(stmts,
			statement{"ABC_TRADE_QUOTE", abcMasterSQL, abcMasterArgs(b.Rows, masters), len(masters)},
			statement{"GHI_CNT_MSTR", ghiSQL, ghiArgs(b.Rows, masters), len(masters)})
	}
	if len(stmts) == 0 {
		return nil
	}
	done = make([]int64, len(stmts))

	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	for i, s := range stmts {
		res, err = tx.ExecContext(ctx, s.query, s.args...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("update %s (%d rows): %w", s.table, s.rows, err)
		}
		done[i] = -1 // unknown unless the driver reports it
		if n, err = res.RowsAffected(); err == nil {
			done[i] = n
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	for i, s := range stmts {
		if done[i] >= 0 && done[i] < int64(s.rows) {
			st.Unmatched.Add(uint64(int64(s.rows) - done[i]))
			slog.Warn("tokens matched no row, not written", "table", s.table, "rows", s.rows, "updated", done[i])
		}
	}
	return nil
}

// quoteArgs returns updateSQL's bind arrays for rows[idx...]: one []int64
// per value, in bind order, then the tokens.
func quoteArgs(rows []mirror.BatchRow, idx []int) []any {
	var (
		cols [valueBinds + 1][]int64 // the bind arrays
		args []any                   // the same, as driver arguments
		q    *mirror.Quote           // the row being added
	)

	for i := range cols {
		cols[i] = make([]int64, len(idx))
	}
	args = make([]any, len(cols))

	for r, at := range idx {
		q = &rows[at].Quote
		cols[0][r] = q.LastTradedPrice
		cols[1][r] = q.LastTradeQuantity
		cols[2][r] = q.LastTradeTime
		cols[3][r] = q.AverageTradePrice
		cols[4][r] = q.VolumeTradedToday
		cols[5][r] = q.NetChangeIndicator
		cols[6][r] = q.PctChangePrevClose
		cols[7][r] = q.ClosingPrice
		cols[8][r] = q.OpenPrice
		cols[9][r] = q.HighPrice
		cols[10][r] = q.LowPrice
		cols[11][r] = q.TotalBuyQuantity
		cols[12][r] = q.TotalSellQuantity
		for l := range q.BuyPrice {
			cols[13+2*l][r] = q.BuyPrice[l]
			cols[14+2*l][r] = q.BuyQuantity[l]
			cols[23+2*l][r] = q.SellPrice[l]
			cols[24+2*l][r] = q.SellQuantity[l]
		}
		cols[valueBinds][r] = int64(rows[at].Token)
	}
	for i := range cols {
		args[i] = cols[i]
	}
	return args
}

// abcMasterArgs returns abcMasterSQL's bind arrays for rows[idx...].
func abcMasterArgs(rows []mirror.BatchRow, idx []int) []any {
	var (
		cols [5][]int64     // the bind arrays
		args []any          // the same, as driver arguments
		m    *mirror.Master // the row being added
	)

	for i := range cols {
		cols[i] = make([]int64, len(idx))
	}
	args = make([]any, len(cols))

	for r, at := range idx {
		m = &rows[at].Master
		cols[0][r] = m.HighPriceRange
		cols[1][r] = m.LowPriceRange
		cols[2][r] = m.BasePrice
		cols[3][r] = m.Multiplier
		cols[4][r] = int64(rows[at].Token)
	}
	for i := range cols {
		args[i] = cols[i]
	}
	return args
}

// ghiArgs returns ghiSQL's bind arrays for rows[idx...]. The remark is sent
// without its blank padding (bible §2.6); CHAR(25) pads it again.
func ghiArgs(rows []mirror.BatchRow, idx []int) []any {
	var (
		nums    [6][]int64     // the integer binds :1..:6
		remarks []string       // bind :7
		tokens  []int64        // bind :8
		m       *mirror.Master // the row being added
	)

	for i := range nums {
		nums[i] = make([]int64, len(idx))
	}
	remarks = make([]string, len(idx))
	tokens = make([]int64, len(idx))

	for r, at := range idx {
		m = &rows[at].Master
		nums[0][r] = m.MinimumLotQuantity
		nums[1][r] = m.BoardLotQuantity
		nums[2][r] = m.TickSize
		nums[3][r] = m.FreezeQuantity
		nums[4][r] = m.BasePrice
		nums[5][r] = m.DeleteFlag
		remarks[r] = strings.TrimRight(string(m.Remark[:]), " \x00")
		tokens[r] = int64(rows[at].Token)
	}
	return []any{nums[0], nums[1], nums[2], nums[3], nums[4], nums[5], remarks, tokens}
}
