package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// pageSize is the size of the pages the tests write.
const pageSize = 512

// testHeader returns a header with every field set to a distinct value.
func testHeader() Header {
	return Header{
		Version:    Version,
		Flags:      0,
		DatabaseID: DatabaseID("app.db"),
		Epoch:      7,
		SeqStart:   42,
		PageSize:   pageSize,
		CreatedAt:  1700000000123,
	}
}

// testTx returns a transaction with the given sequence number and pages.
func testTx(seq uint64, commitPages uint32, pages int) Tx {
	tx := Tx{Seq: seq, CommitPages: commitPages}
	for i := range pages {
		data := make([]byte, pageSize)
		for j := range data {
			data[j] = byte(seq + uint64(i) + uint64(j))
		}
		tx.Pages = append(tx.Pages, Page{Pgno: uint32(i + 1), Data: data})
	}
	return tx
}

// writeSegment writes a segment with the given transactions into a buffer.
func writeSegment(t *testing.T, h Header, txs []Tx, pos Pos) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, h)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, tx := range txs {
		if err := w.WriteTx(tx); err != nil {
			t.Fatalf("WriteTx: %v", err)
		}
	}
	if _, err := w.Close(pos); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

func TestHeaderLayout(t *testing.T) {
	if HeaderSize != 64 {
		t.Fatalf("the header is %d bytes, want 64", HeaderSize)
	}
	h := testHeader()
	b := EncodeHeader(h)

	if string(b[0:4]) != "SEG1" {
		t.Errorf("the magic is %q, want %q", b[0:4], "SEG1")
	}
	if v := binary.BigEndian.Uint32(b[4:8]); v != 1 {
		t.Errorf("the version is %d, want 1", v)
	}
	if v := binary.BigEndian.Uint32(b[8:12]); v != 0 {
		t.Errorf("the flags are %d, want 0", v)
	}
	if v := binary.BigEndian.Uint32(b[12:16]); v != crc32.Checksum(b[0:12], castagnoli) {
		t.Errorf("the checksum of the header is %d, want %d", v, crc32.Checksum(b[0:12], castagnoli))
	}
	if !bytes.Equal(b[16:32], h.DatabaseID[:]) {
		t.Errorf("the database ID is %x, want %x", b[16:32], h.DatabaseID)
	}
	if v := binary.BigEndian.Uint64(b[32:40]); v != h.Epoch {
		t.Errorf("the epoch is %d, want %d", v, h.Epoch)
	}
	if v := binary.BigEndian.Uint64(b[40:48]); v != h.SeqStart {
		t.Errorf("the first sequence number is %d, want %d", v, h.SeqStart)
	}
	if v := binary.BigEndian.Uint32(b[48:52]); v != h.PageSize {
		t.Errorf("the page size is %d, want %d", v, h.PageSize)
	}
	if v := binary.BigEndian.Uint32(b[52:56]); v != 0 {
		t.Errorf("the reserved field is %d, want 0", v)
	}
	if v := int64(binary.BigEndian.Uint64(b[56:64])); v != h.CreatedAt {
		t.Errorf("the creation time is %d, want %d", v, h.CreatedAt)
	}

	got, err := ParseHeader(b)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if got.Version != h.Version || got.Epoch != h.Epoch || got.SeqStart != h.SeqStart ||
		got.PageSize != h.PageSize || got.CreatedAt != h.CreatedAt || got.DatabaseID != h.DatabaseID {
		t.Errorf("the header came back as %+v, want %+v", got, h)
	}
}

func TestTrailerLayout(t *testing.T) {
	if TrailerSize != 64 {
		t.Fatalf("the trailer is %d bytes, want 64", TrailerSize)
	}
	tr := Trailer{
		SeqEnd:       99,
		TxCount:      3,
		PageCount:    7,
		PayloadBytes: 4096,
		PayloadCRC:   0xdeadbeef,
		Salt1:        0x11223344,
		Salt2:        0x55667788,
		WALFrame:     12,
		WALOffset:    65536,
	}
	b := EncodeTrailer(tr)

	if v := binary.BigEndian.Uint64(b[0:8]); v != tr.SeqEnd {
		t.Errorf("the last sequence number is %d, want %d", v, tr.SeqEnd)
	}
	if v := binary.BigEndian.Uint64(b[8:16]); v != tr.TxCount {
		t.Errorf("the number of transactions is %d, want %d", v, tr.TxCount)
	}
	if v := binary.BigEndian.Uint64(b[16:24]); v != tr.PageCount {
		t.Errorf("the number of pages is %d, want %d", v, tr.PageCount)
	}
	if v := binary.BigEndian.Uint64(b[24:32]); v != tr.PayloadBytes {
		t.Errorf("the size of the payload is %d, want %d", v, tr.PayloadBytes)
	}
	if v := binary.BigEndian.Uint32(b[32:36]); v != tr.PayloadCRC {
		t.Errorf("the checksum of the payload is %d, want %d", v, tr.PayloadCRC)
	}
	if v := binary.BigEndian.Uint32(b[36:40]); v != tr.Salt1 {
		t.Errorf("the first salt is %d, want %d", v, tr.Salt1)
	}
	if v := binary.BigEndian.Uint32(b[40:44]); v != tr.Salt2 {
		t.Errorf("the second salt is %d, want %d", v, tr.Salt2)
	}
	if v := binary.BigEndian.Uint64(b[44:52]); v != tr.WALFrame {
		t.Errorf("the frame is %d, want %d", v, tr.WALFrame)
	}
	if v := int64(binary.BigEndian.Uint64(b[52:60])); v != tr.WALOffset {
		t.Errorf("the offset is %d, want %d", v, tr.WALOffset)
	}
	if v := binary.BigEndian.Uint32(b[60:64]); v != crc32.Checksum(b[0:60], castagnoli) {
		t.Errorf("the checksum of the trailer is %d, want %d", v, crc32.Checksum(b[0:60], castagnoli))
	}

	got, err := ParseTrailer(b)
	if err != nil {
		t.Fatalf("ParseTrailer: %v", err)
	}
	tr.TrailerCRC = got.TrailerCRC
	if got != tr {
		t.Errorf("the trailer came back as %+v, want %+v", got, tr)
	}
}

func TestReadBacksEveryField(t *testing.T) {
	h := testHeader()
	txs := []Tx{testTx(42, 5, 2), testTx(43, 6, 1), testTx(44, 6, 3)}
	pos := Pos{Salt1: 1, Salt2: 2, Frame: 77, WALOffset: 40960}

	b := writeSegment(t, h, txs, pos)
	seg, err := Read(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	h.HeaderCRC = seg.Header.HeaderCRC
	if seg.Header != h {
		t.Errorf("the header came back as %+v, want %+v", seg.Header, h)
	}
	if len(seg.Txs) != len(txs) {
		t.Fatalf("the segment holds %d transactions, want %d", len(seg.Txs), len(txs))
	}
	for i, tx := range seg.Txs {
		want := txs[i]
		if tx.Seq != want.Seq {
			t.Errorf("transaction %d has the sequence number %d, want %d", i, tx.Seq, want.Seq)
		}
		if tx.CommitPages != want.CommitPages {
			t.Errorf("transaction %d has %d pages after the commit, want %d", i, tx.CommitPages, want.CommitPages)
		}
		if len(tx.Pages) != len(want.Pages) {
			t.Fatalf("transaction %d holds %d pages, want %d", i, len(tx.Pages), len(want.Pages))
		}
		for j, p := range tx.Pages {
			if p.Pgno != want.Pages[j].Pgno {
				t.Errorf("page %d of transaction %d is the page %d, want %d", j, i, p.Pgno, want.Pages[j].Pgno)
			}
			if !bytes.Equal(p.Data, want.Pages[j].Data) {
				t.Errorf("page %d of transaction %d has another content", j, i)
			}
		}
	}
	if seg.Trailer.SeqEnd != 44 {
		t.Errorf("the segment ends at %d, want 44", seg.Trailer.SeqEnd)
	}
	if seg.Trailer.TxCount != 3 {
		t.Errorf("the segment holds %d transactions, want 3", seg.Trailer.TxCount)
	}
	if seg.Trailer.PageCount != 6 {
		t.Errorf("the segment holds %d pages, want 6", seg.Trailer.PageCount)
	}
	if got := seg.Trailer.Pos(); got != pos {
		t.Errorf("the position of the segment is %+v, want %+v", got, pos)
	}
}

func TestSequenceRangeCoversEveryTransaction(t *testing.T) {
	h := testHeader()
	h.SeqStart = 1
	var txs []Tx
	for i := range 5 {
		txs = append(txs, testTx(uint64(i+1), 4, 1))
	}
	seg, err := Read(bytes.NewReader(writeSegment(t, h, txs, Pos{})))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if seg.Header.SeqStart != 1 || seg.Trailer.SeqEnd != 5 {
		t.Errorf("the segment covers %d to %d, want 1 to 5", seg.Header.SeqStart, seg.Trailer.SeqEnd)
	}
	for i, tx := range seg.Txs {
		if tx.Seq != uint64(i+1) {
			t.Errorf("transaction %d has the sequence number %d, want %d", i, tx.Seq, i+1)
		}
		if i > 0 && tx.Seq <= seg.Txs[i-1].Seq {
			t.Errorf("the sequence numbers do not increase: %d after %d", tx.Seq, seg.Txs[i-1].Seq)
		}
	}
}

func TestCommitPagesAreKeptPerTransaction(t *testing.T) {
	h := testHeader()
	h.SeqStart = 1
	txs := []Tx{testTx(1, 10, 2), testTx(2, 4, 1), testTx(3, 12, 2)}

	seg, err := Read(bytes.NewReader(writeSegment(t, h, txs, Pos{})))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	want := []uint32{10, 4, 12}
	for i, tx := range seg.Txs {
		if tx.CommitPages != want[i] {
			t.Errorf("transaction %d has %d pages after the commit, want %d", i, tx.CommitPages, want[i])
		}
	}
}

func TestTamperedPayloadIsRejected(t *testing.T) {
	h := testHeader()
	h.SeqStart = 1
	b := writeSegment(t, h, []Tx{testTx(1, 4, 2)}, Pos{})

	b[HeaderSize+20] ^= 0xff
	if _, err := Read(bytes.NewReader(b)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a tampered payload was accepted: %v", err)
	}
}

func TestTruncatedFileIsRejected(t *testing.T) {
	h := testHeader()
	h.SeqStart = 1
	b := writeSegment(t, h, []Tx{testTx(1, 4, 2)}, Pos{})

	if _, err := Read(bytes.NewReader(b[:len(b)-1])); err == nil {
		t.Fatal("a truncated segment was accepted")
	}
	if _, err := Read(bytes.NewReader(b[:20])); !errors.Is(err, ErrShort) {
		t.Fatalf("a short segment was accepted: %v", err)
	}
}

func TestWrongMagicIsRejected(t *testing.T) {
	h := testHeader()
	h.SeqStart = 1
	b := writeSegment(t, h, []Tx{testTx(1, 4, 1)}, Pos{})

	b[0] = 'X'
	if _, err := Read(bytes.NewReader(b)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a segment with another magic was accepted: %v", err)
	}
}

func TestWrongVersionIsRejected(t *testing.T) {
	h := testHeader()
	h.SeqStart = 1
	b := writeSegment(t, h, []Tx{testTx(1, 4, 1)}, Pos{})

	binary.BigEndian.PutUint32(b[4:8], 2)
	if _, err := Read(bytes.NewReader(b)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a segment of another version was accepted: %v", err)
	}
}

func TestTamperedTrailerIsRejected(t *testing.T) {
	h := testHeader()
	h.SeqStart = 1
	b := writeSegment(t, h, []Tx{testTx(1, 4, 1)}, Pos{})

	b[len(b)-1] ^= 0xff
	if _, err := Read(bytes.NewReader(b)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a segment with a tampered trailer was accepted: %v", err)
	}
}

func TestReadFileAndReadTrailer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment.seg")
	h := testHeader()
	h.SeqStart = 1
	pos := Pos{Salt1: 9, Salt2: 8, Frame: 3, WALOffset: 1024}
	b := writeSegment(t, h, []Tx{testTx(1, 4, 1)}, pos)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	seg, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	h.HeaderCRC = seg.Header.HeaderCRC
	if seg.Header != h {
		t.Errorf("the header came back as %+v, want %+v", seg.Header, h)
	}
	trailer, err := ReadTrailer(path)
	if err != nil {
		t.Fatalf("ReadTrailer: %v", err)
	}
	if trailer != seg.Trailer {
		t.Errorf("the trailer came back as %+v, want %+v", trailer, seg.Trailer)
	}
}

func TestASegmentWithoutATransactionIsRejected(t *testing.T) {
	h := testHeader()
	b := EncodeHeader(h)
	b = append(b, EncodeTrailer(Trailer{PayloadCRC: crc32.Checksum(nil, castagnoli)})...)

	if _, err := Read(bytes.NewReader(b)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a segment without a transaction was accepted: %v", err)
	}
}

func TestASegmentWithAnImpossibleTransactionCountIsRejected(t *testing.T) {
	h := testHeader()
	b := EncodeHeader(h)
	trailer := Trailer{SeqEnd: 1, TxCount: 1 << 40, PayloadBytes: 0, PayloadCRC: crc32.Checksum(nil, castagnoli)}
	b = append(b, EncodeTrailer(trailer)...)

	if _, err := Read(bytes.NewReader(b)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a segment with an impossible transaction count was accepted: %v", err)
	}
}

func TestASegmentWithAnImpossiblePageSizeIsRejected(t *testing.T) {
	h := testHeader()
	h.PageSize = 1 << 30
	b := EncodeHeader(h)
	b = append(b, EncodeTrailer(Trailer{PayloadCRC: crc32.Checksum(nil, castagnoli)})...)

	if _, err := Read(bytes.NewReader(b)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a segment with an impossible page size was accepted: %v", err)
	}
}
