package wal

// Checksum is the pair of 32-bit values SQLite chains through a WAL.
//
// The header checksum is computed over the first 24 bytes of the WAL header
// with a zero seed; every frame extends the chain with its 8-byte header
// prefix followed by the page image.
type Checksum struct {
	S1 uint32
	S2 uint32
}

// wordOrder selects how the 32-bit words of a checksummed block are read.
type wordOrder int

const (
	// littleEndianWords reads the words least significant byte first.
	littleEndianWords wordOrder = iota
	// bigEndianWords reads the words most significant byte first.
	bigEndianWords
)

// update folds b into the checksum. len(b) must be a multiple of 8.
func (c Checksum) update(order wordOrder, b []byte) Checksum {
	s1, s2 := c.S1, c.S2
	for i := 0; i+8 <= len(b); i += 8 {
		x0 := uint32(b[i]) | uint32(b[i+1])<<8 | uint32(b[i+2])<<16 | uint32(b[i+3])<<24
		x1 := uint32(b[i+4]) | uint32(b[i+5])<<8 | uint32(b[i+6])<<16 | uint32(b[i+7])<<24
		if order == bigEndianWords {
			x0 = swap32(x0)
			x1 = swap32(x1)
		}
		s1 += x0 + s2
		s2 += x1 + s1
	}
	return Checksum{S1: s1, S2: s2}
}

func swap32(v uint32) uint32 {
	return v<<24 | (v&0xff00)<<8 | (v&0xff0000)>>8 | v>>24
}
