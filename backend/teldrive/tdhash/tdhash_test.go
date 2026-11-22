package tdhash

import (
	"bytes"
	"github.com/zeebo/blake3"
	"testing"
)

func TestBlockHashesComposeTreeHash(t *testing.T) {
	data := bytes.Repeat([]byte("teldrive"), BlockSize/8+1)
	onePass := New()
	_, _ = onePass.Write(data)
	want := onePass.Sum(nil)

	var blocks []byte
	for offset := 0; offset < len(data); {
		end := min(offset+BlockSize, len(data))
		part := New().(*digest)
		_, _ = part.Write(data[offset:end])
		blocks = append(blocks, part.BlockHashes()...)
		offset = end
	}
	if got := SumBlockHashes(blocks); !bytes.Equal(got, want) {
		t.Fatalf("composed tree hash = %x, want %x", got, want)
	}
}

func TestB1TreeHashOracle(t *testing.T) {
	for _, size := range []int{0, 17, BlockSize, BlockSize + 17, 2*BlockSize + 17} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i*31 + 7)
		}
		root := blake3.New()
		for offset := 0; offset < len(data); offset += BlockSize {
			block := blake3.Sum256(data[offset:min(offset+BlockSize, len(data))])
			_, _ = root.Write(block[:])
		}
		got := New()
		for offset := 0; offset < len(data); offset += 7919 {
			_, _ = got.Write(data[offset:min(offset+7919, len(data))])
		}
		if !bytes.Equal(got.Sum(nil), root.Sum(nil)) {
			t.Fatalf("size %d: tree root differs from direct global-block oracle", size)
		}
		for _, partSize := range []int{BlockSize, 2 * BlockSize} {
			composed := blake3.New()
			for offset := 0; offset < len(data); offset += partSize {
				part := New().(*digest)
				_, _ = part.Write(data[offset:min(offset+partSize, len(data))])
				_, _ = composed.Write(part.BlockHashes())
			}
			if !bytes.Equal(composed.Sum(nil), root.Sum(nil)) {
				t.Fatalf("size %d, part size %d: multipart root differs from oracle", size, partSize)
			}
		}
	}
}
