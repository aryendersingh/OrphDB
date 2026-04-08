package orphdb

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestCompressRoundtrip(t *testing.T) {
	algos := []struct {
		name string
		algo Compression
	}{
		{"none", CompressionNone},
		{"snappy", CompressionSnappy},
		{"zstd", CompressionZstd},
	}

	sizes := []int{0, 1, 100, 1024, 64 * 1024, 256 * 1024}

	for _, a := range algos {
		for _, sz := range sizes {
			t.Run(a.name+"/"+string(rune('0'+sz)), func(t *testing.T) {
				data := make([]byte, sz)
				rand.Read(data)

				compressed := compressValue(data, a.algo)
				decompressed, err := decompressValue(compressed)
				if err != nil {
					t.Fatalf("decompress: %v", err)
				}
				if !bytes.Equal(data, decompressed) {
					t.Fatalf("roundtrip mismatch for size %d", sz)
				}
			})
		}
	}
}

func TestDecompressTooShort(t *testing.T) {
	_, err := decompressValue([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error for short data")
	}
}

func TestDecompressUnknownAlgo(t *testing.T) {
	data := []byte{0xFF, 0x00, 0x00, 0x00, 0x00}
	_, err := decompressValue(data)
	if err == nil {
		t.Fatal("expected error for unknown algo")
	}
}

func BenchmarkCompress(b *testing.B) {
	data := make([]byte, 64*1024)
	rand.Read(data)

	b.Run("none", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			compressValue(data, CompressionNone)
		}
	})
	b.Run("snappy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			compressValue(data, CompressionSnappy)
		}
	})
	b.Run("zstd", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			compressValue(data, CompressionZstd)
		}
	})
}

func BenchmarkDecompress(b *testing.B) {
	data := make([]byte, 64*1024)
	rand.Read(data)

	for _, algo := range []struct {
		name string
		c    Compression
	}{
		{"none", CompressionNone},
		{"snappy", CompressionSnappy},
		{"zstd", CompressionZstd},
	} {
		compressed := compressValue(data, algo.c)
		b.Run(algo.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				decompressValue(compressed)
			}
		})
	}
}
