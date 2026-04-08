package orphdb

import (
	"encoding/binary"
	"fmt"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// Compression header byte identifies the compression format.
const (
	compressHeaderNone   byte = 0x00
	compressHeaderSnappy byte = 0x01
	compressHeaderZstd   byte = 0x02
)

var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		panic("orphdb: failed to create zstd encoder: " + err.Error())
	}
	zstdDecoder, err = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxMemory(256*1024*1024),
	)
	if err != nil {
		panic("orphdb: failed to create zstd decoder: " + err.Error())
	}
}

// compressValue compresses data and prepends a 5-byte header:
// [1 byte: compression type] [4 bytes: original size, little-endian]
func compressValue(data []byte, algo Compression) []byte {
	switch algo {
	case CompressionSnappy:
		compressed := s2.EncodeSnappy(nil, data)
		out := make([]byte, 5+len(compressed))
		out[0] = compressHeaderSnappy
		binary.LittleEndian.PutUint32(out[1:5], uint32(len(data)))
		copy(out[5:], compressed)
		return out

	case CompressionZstd:
		compressed := zstdEncoder.EncodeAll(data, make([]byte, 0, len(data)/2))
		out := make([]byte, 5+len(compressed))
		out[0] = compressHeaderZstd
		binary.LittleEndian.PutUint32(out[1:5], uint32(len(data)))
		copy(out[5:], compressed)
		return out

	default:
		out := make([]byte, 5+len(data))
		out[0] = compressHeaderNone
		binary.LittleEndian.PutUint32(out[1:5], uint32(len(data)))
		copy(out[5:], data)
		return out
	}
}

// decompressValue decompresses data based on the header byte.
func decompressValue(data []byte) ([]byte, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("orphdb: compressed data too short (%d bytes)", len(data))
	}

	algo := data[0]
	origSize := binary.LittleEndian.Uint32(data[1:5])
	payload := data[5:]

	switch algo {
	case compressHeaderNone:
		result := make([]byte, len(payload))
		copy(result, payload)
		return result, nil

	case compressHeaderSnappy:
		result, err := s2.Decode(make([]byte, 0, origSize), payload)
		if err != nil {
			return nil, fmt.Errorf("orphdb: snappy decompress: %w", err)
		}
		return result, nil

	case compressHeaderZstd:
		result, err := zstdDecoder.DecodeAll(payload, make([]byte, 0, origSize))
		if err != nil {
			return nil, fmt.Errorf("orphdb: zstd decompress: %w", err)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("orphdb: unknown compression type 0x%02x", algo)
	}
}
