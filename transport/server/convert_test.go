package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"go.sriracha.dev/sriracha"
	srirachav1 "go.sriracha.dev/transport/proto/srirachav1"
)

func TestProtoToTokenRecordEmpty(t *testing.T) {
	t.Parallel()

	_, err := ProtoToTokenRecord(nil)
	assert.Error(t, err)

	_, err = ProtoToTokenRecord([]byte{})
	assert.Error(t, err)
}

func TestProtoToTokenRecordInvalidProto(t *testing.T) {
	t.Parallel()

	_, err := ProtoToTokenRecord([]byte{0xFF, 0xFE, 0x01})
	assert.Error(t, err)
}

func TestProtoToTokenRecordBadChecksum(t *testing.T) {
	t.Parallel()

	// Valid proto but checksum is not 32 bytes.
	pb := &srirachav1.TokenRecord{
		FieldsetVersion: "test-v1",
		Mode:            srirachav1.MatchMode_DETERMINISTIC,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("test"),
		Checksum:        []byte{0x01, 0x02}, // only 2 bytes, not 32
	}
	b, err := proto.Marshal(pb)
	require.NoError(t, err)

	_, err = ProtoToTokenRecord(b)
	assert.Error(t, err)
}

func TestProtoToTokenRecordProbabilistic(t *testing.T) {
	t.Parallel()

	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "test-v1",
		Mode:            sriracha.Probabilistic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("payload"),
		Checksum:        checksum,
	}

	b, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	got, err := ProtoToTokenRecord(b)
	require.NoError(t, err)
	assert.Equal(t, sriracha.Probabilistic, got.Mode)
}

func TestProtoToTokenRecordUnknownMode(t *testing.T) {
	t.Parallel()

	// Valid proto bytes but unknown mode — ProtoToMatchMode fails inside ProtoToTokenRecord.
	checksum := make([]byte, 32)
	pb := &srirachav1.TokenRecord{
		FieldsetVersion: "test-v1",
		Mode:            srirachav1.MatchMode(999),
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("payload"),
		Checksum:        checksum,
	}
	b, err := proto.Marshal(pb)
	require.NoError(t, err)

	_, err = ProtoToTokenRecord(b)
	assert.Error(t, err)
}

func TestProtoToMatchModeUnknown(t *testing.T) {
	t.Parallel()

	_, err := ProtoToMatchMode(srirachav1.MatchMode(999))
	assert.Error(t, err)
}

func TestMatchModeToProtoUnknown(t *testing.T) {
	t.Parallel()

	_, err := MatchModeToProto(sriracha.MatchMode(99))
	assert.Error(t, err)
}

func TestTokenRecordToProtoInvalidMode(t *testing.T) {
	t.Parallel()

	_, err := TokenRecordToProto(sriracha.TokenRecord{Mode: sriracha.MatchMode(99)})
	assert.Error(t, err)
}

func BenchmarkTokenRecordToProto(b *testing.B) {
	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "v1",
		Mode:            sriracha.Deterministic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("benchmark-payload"),
		Checksum:        checksum,
	}
	b.ResetTimer()
	for range b.N {
		_, _ = TokenRecordToProto(tr)
	}
}

func BenchmarkProtoToTokenRecord(b *testing.B) {
	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "v1",
		Mode:            sriracha.Deterministic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("benchmark-payload"),
		Checksum:        checksum,
	}
	data, err := TokenRecordToProto(tr)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		_, _ = ProtoToTokenRecord(data)
	}
}

func FuzzProtoToTokenRecord(f *testing.F) {
	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "v1",
		Mode:            sriracha.Deterministic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("seed"),
		Checksum:        checksum,
	}
	seed, err := TokenRecordToProto(tr)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte(nil))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic regardless of input.
		_, _ = ProtoToTokenRecord(data)
	})
}
