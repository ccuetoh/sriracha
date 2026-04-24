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
