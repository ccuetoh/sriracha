package server

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"go.sriracha.dev/sriracha"
	srirachav1 "go.sriracha.dev/transport/proto/srirachav1"
)

// ProtoToTokenRecord deserialises QueryRequest.token_record bytes into a sriracha.TokenRecord.
func ProtoToTokenRecord(b []byte) (sriracha.TokenRecord, error) {
	if len(b) == 0 {
		return sriracha.TokenRecord{}, sriracha.ErrTokenMalformed(sriracha.FieldPath{})
	}

	var pb srirachav1.TokenRecord
	if err := proto.Unmarshal(b, &pb); err != nil {
		return sriracha.TokenRecord{}, sriracha.ErrTokenMalformed(sriracha.FieldPath{})
	}

	if len(pb.Checksum) != 32 {
		return sriracha.TokenRecord{}, sriracha.ErrChecksumMismatch()
	}

	mode, err := ProtoToMatchMode(pb.Mode)
	if err != nil {
		return sriracha.TokenRecord{}, err
	}

	var checksum [32]byte
	copy(checksum[:], pb.Checksum)

	return sriracha.TokenRecord{
		FieldSetVersion: pb.FieldsetVersion,
		Mode:            mode,
		Algo:            pb.Algo,
		Payload:         pb.Payload,
		Checksum:        checksum,
	}, nil
}

// TokenRecordToProto serialises a sriracha.TokenRecord to bytes for use in QueryRequest.token_record.
func TokenRecordToProto(tr sriracha.TokenRecord) ([]byte, error) {
	mode, err := MatchModeToProto(tr.Mode)
	if err != nil {
		return nil, err
	}

	pb := &srirachav1.TokenRecord{
		FieldsetVersion: tr.FieldSetVersion,
		Mode:            mode,
		Algo:            tr.Algo,
		Payload:         tr.Payload,
		Checksum:        tr.Checksum[:],
	}

	return proto.Marshal(pb)
}

// ProtoToMatchMode converts a proto MatchMode to the sriracha equivalent.
func ProtoToMatchMode(m srirachav1.MatchMode) (sriracha.MatchMode, error) {
	switch m {
	case srirachav1.MatchMode_DETERMINISTIC:
		return sriracha.Deterministic, nil
	case srirachav1.MatchMode_PROBABILISTIC:
		return sriracha.Probabilistic, nil
	default:
		return 0, fmt.Errorf("transport: unknown MatchMode %d", m)
	}
}

// MatchModeToProto converts a sriracha MatchMode to its proto representation.
func MatchModeToProto(m sriracha.MatchMode) (srirachav1.MatchMode, error) {
	switch m {
	case sriracha.Deterministic:
		return srirachav1.MatchMode_DETERMINISTIC, nil
	case sriracha.Probabilistic:
		return srirachav1.MatchMode_PROBABILISTIC, nil
	default:
		return srirachav1.MatchMode_MATCH_MODE_UNSPECIFIED, fmt.Errorf("transport: unknown MatchMode %d", m)
	}
}

// protoToMatchConfig converts a proto MatchConfig to the sriracha equivalent.
// A nil proto config returns the zero value; defaults are applied by the indexer.
func protoToMatchConfig(mc *srirachav1.MatchConfig) sriracha.MatchConfig {
	if mc == nil {
		return sriracha.MatchConfig{}
	}

	weights := make([]sriracha.FieldWeight, 0, len(mc.FieldWeights))
	for _, fw := range mc.FieldWeights {
		fp, err := sriracha.ParseFieldPath(fw.FieldPath)
		if err != nil {
			continue
		}
		weights = append(weights, sriracha.FieldWeight{Path: fp, Weight: float64(fw.Weight)})
	}

	return sriracha.MatchConfig{
		Threshold:    mc.Threshold,
		MaxResults:   mc.MaxResults,
		FieldWeights: weights,
	}
}

// candidatesToStatus maps a candidate list to the appropriate MatchStatus.
func candidatesToStatus(candidates []sriracha.Candidate) srirachav1.MatchStatus {
	if len(candidates) == 0 {
		return srirachav1.MatchStatus_NO_MATCH
	}
	if candidates[0].Confidence == 1.0 {
		return srirachav1.MatchStatus_MATCHED
	}
	if len(candidates) > 1 && (candidates[0].Confidence-candidates[1].Confidence) < 0.01 {
		return srirachav1.MatchStatus_MULTIPLE_CANDIDATES
	}
	return srirachav1.MatchStatus_MATCHED
}
