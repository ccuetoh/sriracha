package server

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.sriracha.dev/sriracha"
)

// toGRPCStatus maps a sriracha error (or any error) to a gRPC status error.
func toGRPCStatus(err error) error {
	if err == nil {
		return nil
	}

	var sErr *sriracha.Error
	if errors.As(err, &sErr) {
		return status.Error(codeForSrirachaCode(sErr.Code), sErr.Message)
	}

	return status.Error(codes.Internal, err.Error())
}

func codeForSrirachaCode(c sriracha.ErrorCode) codes.Code {
	switch c {
	case sriracha.CodePolicyMissing:
		return codes.PermissionDenied
	case sriracha.CodeTokenMalformed:
		return codes.InvalidArgument
	case sriracha.CodeFieldSetIncompatible:
		return codes.InvalidArgument
	case sriracha.CodeNormalizationFailed:
		return codes.InvalidArgument
	case sriracha.CodeChecksumMismatch:
		return codes.InvalidArgument
	case sriracha.CodeRecordNotFound:
		return codes.NotFound
	case sriracha.CodeIndexCorrupted:
		return codes.Internal
	case sriracha.CodeAuditViolation:
		return codes.Internal
	case sriracha.CodeVersionUnsupported:
		return codes.Unimplemented
	case sriracha.CodeInternalError:
		return codes.Internal
	default:
		return codes.Internal
	}
}
