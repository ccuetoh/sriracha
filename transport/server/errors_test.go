package server

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"go.sriracha.dev/sriracha"
)

func TestToGRPCStatus(t *testing.T) {
	t.Parallel()

	assert.Nil(t, toGRPCStatus(nil))

	plain := errors.New("plain error")
	s, _ := status.FromError(toGRPCStatus(plain))
	assert.Equal(t, codes.Internal, s.Code())
}

func TestCodeForSrirachaCode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		code     sriracha.ErrorCode
		wantGRPC codes.Code
	}{
		{sriracha.CodePolicyMissing, codes.PermissionDenied},
		{sriracha.CodeTokenMalformed, codes.InvalidArgument},
		{sriracha.CodeFieldSetIncompatible, codes.InvalidArgument},
		{sriracha.CodeNormalizationFailed, codes.InvalidArgument},
		{sriracha.CodeChecksumMismatch, codes.InvalidArgument},
		{sriracha.CodeRecordNotFound, codes.NotFound},
		{sriracha.CodeIndexCorrupted, codes.Internal},
		{sriracha.CodeAuditViolation, codes.Internal},
		{sriracha.CodeVersionUnsupported, codes.Unimplemented},
		{sriracha.CodeInternalError, codes.Internal},
		{sriracha.ErrorCode(9999), codes.Internal},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("code_%d", tc.code), func(t *testing.T) {
			t.Parallel()
			got := codeForSrirachaCode(tc.code)
			assert.Equal(t, tc.wantGRPC, got)
		})
	}
}

func TestPeerInstitutionID(t *testing.T) {
	t.Parallel()

	t.Run("uri san takes priority", func(t *testing.T) {
		t.Parallel()
		u, err := url.Parse("spiffe://org.example.a")
		assert.NoError(t, err)
		info := credentials.TLSInfo{
			State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{
					{URIs: []*url.URL{u}, Subject: pkix.Name{CommonName: "ignored"}},
				},
			},
		}
		assert.Equal(t, "spiffe://org.example.a", peerInstitutionID(info))
	})

	t.Run("falls back to common name", func(t *testing.T) {
		t.Parallel()
		info := credentials.TLSInfo{
			State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{
					{Subject: pkix.Name{CommonName: "org.example.b"}},
				},
			},
		}
		assert.Equal(t, "org.example.b", peerInstitutionID(info))
	})

	t.Run("empty chain returns empty string", func(t *testing.T) {
		t.Parallel()
		info := credentials.TLSInfo{}
		assert.Empty(t, peerInstitutionID(info))
	})
}
