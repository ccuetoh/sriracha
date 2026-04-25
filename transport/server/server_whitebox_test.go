package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	mocksriracha "go.sriracha.dev/mock/sriracha"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/transport/internal/replay"
	"go.sriracha.dev/transport/internal/tlsconf"
	srirachav1 "go.sriracha.dev/transport/proto/srirachav1"
)

// newWhiteboxServer creates a minimal *server for whitebox tests via type assertion.
func newWhiteboxServer(t *testing.T) *server {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})

	tlsCfg, err := tlsconf.LoadServerTLS(certPEM, keyPEM, certPEM)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cache := replay.New(ctx)
	indexer := mocksriracha.NewMockTokenIndexer(t)
	source := mocksriracha.NewMockRecordSource(t)

	srv, err := New(
		Config{InstitutionID: "org.example.b", SpecVersion: "0.1.0"},
		indexer, source, tlsCfg, cache, nil,
	)
	require.NoError(t, err)
	s, ok := srv.(*server)
	require.True(t, ok)
	return s
}

// generateWhiteboxClientCert creates a self-signed Ed25519 certificate for a fake peer.
func generateWhiteboxClientCert(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey, *x509.Certificate) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "org.example.a"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return pub, priv, cert
}

// signWhiteboxPolicy signs the policy with the given key using the same format as consent.policyMessage.
func signWhiteboxPolicy(priv ed25519.PrivateKey, p *srirachav1.ConsentPolicy) {
	var buf []byte
	buf = append(buf, []byte(p.PolicyId)...)
	buf = append(buf, []byte(p.IssuerId)...)
	buf = append(buf, []byte(p.TargetId)...)
	buf = append(buf, []byte(p.Purpose)...)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(p.IssuedAt))
	buf = append(buf, ts[:]...)
	binary.BigEndian.PutUint64(ts[:], uint64(p.ExpiresAt))
	buf = append(buf, ts[:]...)
	hash := sha256.Sum256(buf)
	p.Signature = ed25519.Sign(priv, hash[:])
}

// fakeBulkStream implements srirachav1.SrirachaService_BulkLinkServer for testing.
type fakeBulkStream struct {
	grpc.ServerStream
	ctx     context.Context
	batches []*srirachav1.BulkTokenBatch
	pos     int
	recvErr error
	sendErr error
	sent    []*srirachav1.BulkMatchResult
}

func (f *fakeBulkStream) Context() context.Context { return f.ctx }

func (f *fakeBulkStream) Recv() (*srirachav1.BulkTokenBatch, error) {
	if f.pos >= len(f.batches) {
		if f.recvErr != nil {
			return nil, f.recvErr
		}
		return nil, io.EOF
	}
	b := f.batches[f.pos]
	f.pos++
	return b, nil
}

func (f *fakeBulkStream) Send(r *srirachav1.BulkMatchResult) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, r)
	return nil
}

// fakeAuthInfo satisfies credentials.AuthInfo but is not TLSInfo.
type fakeAuthInfo struct{}

func (fakeAuthInfo) AuthType() string { return "fake" }

func TestBuildFieldValuesInvalidPath(t *testing.T) {
	t.Parallel()
	record := sriracha.RawRecord{}
	_, notFound := buildFieldValues(record, []string{"not-a-valid-path"})
	assert.Equal(t, []string{"not-a-valid-path"}, notFound)
}

func TestPeerIdentityNoPeer(t *testing.T) {
	t.Parallel()
	s := newWhiteboxServer(t)
	_, _, err := s.peerIdentity(context.Background())
	assert.Error(t, err)
}

func TestPeerIdentityNonTLS(t *testing.T) {
	t.Parallel()
	s := newWhiteboxServer(t)
	p := &peer.Peer{AuthInfo: fakeAuthInfo{}}
	ctx := peer.NewContext(context.Background(), p)
	_, _, err := s.peerIdentity(ctx)
	assert.Error(t, err)
}

// TestBulkLinkSendError covers stream.Send returning an error (server.go:254-256).
// It injects a valid Ed25519 peer context so peerIdentity succeeds, sends a batch
// with no token records so processBatch returns immediately, then Send fails.
func TestBulkLinkSendError(t *testing.T) {
	t.Parallel()

	s := newWhiteboxServer(t)

	_, clientPriv, clientCert := generateWhiteboxClientCert(t)

	info := credentials.TLSInfo{
		State: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{clientCert},
		},
	}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})

	now := time.Now()
	policy := &srirachav1.ConsentPolicy{
		PolicyId:  "pol-send-err",
		IssuerId:  "org.example.a",
		TargetId:  "org.example.b",
		Purpose:   "testing",
		IssuedAt:  now.Add(-time.Minute).Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(),
	}
	signWhiteboxPolicy(clientPriv, policy)

	stream := &fakeBulkStream{
		ctx:     ctx,
		sendErr: errors.New("send failed"),
		batches: []*srirachav1.BulkTokenBatch{
			{
				SessionId:    "bulk-send-err",
				TokenRecords: nil,
				Policy:       policy,
			},
		},
	}

	err := s.BulkLink(stream)
	assert.Error(t, err)
}

// TestBulkLinkRecvError covers stream.Recv() returning a non-EOF error (server.go:227-229).
func TestBulkLinkRecvError(t *testing.T) {
	t.Parallel()

	s := newWhiteboxServer(t)

	stream := &fakeBulkStream{
		ctx:     context.Background(),
		recvErr: errors.New("network error"),
	}

	err := s.BulkLink(stream)
	assert.Error(t, err)
}
