package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func TestNopAuditLogVerify(t *testing.T) {
	t.Parallel()
	assert.NoError(t, nopAuditLog{}.Verify(context.Background()))
}

func TestBuildFieldValuesInvalidPath(t *testing.T) {
	t.Parallel()
	_, notFound := buildFieldValues(sriracha.RawRecord{}, []string{"badpath"})
	assert.Equal(t, []string{"badpath"}, notFound)
}

// internalTestEnv holds the pieces needed for internal server unit tests.
type internalTestEnv struct {
	srv        *Server
	indexer    *mocksriracha.MockTokenIndexer
	source     *mocksriracha.MockRecordSource
	clientPub  ed25519.PublicKey
	clientPriv ed25519.PrivateKey
}

// newInternalTestEnv creates a minimal *Server plus client PKI for use in
// internal tests that bypass the gRPC transport layer.
func newInternalTestEnv(t *testing.T) *internalTestEnv {
	t.Helper()

	// Server key and self-signed cert.
	srvPub, srvPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	srvTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "org.example.b"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, srvTmpl, srvPub, srvPriv)
	require.NoError(t, err)
	srvCert, err := x509.ParseCertificate(srvDER)
	require.NoError(t, err)

	caPool := x509.NewCertPool()
	caPool.AddCert(srvCert)

	tlsCert := tls.Certificate{PrivateKey: srvPriv, Leaf: srvCert, Certificate: [][]byte{srvDER}}
	tlsCfg := tlsconf.ServerTLS(tlsCert, caPool)

	// Client key (not used for transport, only for policy signing).
	clientPub, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	idx := mocksriracha.NewMockTokenIndexer(t)
	src := mocksriracha.NewMockRecordSource(t)

	srv, err := New(Config{
		InstitutionID:    "org.example.b",
		SpecVersion:      "0.1.0",
		SupportedFields:  []string{sriracha.FieldNameGiven.String()},
		FieldSetVersions: []string{"test-v1"},
		SupportedModes:   []sriracha.MatchMode{sriracha.Deterministic},
	}, idx, src, tlsCfg, replay.New(ctx), nil)
	require.NoError(t, err)

	_ = clientPub
	return &internalTestEnv{srv: srv, indexer: idx, source: src, clientPub: clientPub, clientPriv: clientPriv}
}

// makePeerCtx builds a context containing a gRPC peer with the given Ed25519 cert.
func makePeerCtx(t *testing.T, cn string) (context.Context, ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	info := credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})
	return ctx, pub, priv
}

// signInternalPolicy signs a ConsentPolicy using the policyMessage wire format.
func signInternalPolicy(p *srirachav1.ConsentPolicy, priv ed25519.PrivateKey) {
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

// Query path tests —————————————————————————————————————————————————————————

func TestQueryPeerIdentityError(t *testing.T) {
	t.Parallel()
	env := newInternalTestEnv(t)

	// No peer in context → peerIdentity returns error → Query returns error.
	_, err := env.srv.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-peer-err",
		FieldsetVersion: "test-v1",
		Policy:          &srirachav1.ConsentPolicy{},
	})
	assert.Error(t, err)
}

// peerIdentity tests —————————————————————————————————————————————————————

func TestPeerIdentityNoPeer(t *testing.T) {
	t.Parallel()
	env := newInternalTestEnv(t)
	_, _, err := env.srv.peerIdentity(context.Background())
	assert.Error(t, err)
}

type fakeAuthInfo struct{}

func (fakeAuthInfo) AuthType() string { return "fake" }

func TestPeerIdentityNonTLS(t *testing.T) {
	t.Parallel()
	env := newInternalTestEnv(t)

	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: fakeAuthInfo{}})
	_, _, err := env.srv.peerIdentity(ctx)
	assert.Error(t, err)
}

func TestPeerIdentityNonEd25519Cert(t *testing.T) {
	t.Parallel()
	env := newInternalTestEnv(t)

	ecPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "ecdsa-peer"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &ecPriv.PublicKey, ecPriv)
	require.NoError(t, err)
	ecCert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	info := credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{ecCert}}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})
	_, _, err = env.srv.peerIdentity(ctx)
	assert.Error(t, err)
}

// fakeBulkStream implements SrirachaService_BulkLinkServer without gRPC transport.
type fakeBulkStream struct {
	grpc.ServerStream // nil — panics on any unoverridden method
	ctx     context.Context
	batches []*srirachav1.BulkTokenBatch
	pos     int
	sendErr error
	sent    []*srirachav1.BulkMatchResult
}

func (f *fakeBulkStream) Context() context.Context { return f.ctx }

func (f *fakeBulkStream) Recv() (*srirachav1.BulkTokenBatch, error) {
	if f.pos >= len(f.batches) {
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

// BulkLink path tests ——————————————————————————————————————————————————————

func TestBulkLinkPeerIdentityError(t *testing.T) {
	t.Parallel()
	env := newInternalTestEnv(t)

	stream := &fakeBulkStream{
		ctx: context.Background(), // no peer → peerIdentity fails
		batches: []*srirachav1.BulkTokenBatch{{
			SessionId: "bulk-peer-err",
			Policy:    &srirachav1.ConsentPolicy{},
		}},
	}
	err := env.srv.BulkLink(stream)
	assert.Error(t, err)
}

func TestBulkLinkSendError(t *testing.T) {
	t.Parallel()
	env := newInternalTestEnv(t)

	// Build a peer context with a valid Ed25519 client cert (CN = issuer).
	peerCtx, _, clientPriv := makePeerCtx(t, "org.example.a")

	now := time.Now()
	policy := &srirachav1.ConsentPolicy{
		PolicyId:  "pol-send-err-internal",
		IssuerId:  "org.example.a",
		TargetId:  "org.example.b",
		Purpose:   "test",
		IssuedAt:  now.Add(-time.Minute).Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(),
	}
	signInternalPolicy(policy, clientPriv)

	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "test-v1",
		Mode:            sriracha.Deterministic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("payload"),
		Checksum:        checksum,
	}
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	// Indexer returns no candidates so processBatch produces a result immediately.
	env.indexer.EXPECT().Match(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	stream := &fakeBulkStream{
		ctx:     peerCtx,
		sendErr: errors.New("send failed"),
		batches: []*srirachav1.BulkTokenBatch{{
			SessionId:    "bulk-send-err",
			TokenRecords: [][]byte{trBytes},
			RecordRefs:   []string{"ref-1"},
			Policy:       policy,
		}},
	}
	err = env.srv.BulkLink(stream)
	assert.Error(t, err)
}
