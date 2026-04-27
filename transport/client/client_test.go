package client_test

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
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocksriracha "go.sriracha.dev/mock/sriracha"
	"go.sriracha.dev/sriracha"
	. "go.sriracha.dev/transport/client"
	"go.sriracha.dev/transport/internal/replay"
	"go.sriracha.dev/transport/internal/tlsconf"
	srirachav1 "go.sriracha.dev/transport/proto/sriracha/v1"
	"go.sriracha.dev/transport/server"
)

type testPKI struct {
	serverCert tls.Certificate
	clientCert tls.Certificate
	clientPriv ed25519.PrivateKey
	caPool     *x509.CertPool
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()

	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caPub, caPriv)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	makeCert := func(cn string, ipSANs []net.IP) (tls.Certificate, ed25519.PrivateKey) {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)

		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			IPAddresses:  ipSANs,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, pub, caPriv)
		require.NoError(t, err)

		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		privDER, err := x509.MarshalPKCS8PrivateKey(priv)
		require.NoError(t, err)
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})

		tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
		require.NoError(t, err)
		return tlsCert, priv
	}

	srvCert, _ := makeCert("org.example.b", []net.IP{net.ParseIP("127.0.0.1")})
	cliCert, cliPriv := makeCert("org.example.a", nil)

	return &testPKI{
		serverCert: srvCert,
		clientCert: cliCert,
		clientPriv: cliPriv,
		caPool:     caPool,
	}
}

func (p *testPKI) serverTLSConfig() *tls.Config {
	return tlsconf.ServerTLS(p.serverCert, p.caPool)
}

func (p *testPKI) clientTLSConfig() *tls.Config {
	cfg := tlsconf.ClientTLS(p.clientCert, p.caPool)
	cfg.ServerName = "127.0.0.1"
	return cfg
}

func startTestServer(t *testing.T, pki *testPKI) string {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache := replay.New(ctx)

	indexer := mocksriracha.NewMockTokenIndexer(t)
	source := mocksriracha.NewMockRecordSource(t)
	audit := mocksriracha.NewMockAuditLog(t)
	audit.EXPECT().Append(mock.Anything, mock.Anything).Return(nil).Maybe()

	indexer.EXPECT().Match(mock.Anything, mock.Anything, mock.Anything).
		Return([]sriracha.Candidate{{RecordID: "rec-1", Confidence: 1.0}}, nil).Maybe()
	source.EXPECT().Fetch(mock.Anything, "rec-1").
		Return(sriracha.RawRecord{}, nil).Maybe()

	cfg := server.Config{
		InstitutionID:    "org.example.b",
		SpecVersion:      "0.1.0",
		SupportedFields:  []string{sriracha.FieldNameGiven.String()},
		FieldSetVersions: []string{"1.0.0-test"},
		SupportedModes:   []sriracha.MatchMode{sriracha.Deterministic},
	}

	srv, err := server.New(
		server.WithConfig(cfg),
		server.WithTokenIndexer(indexer),
		server.WithRecordSource(source),
		server.WithTLSConfig(pki.serverTLSConfig()),
		server.WithReplayCache(cache),
		server.WithAuditLog(audit),
	)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.GracefulStop(context.Background()) })

	return lis.Addr().String()
}

func TestNewClient(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	addr := startTestServer(t, pki)

	ctx := context.Background()
	c, err := New(ctx, Config{
		ServerAddr: addr,
		TLSConfig:  pki.clientTLSConfig(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	caps := c.Capabilities()
	assert.Equal(t, "0.1.0", caps.SpecVersion)
}

func TestNewClientUnreachable(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	t.Cleanup(cancel)

	_, err := New(ctx, Config{
		ServerAddr: "127.0.0.1:1", // nothing listening
		TLSConfig:  pki.clientTLSConfig(),
	})
	assert.Error(t, err)
}

func TestNewClientValidation(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	addr := startTestServer(t, pki)
	ctx := context.Background()

	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "empty server addr",
			cfg:     Config{TLSConfig: pki.clientTLSConfig()},
			wantErr: true,
		},
		{
			name:    "nil TLS config",
			cfg:     Config{ServerAddr: addr},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(ctx, tc.cfg)
			assert.Error(t, err)
		})
	}
}

func TestClientClose(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	addr := startTestServer(t, pki)

	c, err := New(context.Background(), Config{
		ServerAddr: addr,
		TLSConfig:  pki.clientTLSConfig(),
	})
	require.NoError(t, err)
	assert.NoError(t, c.Close())
}

func TestNewQueryRequest(t *testing.T) {
	t.Parallel()

	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "1.0.0-test",
		Mode:            sriracha.Deterministic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("payload"),
		Checksum:        checksum,
	}

	now := time.Now()
	policy := &srirachav1.ConsentPolicy{
		PolicyId:  "pol-1",
		IssuerId:  "org.a",
		TargetId:  "org.b",
		Purpose:   "test",
		IssuedAt:  now.Add(-time.Minute).Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(),
	}

	req, err := NewQueryRequest(tr, "1.0.0-test", []string{"sriracha::name::given"}, policy, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, req.SessionId)
	assert.NotEmpty(t, req.TokenRecord)
	assert.Equal(t, "1.0.0-test", req.FieldsetVersion)
	assert.Equal(t, srirachav1.MatchMode_MATCH_MODE_DETERMINISTIC, req.MatchMode)
}

func TestNewQueryRequestProbabilistic(t *testing.T) {
	t.Parallel()

	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "1.0.0-test",
		Mode:            sriracha.Probabilistic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("payload"),
		Checksum:        checksum,
	}

	req, err := NewQueryRequest(tr, "1.0.0-test", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, srirachav1.MatchMode_MATCH_MODE_PROBABILISTIC, req.MatchMode)
}

func TestNewQueryRequestInvalidMode(t *testing.T) {
	t.Parallel()

	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "1.0.0-test",
		Mode:            sriracha.MatchMode(99),
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("payload"),
		Checksum:        checksum,
	}

	_, err := NewQueryRequest(tr, "1.0.0-test", nil, nil, nil)
	assert.Error(t, err)
}

// signTestPolicy signs a ConsentPolicy using the provided Ed25519 private key.
// The message format mirrors consent.policyMessage (domain prefix +
// length-prefixed fields + big-endian timestamps).
func signTestPolicy(t *testing.T, priv ed25519.PrivateKey, p *srirachav1.ConsentPolicy) {
	t.Helper()
	const domain = "sriracha.consent.v1\x00"
	fields := []string{p.PolicyId, p.IssuerId, p.TargetId, p.Purpose}
	var buf []byte
	buf = append(buf, domain...)
	var lp [4]byte
	for _, f := range fields {
		binary.BigEndian.PutUint32(lp[:], uint32(len(f))) //nolint:gosec // G115: policy field length bounded by validation
		buf = append(buf, lp[:]...)
		buf = append(buf, f...)
	}
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(p.IssuedAt)) //nolint:gosec // G115: bit-pattern serialisation for HMAC; sign is irrelevant
	buf = append(buf, ts[:]...)
	binary.BigEndian.PutUint64(ts[:], uint64(p.ExpiresAt)) //nolint:gosec // G115: bit-pattern serialisation for HMAC; sign is irrelevant
	buf = append(buf, ts[:]...)
	hash := sha256.Sum256(buf)
	p.Signature = ed25519.Sign(priv, hash[:])
}

func newTestPolicy(t *testing.T, pki *testPKI, policyID string) *srirachav1.ConsentPolicy {
	t.Helper()
	now := time.Now()
	p := &srirachav1.ConsentPolicy{
		PolicyId:  policyID,
		IssuerId:  "org.example.a",
		TargetId:  "org.example.b",
		Purpose:   "test",
		IssuedAt:  now.Add(-time.Minute).Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(),
	}
	signTestPolicy(t, pki.clientPriv, p)
	return p
}

func TestClientQuery(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	addr := startTestServer(t, pki)

	c, err := New(context.Background(), Config{
		ServerAddr: addr,
		TLSConfig:  pki.clientTLSConfig(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "1.0.0-test",
		Mode:            sriracha.Deterministic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("payload"),
		Checksum:        checksum,
	}

	req, err := NewQueryRequest(tr, "1.0.0-test", []string{sriracha.FieldNameGiven.String()},
		newTestPolicy(t, pki, "pol-client-query-1"), nil)
	require.NoError(t, err)

	resp, err := c.Query(context.Background(), req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestClientQueryEmptySessionID(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	addr := startTestServer(t, pki)

	c, err := New(context.Background(), Config{
		ServerAddr: addr,
		TLSConfig:  pki.clientTLSConfig(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	trBytes, err := server.TokenRecordToProto(sriracha.TokenRecord{
		FieldSetVersion: "1.0.0-test",
		Mode:            sriracha.Deterministic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("payload"),
	})
	require.NoError(t, err)

	// Empty SessionId is now rejected at the client level.
	resp, err := c.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "",
		TokenRecord:     trBytes,
		FieldsetVersion: "1.0.0-test",
		Policy:          newTestPolicy(t, pki, "pol-empty-session"),
	})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestClientBulkLink(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	addr := startTestServer(t, pki)

	c, err := New(context.Background(), Config{
		ServerAddr: addr,
		TLSConfig:  pki.clientTLSConfig(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	stream, err := c.BulkLink(context.Background())
	require.NoError(t, err)

	var checksum [32]byte
	tr := sriracha.TokenRecord{
		FieldSetVersion: "1.0.0-test",
		Mode:            sriracha.Deterministic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("payload"),
		Checksum:        checksum,
	}
	trBytes, err := server.TokenRecordToProto(tr)
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkLinkRequest{
		SessionId:    "bulk-client-1",
		TokenRecords: [][]byte{trBytes},
		RecordRefs:   []string{"ref-1"},
		Policy:       newTestPolicy(t, pki, "pol-client-bulk-1"),
	})
	require.NoError(t, err)

	result, err := stream.Recv()
	require.NoError(t, err)
	assert.Len(t, result.Entries, 1)
	require.NoError(t, stream.CloseSend())
}

func TestNewClientInvalidTarget(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	// A null byte in the target makes grpc.NewClient fail during URI parsing.
	_, err := New(context.Background(), Config{
		ServerAddr: "\x00",
		TLSConfig:  pki.clientTLSConfig(),
	})
	assert.Error(t, err)
}
