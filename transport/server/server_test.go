package server_test

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
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	mocksriracha "go.sriracha.dev/mock/sriracha"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/transport/internal/replay"
	"go.sriracha.dev/transport/internal/tlsconf"
	srirachav1 "go.sriracha.dev/transport/proto/srirachav1"
	. "go.sriracha.dev/transport/server"
)

// testPKI holds a minimal PKI for integration tests: one CA, one server cert,
// one client cert. All keys are Ed25519.
type testPKI struct {
	serverCert tls.Certificate
	clientCert tls.Certificate
	clientPriv ed25519.PrivateKey
	caPool     *x509.CertPool
	caCert     *x509.Certificate
	caPriv     ed25519.PrivateKey
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()

	// CA
	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caPub, caPriv)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	makeCert := func(cn string, ipSANs []net.IP) (tls.Certificate, ed25519.PublicKey, ed25519.PrivateKey) {
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
		return tlsCert, pub, priv
	}

	loopback := []net.IP{net.ParseIP("127.0.0.1")}
	serverTLSCert, _, _ := makeCert("org.example.b", loopback)
	clientTLSCert, _, clientPriv := makeCert("org.example.a", nil)

	return &testPKI{
		serverCert: serverTLSCert,
		clientCert: clientTLSCert,
		clientPriv: clientPriv,
		caPool:     caPool,
		caCert:     caCert,
		caPriv:     caPriv,
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

func (p *testPKI) ecdsaClientTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(999),
		Subject:      pkix.Name{CommonName: "org.example.ecdsa"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &ecKey.PublicKey, p.caPriv)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(ecKey)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	cfg := tlsconf.ClientTLS(tlsCert, p.caPool)
	cfg.ServerName = "127.0.0.1"
	return cfg
}

func (p *testPKI) signPolicy(policy *srirachav1.ConsentPolicy) {
	msg := policyMessage(policy)
	hash := sha256.Sum256(msg)
	policy.Signature = ed25519.Sign(p.clientPriv, hash[:])
}

// policyMessage mirrors the internal consent.policyMessage function.
func policyMessage(p *srirachav1.ConsentPolicy) []byte {
	var buf []byte
	buf = append(buf, []byte(p.PolicyId)...)
	buf = append(buf, []byte(p.IssuerId)...)
	buf = append(buf, []byte(p.TargetId)...)
	buf = append(buf, []byte(p.Purpose)...)
	var ts [8]byte
	for _, v := range []int64{p.IssuedAt, p.ExpiresAt} {
		ts[0] = byte(v >> 56)
		ts[1] = byte(v >> 48)
		ts[2] = byte(v >> 40)
		ts[3] = byte(v >> 32)
		ts[4] = byte(v >> 24)
		ts[5] = byte(v >> 16)
		ts[6] = byte(v >> 8)
		ts[7] = byte(v)
		buf = append(buf, ts[:]...)
	}
	return buf
}

const (
	issuerID = "org.example.a" // matches client cert CommonName
	targetID = "org.example.b"
)

func testServerConfig() Config {
	return Config{
		InstitutionID: targetID,
		SpecVersion:   "0.1.0",
		SupportedFields: []string{
			sriracha.FieldNameGiven.String(),
			sriracha.FieldNameFamily.String(),
			sriracha.FieldDateBirth.String(),
		},
		FieldSetVersions: []string{"test-v1"},
		SupportedModes:   []sriracha.MatchMode{sriracha.Deterministic, sriracha.Probabilistic},
	}
}

type testEnv struct {
	pki     *testPKI
	cache   replay.Cache
	indexer *mocksriracha.MockTokenIndexer
	source  *mocksriracha.MockRecordSource
	audit   *mocksriracha.MockAuditLog
	addr    string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	pki := newTestPKI(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache := replay.New(ctx)

	indexer := mocksriracha.NewMockTokenIndexer(t)
	source := mocksriracha.NewMockRecordSource(t)
	audit := mocksriracha.NewMockAuditLog(t)

	srv, err := New(testServerConfig(), indexer, source, pki.serverTLSConfig(), cache, audit)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	return &testEnv{
		pki:     pki,
		cache:   cache,
		indexer: indexer,
		source:  source,
		audit:   audit,
		addr:    lis.Addr().String(),
	}
}

func (e *testEnv) newClient(t *testing.T) srirachav1.SrirachaServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(e.addr,
		grpc.WithTransportCredentials(credentials.NewTLS(e.pki.clientTLSConfig())),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return srirachav1.NewSrirachaServiceClient(conn)
}

func (e *testEnv) newPolicy(t *testing.T) *srirachav1.ConsentPolicy {
	t.Helper()
	now := time.Now()
	p := &srirachav1.ConsentPolicy{
		PolicyId:  "pol-" + t.Name(),
		IssuerId:  issuerID,
		TargetId:  targetID,
		Purpose:   "testing",
		IssuedAt:  now.Add(-time.Minute).Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(),
	}
	e.pki.signPolicy(p)
	return p
}

func (e *testEnv) expectAudit(t *testing.T) {
	t.Helper()
	e.audit.EXPECT().Append(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
}

func TestNopAuditLogVerify(t *testing.T) {
	t.Parallel()
	assert.NoError(t, NopAuditLog{}.Verify(context.Background()))
}

func TestGetCapabilities(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	resp, err := client.GetCapabilities(context.Background(), &srirachav1.CapabilitiesRequest{})
	require.NoError(t, err)

	assert.Equal(t, "0.1.0", resp.SpecVersion)
	assert.Equal(t, []string{"test-v1"}, resp.FieldsetVersions)
	assert.NotEmpty(t, resp.SupportedFields)
	assert.NotEmpty(t, resp.MatchModes)
}

func testTokenRecord(t *testing.T) sriracha.TokenRecord {
	t.Helper()
	var checksum [32]byte
	checksum[0] = 1
	return sriracha.TokenRecord{
		FieldSetVersion: "test-v1",
		Mode:            sriracha.Deterministic,
		Algo:            sriracha.AlgoHMACSHA256V1,
		Payload:         []byte("test-payload"),
		Checksum:        checksum,
	}
}

func TestQuery(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		candidates []sriracha.Candidate
		fetchRec   sriracha.RawRecord
		wantStatus srirachav1.MatchStatus
		wantFields int
	}{
		{
			name:       "no match",
			candidates: nil,
			wantStatus: srirachav1.MatchStatus_NO_MATCH,
		},
		{
			name: "deterministic match",
			candidates: []sriracha.Candidate{
				{RecordID: "rec-1", Confidence: 1.0},
			},
			fetchRec: sriracha.RawRecord{
				sriracha.FieldNameGiven:  "Alice",
				sriracha.FieldNameFamily: "Smith",
			},
			wantStatus: srirachav1.MatchStatus_MATCHED,
			wantFields: 2,
		},
		{
			name: "multiple candidates",
			candidates: []sriracha.Candidate{
				{RecordID: "rec-1", Confidence: 0.95},
				{RecordID: "rec-2", Confidence: 0.949},
			},
			wantStatus: srirachav1.MatchStatus_MULTIPLE_CANDIDATES,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			env.expectAudit(t)
			client := env.newClient(t)

			tr := testTokenRecord(t)
			trBytes, err := TokenRecordToProto(tr)
			require.NoError(t, err)

			env.indexer.EXPECT().
				Match(mock.Anything, mock.Anything, mock.Anything).
				Return(tc.candidates, nil)

			if len(tc.candidates) == 1 && tc.candidates[0].Confidence == 1.0 {
				env.source.EXPECT().
					Fetch(mock.Anything, tc.candidates[0].RecordID).
					Return(tc.fetchRec, nil)
			}

			resp, err := client.Query(context.Background(), &srirachav1.QueryRequest{
				SessionId:       "sess-1",
				TokenRecord:     trBytes,
				FieldsetVersion: "test-v1",
				MatchMode:       srirachav1.MatchMode_DETERMINISTIC,
				RequestedFields: []string{
					sriracha.FieldNameGiven.String(),
					sriracha.FieldNameFamily.String(),
				},
				Policy: env.newPolicy(t),
			})

			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, resp.Status)
			assert.Len(t, resp.Fields, tc.wantFields)
		})
	}
}

func TestQueryMissingPolicy(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	_, err = client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-1",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
	})
	assert.Error(t, err)
}

func TestQueryInvalidPolicy(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	trBytes, err := TokenRecordToProto(testTokenRecord(t))
	require.NoError(t, err)

	p := env.newPolicy(t)
	p.Signature = []byte("wrong-signature")

	_, err = client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-invalid-policy",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
		Policy:          p,
	})
	assert.Error(t, err)
}

func TestQueryNotHeld(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.expectAudit(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	resp, err := client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-noteld",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
		RequestedFields: []string{
			sriracha.FieldNameGiven.String(),
			"sriracha::contact::fax", // unsupported field
		},
		Policy: env.newPolicy(t),
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"sriracha::contact::fax"}, resp.NotHeld)
}

func TestBulkLink(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	candidates := []sriracha.Candidate{{RecordID: "rec-1", Confidence: 1.0}}
	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return(candidates, nil)
	env.source.EXPECT().
		Fetch(mock.Anything, "rec-1").
		Return(sriracha.RawRecord{
			sriracha.FieldNameGiven: "Bob",
		}, nil)

	stream, err := client.BulkLink(context.Background())
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkTokenBatch{
		SessionId:    "bulk-sess-1",
		TokenRecords: [][]byte{trBytes},
		RecordRefs:   []string{"ref-a"},
		Policy:       env.newPolicy(t),
	})
	require.NoError(t, err)

	result, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, result.Entries, 1)
	assert.Equal(t, "ref-a", result.Entries[0].RecordRef)
	assert.Equal(t, srirachav1.MatchStatus_MATCHED, result.Entries[0].Status)

	require.NoError(t, stream.CloseSend())
}

func TestBulkLinkMissingPolicy(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	stream, err := client.BulkLink(context.Background())
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkTokenBatch{
		SessionId:    "bulk-no-policy",
		TokenRecords: [][]byte{trBytes},
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	assert.Error(t, err)
}

func TestQueryMalformedToken(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	_, err := client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-bad-token",
		TokenRecord:     []byte{0xFF, 0xFE, 0x01}, // invalid proto bytes
		FieldsetVersion: "test-v1",
		Policy:          env.newPolicy(t),
	})
	assert.Error(t, err)
}

func TestQuerySingleProbabilisticMatch(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.expectAudit(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	// Single candidate with confidence < 1.0 and no close second candidate.
	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return([]sriracha.Candidate{{RecordID: "rec-1", Confidence: 0.90}}, nil)
	env.source.EXPECT().
		Fetch(mock.Anything, "rec-1").
		Return(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, nil)

	resp, err := client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-prob-single",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
		RequestedFields: []string{sriracha.FieldNameGiven.String()},
		Policy:          env.newPolicy(t),
	})
	require.NoError(t, err)
	assert.Equal(t, srirachav1.MatchStatus_MATCHED, resp.Status)
	assert.InDelta(t, 0.90, float64(resp.Confidence), 0.001)
}

func TestBulkLinkFetchError(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return([]sriracha.Candidate{{RecordID: "rec-err", Confidence: 1.0}}, nil)
	env.source.EXPECT().
		Fetch(mock.Anything, "rec-err").
		Return(nil, sriracha.ErrRecordNotFound("rec-err"))

	stream, err := client.BulkLink(context.Background())
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkTokenBatch{
		SessionId:    "bulk-fetch-err",
		TokenRecords: [][]byte{trBytes},
		RecordRefs:   []string{"ref-a"},
		Policy:       env.newPolicy(t),
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	assert.Error(t, err)
}

func TestQueryIndexerError(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, sriracha.ErrIndexCorrupted("simulated index failure"))

	_, err = client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-idx-err",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
		Policy:          env.newPolicy(t),
	})
	assert.Error(t, err)
}

func TestNewServerNilAudit(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache := replay.New(ctx)

	indexer := mocksriracha.NewMockTokenIndexer(t)
	source := mocksriracha.NewMockRecordSource(t)

	srv, err := New(testServerConfig(), indexer, source, pki.serverTLSConfig(), cache, nil)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(pki.clientTLSConfig())),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	stub := srirachav1.NewSrirachaServiceClient(conn)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	indexer.EXPECT().Match(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	env := &testEnv{pki: pki}
	resp, err := stub.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-nop-audit",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
		Policy:          env.newPolicy(t),
	})
	require.NoError(t, err)
	assert.Equal(t, srirachav1.MatchStatus_NO_MATCH, resp.Status)
}

func TestQueryRecordFieldNotHeld(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.expectAudit(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return([]sriracha.Candidate{{RecordID: "rec-1", Confidence: 1.0}}, nil)
	env.source.EXPECT().
		Fetch(mock.Anything, "rec-1").
		Return(sriracha.RawRecord{
			sriracha.FieldNameGiven: string(sriracha.NotHeld),
		}, nil)

	resp, err := client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-notheld-rec",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
		RequestedFields: []string{sriracha.FieldNameGiven.String()},
		Policy:          env.newPolicy(t),
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Fields)
	assert.Equal(t, []string{sriracha.FieldNameGiven.String()}, resp.NotFound)
}

func TestQueryWithMatchConfig(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.expectAudit(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	resp, err := client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-matchcfg",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
		MatchConfig: &srirachav1.MatchConfig{
			Threshold:  0.8,
			MaxResults: 5,
			FieldWeights: []*srirachav1.FieldWeight{
				{FieldPath: sriracha.FieldNameGiven.String(), Weight: 0.5},
				{FieldPath: "invalid::field", Weight: 0.1},
			},
		},
		Policy: env.newPolicy(t),
	})
	require.NoError(t, err)
	assert.Equal(t, srirachav1.MatchStatus_NO_MATCH, resp.Status)
}

func TestNewServerValidation(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache := replay.New(ctx)
	idx := mocksriracha.NewMockTokenIndexer(t)
	src := mocksriracha.NewMockRecordSource(t)

	cases := []struct {
		name    string
		cfg     Config
		idx     sriracha.TokenIndexer
		src     sriracha.RecordSource
		tlsCfg  *tls.Config
		cache   replay.Cache
		wantErr bool
	}{
		{
			name:    "empty institution ID",
			cfg:     Config{},
			idx:     idx,
			src:     src,
			tlsCfg:  pki.serverTLSConfig(),
			cache:   cache,
			wantErr: true,
		},
		{
			name:    "nil indexer",
			cfg:     Config{InstitutionID: "x"},
			idx:     nil,
			src:     src,
			tlsCfg:  pki.serverTLSConfig(),
			cache:   cache,
			wantErr: true,
		},
		{
			name:    "nil source",
			cfg:     Config{InstitutionID: "x"},
			idx:     idx,
			src:     nil,
			tlsCfg:  pki.serverTLSConfig(),
			cache:   cache,
			wantErr: true,
		},
		{
			name:    "nil TLS config",
			cfg:     Config{InstitutionID: "x"},
			idx:     idx,
			src:     src,
			tlsCfg:  nil,
			cache:   cache,
			wantErr: true,
		},
		{
			name:    "nil cache",
			cfg:     Config{InstitutionID: "x"},
			idx:     idx,
			src:     src,
			tlsCfg:  pki.serverTLSConfig(),
			cache:   nil,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tc.cfg, tc.idx, tc.src, tc.tlsCfg, tc.cache, nil)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestQueryUnsupportedFieldsetVersion(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	tr := testTokenRecord(t)
	trBytes, err := TokenRecordToProto(tr)
	require.NoError(t, err)

	_, err = client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-bad-version",
		TokenRecord:     trBytes,
		FieldsetVersion: "unknown-v99",
		Policy:          env.newPolicy(t),
	})
	require.Error(t, err)
	s, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, s.Code())
}

func TestGetCapabilitiesInvalidMode(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache := replay.New(ctx)

	cfg := testServerConfig()
	cfg.SupportedModes = []sriracha.MatchMode{sriracha.MatchMode(99)} // invalid — skipped in GetCapabilities

	srv, err := New(cfg, mocksriracha.NewMockTokenIndexer(t), mocksriracha.NewMockRecordSource(t),
		pki.serverTLSConfig(), cache, nil)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(pki.clientTLSConfig())),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	stub := srirachav1.NewSrirachaServiceClient(conn)

	resp, err := stub.GetCapabilities(context.Background(), &srirachav1.CapabilitiesRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.MatchModes)
}

func TestQueryFetchError(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	trBytes, err := TokenRecordToProto(testTokenRecord(t))
	require.NoError(t, err)

	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return([]sriracha.Candidate{{RecordID: "rec-fetch-err", Confidence: 1.0}}, nil)
	env.source.EXPECT().
		Fetch(mock.Anything, "rec-fetch-err").
		Return(nil, sriracha.ErrRecordNotFound("rec-fetch-err"))

	_, err = client.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-fetch-err",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
		Policy:          env.newPolicy(t),
	})
	assert.Error(t, err)
}

func TestBulkLinkNoMatch(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	trBytes, err := TokenRecordToProto(testTokenRecord(t))
	require.NoError(t, err)

	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	stream, err := client.BulkLink(context.Background())
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkTokenBatch{
		SessionId:    "bulk-no-match",
		TokenRecords: [][]byte{trBytes},
		RecordRefs:   []string{"ref-nm"},
		Policy:       env.newPolicy(t),
	})
	require.NoError(t, err)

	result, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, result.Entries, 1)
	assert.Equal(t, srirachav1.MatchStatus_NO_MATCH, result.Entries[0].Status)
	require.NoError(t, stream.CloseSend())
}

func TestBulkLinkMultipleCandidates(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	trBytes, err := TokenRecordToProto(testTokenRecord(t))
	require.NoError(t, err)

	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return([]sriracha.Candidate{
			{RecordID: "rec-1", Confidence: 0.95},
			{RecordID: "rec-2", Confidence: 0.945}, // gap < 0.01 → MULTIPLE_CANDIDATES
		}, nil)

	stream, err := client.BulkLink(context.Background())
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkTokenBatch{
		SessionId:    "bulk-multi",
		TokenRecords: [][]byte{trBytes},
		RecordRefs:   []string{"ref-mc"},
		Policy:       env.newPolicy(t),
	})
	require.NoError(t, err)

	result, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, result.Entries, 1)
	assert.Equal(t, srirachav1.MatchStatus_MULTIPLE_CANDIDATES, result.Entries[0].Status)
	assert.InDelta(t, 0.95, float64(result.Entries[0].Confidence), 0.001)
	require.NoError(t, stream.CloseSend())
}

func TestBulkLinkMalformedToken(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	stream, err := client.BulkLink(context.Background())
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkTokenBatch{
		SessionId:    "bulk-bad-token",
		TokenRecords: [][]byte{{0xFF, 0xFE}}, // invalid proto bytes → NO_MATCH entry
		RecordRefs:   []string{"ref-bad"},
		Policy:       env.newPolicy(t),
	})
	require.NoError(t, err)

	result, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, result.Entries, 1)
	assert.Equal(t, srirachav1.MatchStatus_NO_MATCH, result.Entries[0].Status)
	require.NoError(t, stream.CloseSend())
}

func TestBulkLinkIndexerError(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	trBytes, err := TokenRecordToProto(testTokenRecord(t))
	require.NoError(t, err)

	env.indexer.EXPECT().
		Match(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, sriracha.ErrIndexCorrupted("simulated index failure"))

	stream, err := client.BulkLink(context.Background())
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkTokenBatch{
		SessionId:    "bulk-idx-err",
		TokenRecords: [][]byte{trBytes},
		RecordRefs:   []string{"ref-ie"},
		Policy:       env.newPolicy(t),
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	assert.Error(t, err)
}

func TestBulkLinkInvalidPolicy(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	trBytes, err := TokenRecordToProto(testTokenRecord(t))
	require.NoError(t, err)

	p := env.newPolicy(t)
	p.Signature = []byte("wrong-signature")

	stream, err := client.BulkLink(context.Background())
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkTokenBatch{
		SessionId:    "bulk-bad-policy",
		TokenRecords: [][]byte{trBytes},
		RecordRefs:   []string{"ref-bp"},
		Policy:       p,
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	assert.Error(t, err)
}

func TestBulkLinkContextCancel(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	client := env.newClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.BulkLink(ctx)
	require.NoError(t, err)

	// Cancel immediately — server's stream.Recv() returns a non-EOF error.
	cancel()

	_, err = stream.Recv()
	assert.Error(t, err)
}

// TestQueryECDSAClient covers peerIdentity rejecting a non-Ed25519 cert (server.go:337-339)
// and the Query handler returning the peerIdentity error (server.go:156-158).
func TestQueryECDSAClient(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	ecdsaTLS := env.pki.ecdsaClientTLSConfig(t)

	conn, err := grpc.NewClient(env.addr,
		grpc.WithTransportCredentials(credentials.NewTLS(ecdsaTLS)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	stub := srirachav1.NewSrirachaServiceClient(conn)

	trBytes, err := TokenRecordToProto(testTokenRecord(t))
	require.NoError(t, err)

	_, err = stub.Query(context.Background(), &srirachav1.QueryRequest{
		SessionId:       "sess-ecdsa",
		TokenRecord:     trBytes,
		FieldsetVersion: "test-v1",
		Policy:          env.newPolicy(t),
	})
	assert.Error(t, err)
}

// TestBulkLinkECDSAClient covers the BulkLink handler returning the peerIdentity error (server.go:233-235).
func TestBulkLinkECDSAClient(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	ecdsaTLS := env.pki.ecdsaClientTLSConfig(t)

	conn, err := grpc.NewClient(env.addr,
		grpc.WithTransportCredentials(credentials.NewTLS(ecdsaTLS)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	stub := srirachav1.NewSrirachaServiceClient(conn)

	trBytes, err := TokenRecordToProto(testTokenRecord(t))
	require.NoError(t, err)

	stream, err := stub.BulkLink(context.Background())
	require.NoError(t, err)

	err = stream.Send(&srirachav1.BulkTokenBatch{
		SessionId:    "bulk-ecdsa",
		TokenRecords: [][]byte{trBytes},
		RecordRefs:   []string{"ref-ecdsa"},
		Policy:       env.newPolicy(t),
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	assert.Error(t, err)
}
