package tlsconf

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateEd25519Cert creates a self-signed Ed25519 certificate and returns
// certPEM, keyPEM, and the parsed *x509.Certificate.
func generateEd25519Cert(t *testing.T) ([]byte, []byte, *x509.Certificate) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ed25519"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})

	return certPEM, keyPEM, cert
}

// generateECDSACert creates a self-signed ECDSA P-256 certificate.
func generateECDSACert(t *testing.T) *x509.Certificate {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-ecdsa"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return cert
}

func TestPeerPublicKey(t *testing.T) {
	t.Parallel()

	_, _, ed25519Cert := generateEd25519Cert(t)
	ecdsaCert := generateECDSACert(t)

	cases := []struct {
		name    string
		chain   []*x509.Certificate
		wantErr bool
	}{
		{
			name:  "ed25519 key extracted successfully",
			chain: []*x509.Certificate{ed25519Cert},
		},
		{
			name:    "empty chain returns error",
			chain:   nil,
			wantErr: true,
		},
		{
			name:    "ecdsa cert returns error",
			chain:   []*x509.Certificate{ecdsaCert},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pub, err := PeerPublicKey(tc.chain)
			if tc.wantErr {
				assert.Error(t, err)
				assert.Nil(t, pub)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, pub)
				assert.IsType(t, ed25519.PublicKey{}, pub)
			}
		})
	}
}

func TestLoadServerTLS(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM, _ := generateEd25519Cert(t)
	caPEM := certPEM // self-signed: cert is its own CA

	cfg, err := LoadServerTLS(certPEM, keyPEM, caPEM)
	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion)
	assert.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
	assert.NotNil(t, cfg.ClientCAs)
}

func TestLoadClientTLS(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM, _ := generateEd25519Cert(t)
	caPEM := certPEM

	cfg, err := LoadClientTLS(certPEM, keyPEM, caPEM)
	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion)
	assert.NotNil(t, cfg.RootCAs)
}

func TestLoadServerTLSInvalidCert(t *testing.T) {
	t.Parallel()

	_, err := LoadServerTLS([]byte("not a cert"), []byte("not a key"), nil)
	assert.Error(t, err)
}

func TestLoadClientTLSError(t *testing.T) {
	t.Parallel()

	_, err := LoadClientTLS([]byte("not a cert"), []byte("not a key"), nil)
	assert.Error(t, err)
}

func TestLoadClientTLSGarbageCA(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM, _ := generateEd25519Cert(t)
	// Garbage caPEM causes pem.Decode to return nil immediately — benign, pool is empty.
	cfg, err := LoadClientTLS(certPEM, keyPEM, []byte("not-a-pem"))
	require.NoError(t, err)
	assert.NotNil(t, cfg)
}

func TestLoadClientTLSInvalidCACert(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM, _ := generateEd25519Cert(t)
	// Valid PEM envelope but non-DER bytes inside — x509.ParseCertificate fails.
	garbage := "-----BEGIN CERTIFICATE-----\nbm90LXZhbGlkLWRlcg==\n-----END CERTIFICATE-----\n"
	_, err := LoadClientTLS(certPEM, keyPEM, []byte(garbage))
	assert.Error(t, err)
}
