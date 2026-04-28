//go:build integration

package integration_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// alpnH2 mirrors the ALPN identifier set by transport/internal/tlsconf so the
// gRPC handshake negotiates HTTP/2 explicitly. We inline this rather than
// import the internal helper.
var alpnH2 = []string{"h2"}

func serverTLSConfig(cert tls.Certificate, caPool *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   alpnH2,
	}
}

func clientTLSConfig(cert tls.Certificate, caPool *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   alpnH2,
	}
}

// integrationPKI is a minimal one-CA PKI used to mint server and client leaf
// certificates for two parties. All keys are Ed25519, matching the Sriracha
// protocol's enforced peer-key type (see tlsconf.PeerPublicKey).
type integrationPKI struct {
	caPool *x509.CertPool
	caCert *x509.Certificate
	caPriv ed25519.PrivateKey
}

func newIntegrationPKI(t testing.TB) *integrationPKI {
	t.Helper()

	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "sriracha-integration-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, caPub, caPriv)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return &integrationPKI{caPool: pool, caCert: caCert, caPriv: caPriv}
}

// leaf is one institution's certificate, private key, and ready-made TLS
// configs. The signing key is exposed so the same identity can sign consent
// policies presented to a peer server.
type leaf struct {
	cn      string
	cert    tls.Certificate
	signKey ed25519.PrivateKey
	server  *tls.Config
	client  *tls.Config
}

// mintLeaf issues a leaf cert with cn as both Common Name and the
// peer-extracted institution identifier (transport/server.peerInstitutionID
// falls back to CN when no URI SAN is present). For server-side use the
// loopback IP SAN ensures the gRPC client TLS verification (with ServerName
// 127.0.0.1) succeeds.
func (p *integrationPKI) mintLeaf(t testing.TB, cn string) *leaf {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, pub, p.caPriv)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	clientCfg := clientTLSConfig(tlsCert, p.caPool)
	clientCfg.ServerName = "127.0.0.1"

	return &leaf{
		cn:      cn,
		cert:    tlsCert,
		signKey: priv,
		server:  serverTLSConfig(tlsCert, p.caPool),
		client:  clientCfg,
	}
}
