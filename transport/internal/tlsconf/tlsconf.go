package tlsconf

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
)

// alpnH2 is the ALPN identifier for HTTP/2, the only protocol gRPC supports.
// Setting it explicitly avoids silent fallback to HTTP/1.1 if the config is
// consumed by something other than google.golang.org/grpc/credentials, which
// otherwise injects this value internally.
var alpnH2 = []string{"h2"}

// ServerTLS returns a *tls.Config for the gRPC server side.
// TLS 1.3 is the minimum version; client certificates are required and verified.
func ServerTLS(cert tls.Certificate, caPool *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   alpnH2,
	}
}

// ClientTLS returns a *tls.Config for the gRPC client side.
// TLS 1.3 is the minimum version; the client presents its certificate.
func ClientTLS(cert tls.Certificate, caPool *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   alpnH2,
	}
}

// LoadServerTLS constructs a server TLS config from PEM-encoded file contents.
func LoadServerTLS(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	cert, pool, err := loadCertAndPool(certPEM, keyPEM, caPEM)
	if err != nil {
		return nil, err
	}
	return ServerTLS(cert, pool), nil
}

// LoadClientTLS constructs a client TLS config from PEM-encoded file contents.
func LoadClientTLS(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	cert, pool, err := loadCertAndPool(certPEM, keyPEM, caPEM)
	if err != nil {
		return nil, err
	}
	return ClientTLS(cert, pool), nil
}

func loadCertAndPool(certPEM, keyPEM, caPEM []byte) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	pool := x509.NewCertPool()
	for len(caPEM) > 0 {
		var block *pem.Block
		block, caPEM = pem.Decode(caPEM)
		if block == nil {
			break
		}
		ca, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return tls.Certificate{}, nil, err
		}
		pool.AddCert(ca)
	}

	return cert, pool, nil
}

// PeerPublicKey extracts the Ed25519 public key from the first certificate in chain.
// Returns an error if chain is empty or the key type is not Ed25519.
func PeerPublicKey(chain []*x509.Certificate) (ed25519.PublicKey, error) {
	if len(chain) == 0 {
		return nil, errors.New("tlsconf: empty peer certificate chain")
	}
	pub, ok := chain[0].PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("tlsconf: peer certificate does not contain an Ed25519 public key")
	}
	return pub, nil
}
