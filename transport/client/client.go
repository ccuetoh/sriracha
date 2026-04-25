package client

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"go.sriracha.dev/sriracha"
	srirachav1 "go.sriracha.dev/transport/proto/srirachav1"
	"go.sriracha.dev/transport/server"
)

// Config holds connection parameters for a Sriracha client.
type Config struct {
	// ServerAddr is the target address in host:port form.
	ServerAddr string
	// TLSConfig is the mTLS configuration. TLS 1.3 minimum is enforced by the server.
	TLSConfig *tls.Config
}

// Client is a Sriracha client for querying a remote institution (responding party B).
// Call Close when done to release the underlying connection.
type Client struct {
	conn         *grpc.ClientConn
	stub         srirachav1.SrirachaServiceClient
	capabilities *srirachav1.CapabilitiesResponse
}

// New dials the remote Sriracha server, performs the mandatory GetCapabilities
// handshake, and returns a ready Client. Returns an error if the connection or
// handshake fails.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.ServerAddr == "" {
		return nil, fmt.Errorf("client: ServerAddr must not be empty")
	}
	if cfg.TLSConfig == nil {
		return nil, fmt.Errorf("client: TLS config must not be nil")
	}

	conn, err := grpc.NewClient(cfg.ServerAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(cfg.TLSConfig)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("client: dial %q: %w", cfg.ServerAddr, err)
	}

	stub := srirachav1.NewSrirachaServiceClient(conn)

	caps, err := stub.GetCapabilities(ctx, &srirachav1.CapabilitiesRequest{})
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("client: capabilities handshake: %w", err)
	}

	return &Client{conn: conn, stub: stub, capabilities: caps}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Capabilities returns the cached server capabilities from the initial handshake.
func (c *Client) Capabilities() *srirachav1.CapabilitiesResponse {
	return c.capabilities
}

// Query sends a single QueryRequest and returns the response.
// req.SessionId must be non-empty; use NewQueryRequest to construct a well-formed request.
func (c *Client) Query(ctx context.Context, req *srirachav1.QueryRequest) (*srirachav1.QueryResponse, error) {
	if req.SessionId == "" {
		return nil, fmt.Errorf("client: req.SessionId must not be empty")
	}
	return c.stub.Query(ctx, req)
}

// BulkLink opens a bidirectional streaming session for bulk record linkage.
// The caller is responsible for sending BulkTokenBatch messages and receiving
// BulkMatchResult messages, and for closing the send side when done.
func (c *Client) BulkLink(ctx context.Context) (srirachav1.SrirachaService_BulkLinkClient, error) {
	return c.stub.BulkLink(ctx)
}

// NewQueryRequest is a convenience constructor for a QueryRequest.
// tr is serialised using server.TokenRecordToProto.
func NewQueryRequest(
	tr sriracha.TokenRecord,
	fieldsetVersion string,
	requestedFields []string,
	policy *srirachav1.ConsentPolicy,
	cfg *srirachav1.MatchConfig,
) (*srirachav1.QueryRequest, error) {
	trBytes, err := server.TokenRecordToProto(tr)
	if err != nil {
		return nil, fmt.Errorf("client: serialise token record: %w", err)
	}

	// Mode already validated by TokenRecordToProto; map to proto directly.
	var mode srirachav1.MatchMode
	switch tr.Mode {
	case sriracha.Deterministic:
		mode = srirachav1.MatchMode_DETERMINISTIC
	case sriracha.Probabilistic:
		mode = srirachav1.MatchMode_PROBABILISTIC
	}

	return &srirachav1.QueryRequest{
		SessionId:       newSessionID(),
		TokenRecord:     trBytes,
		FieldsetVersion: fieldsetVersion,
		MatchMode:       mode,
		MatchConfig:     cfg,
		RequestedFields: requestedFields,
		Policy:          policy,
	}, nil
}

func newSessionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
