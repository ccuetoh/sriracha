package server

import (
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/transport/internal/consent"
	"go.sriracha.dev/transport/internal/replay"
	"go.sriracha.dev/transport/internal/tlsconf"
	srirachav1 "go.sriracha.dev/transport/proto/srirachav1"
)

// Config holds all tunable parameters for the Sriracha gRPC server.
type Config struct {
	// InstitutionID is this institution's canonical identifier (matched against ConsentPolicy.target_id).
	InstitutionID string
	// SpecVersion is the Sriracha spec version implemented, e.g. "0.1.0".
	SpecVersion string
	// SupportedFields lists the FieldPath strings this server can serve.
	SupportedFields []string
	// FieldSetVersions lists the FieldSet version strings this server supports.
	FieldSetVersions []string
	// SupportedModes lists the MatchModes this server accepts.
	SupportedModes []sriracha.MatchMode
	// RateQueriesPerMinute is the maximum number of Query RPCs per minute (0 = unlimited).
	RateQueriesPerMinute uint32
	// RateBulkRecordsPerDay is the maximum bulk records per day (0 = unlimited).
	RateBulkRecordsPerDay uint32
}

// Server is the Sriracha gRPC service (responding party B).
type Server interface {
	// Serve binds lis and blocks until the server stops or an error occurs.
	Serve(lis net.Listener) error
	// GracefulStop stops the server, waiting for in-flight RPCs to complete.
	GracefulStop()
}

type server struct {
	srirachav1.UnimplementedSrirachaServiceServer

	cfg     Config
	indexer sriracha.TokenIndexer
	source  sriracha.RecordSource
	consent consent.Validator
	audit   sriracha.AuditLog
	grpcSrv *grpc.Server
}

// New constructs a Server. Call Serve to begin accepting connections.
// audit may be nil; in that case a no-op implementation is used.
func New(
	cfg Config,
	idx sriracha.TokenIndexer,
	src sriracha.RecordSource,
	tlsCfg *tls.Config,
	cache replay.Cache,
	audit sriracha.AuditLog,
) (Server, error) {
	if cfg.InstitutionID == "" {
		return nil, errors.New("server: InstitutionID must not be empty")
	}
	if idx == nil {
		return nil, errors.New("server: TokenIndexer must not be nil")
	}
	if src == nil {
		return nil, errors.New("server: RecordSource must not be nil")
	}
	if tlsCfg == nil {
		return nil, errors.New("server: TLS config must not be nil")
	}
	if cache == nil {
		return nil, errors.New("server: replay cache must not be nil")
	}

	if audit == nil {
		audit = NopAuditLog{}
	}

	grpcSrv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCfg)),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:             30 * time.Second,
			Timeout:          10 * time.Second,
			MaxConnectionAge: time.Hour,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	srv := &server{
		cfg:     cfg,
		indexer: idx,
		source:  src,
		consent: consent.NewValidator(cfg.InstitutionID, cache),
		audit:   audit,
		grpcSrv: grpcSrv,
	}

	srirachav1.RegisterSrirachaServiceServer(grpcSrv, srv)
	return srv, nil
}

func (s *server) Serve(lis net.Listener) error {
	return s.grpcSrv.Serve(lis)
}

func (s *server) GracefulStop() {
	s.grpcSrv.GracefulStop()
}

// GetCapabilities implements SrirachaService.GetCapabilities.
func (s *server) GetCapabilities(_ context.Context, _ *srirachav1.CapabilitiesRequest) (*srirachav1.CapabilitiesResponse, error) {
	modes := make([]srirachav1.MatchMode, 0, len(s.cfg.SupportedModes))
	for _, m := range s.cfg.SupportedModes {
		pm, err := MatchModeToProto(m)
		if err != nil {
			continue
		}
		modes = append(modes, pm)
	}

	return &srirachav1.CapabilitiesResponse{
		SpecVersion:      s.cfg.SpecVersion,
		FieldsetVersions: s.cfg.FieldSetVersions,
		SupportedFields:  s.cfg.SupportedFields,
		MatchModes:       modes,
		RateLimits: &srirachav1.RateLimits{
			QueriesPerMinute:  s.cfg.RateQueriesPerMinute,
			BulkRecordsPerDay: s.cfg.RateBulkRecordsPerDay,
		},
	}, nil
}

// Query implements SrirachaService.Query.
func (s *server) Query(ctx context.Context, req *srirachav1.QueryRequest) (*srirachav1.QueryResponse, error) {
	peerKey, peerID, err := s.peerIdentity(ctx)
	if err != nil {
		return nil, err
	}

	if req.Policy == nil {
		return nil, status.Error(codes.PermissionDenied, "consent policy is required")
	}

	if err := s.consent.Validate(req.Policy, peerKey, peerID); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// TODO: enforce s.cfg.RateQueriesPerMinute

	if _, err := fieldset.NegotiateVersion(s.cfg.FieldSetVersions, req.FieldsetVersion); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	tr, err := ProtoToTokenRecord(req.TokenRecord)
	if err != nil {
		return nil, toGRPCStatus(err)
	}

	held, notHeld := partitionFields(req.RequestedFields, s.cfg.SupportedFields)

	cfg := protoToMatchConfig(req.MatchConfig)
	candidates, err := s.indexer.Match(ctx, tr, cfg)
	if err != nil {
		return nil, toGRPCStatus(err)
	}

	matchStatus := candidatesToStatus(candidates)

	resp := &srirachav1.QueryResponse{
		SessionId: req.SessionId,
		NotHeld:   notHeld,
		Status:    matchStatus,
	}

	if len(candidates) == 0 || matchStatus == srirachav1.MatchStatus_MULTIPLE_CANDIDATES {
		if len(candidates) > 0 {
			resp.Confidence = float32(candidates[0].Confidence)
		}
		s.emitAudit(ctx, "query", req.SessionId, req.Policy.PolicyId, matchStatus)
		return resp, nil
	}

	best := candidates[0]
	resp.Confidence = float32(best.Confidence)

	record, err := s.source.Fetch(ctx, best.RecordID)
	if err != nil {
		return nil, toGRPCStatus(err)
	}

	resp.Fields, resp.NotFound = buildFieldValues(record, held)

	s.emitAudit(ctx, "query", req.SessionId, req.Policy.PolicyId, matchStatus)
	return resp, nil
}

// BulkLink implements SrirachaService.BulkLink.
func (s *server) BulkLink(stream srirachav1.SrirachaService_BulkLinkServer) error {
	ctx := stream.Context()
	policyValidated := false

	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		if !policyValidated {
			peerKey, peerID, err := s.peerIdentity(ctx)
			if err != nil {
				return err
			}

			if batch.Policy == nil {
				return status.Error(codes.PermissionDenied, "consent policy required on first batch")
			}

			if err := s.consent.Validate(batch.Policy, peerKey, peerID); err != nil {
				return status.Error(codes.PermissionDenied, err.Error())
			}

			policyValidated = true
			// TODO: enforce s.cfg.RateBulkRecordsPerDay per batch
		}

		result, err := s.processBatch(ctx, batch)
		if err != nil {
			return err
		}

		if err := stream.Send(result); err != nil {
			return err
		}
	}
}

func (s *server) processBatch(ctx context.Context, batch *srirachav1.BulkTokenBatch) (*srirachav1.BulkMatchResult, error) {
	entries := make([]*srirachav1.MatchResultEntry, 0, len(batch.TokenRecords))

	for i, trBytes := range batch.TokenRecords {
		ref := ""
		if i < len(batch.RecordRefs) {
			ref = batch.RecordRefs[i]
		}

		entry, err := s.matchOne(ctx, trBytes, ref)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return &srirachav1.BulkMatchResult{
		SessionId: batch.SessionId,
		Entries:   entries,
	}, nil
}

func (s *server) matchOne(ctx context.Context, trBytes []byte, ref string) (*srirachav1.MatchResultEntry, error) {
	tr, err := ProtoToTokenRecord(trBytes)
	if err != nil {
		return &srirachav1.MatchResultEntry{
			RecordRef: ref,
			Status:    srirachav1.MatchStatus_NO_MATCH,
		}, nil
	}

	candidates, err := s.indexer.Match(ctx, tr, sriracha.MatchConfig{})
	if err != nil {
		return nil, toGRPCStatus(err)
	}

	matchStatus := candidatesToStatus(candidates)
	entry := &srirachav1.MatchResultEntry{
		RecordRef: ref,
		Status:    matchStatus,
	}

	if len(candidates) == 0 || matchStatus == srirachav1.MatchStatus_MULTIPLE_CANDIDATES {
		if len(candidates) > 0 {
			entry.Confidence = float32(candidates[0].Confidence)
		}
		return entry, nil
	}

	best := candidates[0]
	entry.Confidence = float32(best.Confidence)

	record, err := s.source.Fetch(ctx, best.RecordID)
	if err != nil {
		return nil, toGRPCStatus(err)
	}

	entry.Fields, entry.NotFound = buildFieldValues(record, s.cfg.SupportedFields)
	return entry, nil
}

// peerIdentity extracts the Ed25519 public key and institution ID from the
// mTLS peer certificate in ctx.
func (s *server) peerIdentity(ctx context.Context) (ed25519.PublicKey, string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, "", status.Error(codes.Unauthenticated, "no peer information in context")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, "", status.Error(codes.Unauthenticated, "peer auth info is not TLS")
	}

	peerCerts := tlsInfo.State.PeerCertificates
	pub, err := tlsconf.PeerPublicKey(peerCerts)
	if err != nil {
		return nil, "", status.Error(codes.Unauthenticated, fmt.Sprintf("peer certificate: %s", err))
	}

	peerID := peerInstitutionID(tlsInfo)
	return pub, peerID, nil
}

// peerInstitutionID extracts the institution identifier from the peer TLS state.
// It prefers the first URI SAN; falls back to the certificate Common Name.
func peerInstitutionID(info credentials.TLSInfo) string {
	if len(info.State.PeerCertificates) == 0 {
		return ""
	}
	cert := info.State.PeerCertificates[0]
	var uri string
	if len(cert.URIs) > 0 {
		uri = cert.URIs[0].String()
	}
	return cmp.Or(uri, cert.Subject.CommonName)
}

func (s *server) emitAudit(ctx context.Context, event, sessionID, policyID string, st srirachav1.MatchStatus) {
	_ = s.audit.Append(ctx, event, map[string]string{
		"session_id": sessionID,
		"policy_id":  policyID,
		"status":     st.String(),
	})
}
