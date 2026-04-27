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
	"go.sriracha.dev/transport/internal/ratelimit"
	"go.sriracha.dev/transport/internal/replay"
	"go.sriracha.dev/transport/internal/tlsconf"
	srirachav1 "go.sriracha.dev/transport/proto/sriracha/v1"
)

// DefaultMaxRecvMessageBytes caps the size of an incoming gRPC message when
// Config.MaxRecvMessageBytes is left at zero. 4 MiB matches gRPC's own
// default and bounds the memory a single peer can force the server to
// allocate per BulkLink batch or Query.
const DefaultMaxRecvMessageBytes = 4 * 1024 * 1024

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
	// MaxRecvMessageBytes caps a single incoming gRPC message. Zero falls
	// back to DefaultMaxRecvMessageBytes; pass a positive value to allow
	// larger BulkLink batches on trusted links.
	MaxRecvMessageBytes int
	// MaxBulkRecordsPerStream caps the cumulative TokenRecords accepted on
	// one BulkLink stream. Zero disables the cap.
	MaxBulkRecordsPerStream int
}

// Server is the Sriracha gRPC service (responding party B).
type Server interface {
	// Serve binds lis and blocks until the server stops or an error occurs.
	Serve(lis net.Listener) error
	// GracefulStop waits for in-flight RPCs to complete. Once the supplied
	// context is cancelled or its deadline elapses, it falls back to a hard
	// Stop so a hung BulkLink stream cannot block shutdown forever.
	GracefulStop(ctx context.Context)
}

type server struct {
	srirachav1.UnimplementedSrirachaServiceServer

	cfg     Config
	indexer sriracha.TokenIndexer
	source  sriracha.RecordSource
	tlsCfg  *tls.Config
	cache   replay.Cache
	consent consent.Validator
	audit   sriracha.AuditLog
	limiter ratelimit.Limiter
	grpcSrv *grpc.Server
}

// Option configures the server. Pass options to New.
type Option func(*server)

// WithConfig sets the server Config. Required.
func WithConfig(cfg Config) Option {
	return func(s *server) { s.cfg = cfg }
}

// WithTokenIndexer sets the token index used for matching. Required.
func WithTokenIndexer(idx sriracha.TokenIndexer) Option {
	return func(s *server) { s.indexer = idx }
}

// WithRecordSource sets the raw-record source used to populate field
// values on a successful match. Required.
func WithRecordSource(src sriracha.RecordSource) Option {
	return func(s *server) { s.source = src }
}

// WithTLSConfig sets the mTLS configuration for the gRPC server. Required.
func WithTLSConfig(tlsCfg *tls.Config) Option {
	return func(s *server) { s.tlsCfg = tlsCfg }
}

// WithReplayCache sets the replay-prevention cache used by the consent
// validator. Required.
func WithReplayCache(cache replay.Cache) Option {
	return func(s *server) { s.cache = cache }
}

// WithAuditLog sets the audit log destination. Optional; defaults to
// NopAuditLog.
func WithAuditLog(a sriracha.AuditLog) Option {
	return func(s *server) {
		if a != nil {
			s.audit = a
		}
	}
}

// WithLimiter sets the rate limiter. Optional; defaults to ratelimit.Noop
// (no enforcement).
func WithLimiter(l ratelimit.Limiter) Option {
	return func(s *server) {
		if l != nil {
			s.limiter = l
		}
	}
}

// New constructs a Server. Call Serve to begin accepting connections.
//
// WithConfig, WithTokenIndexer, WithRecordSource, WithTLSConfig, and
// WithReplayCache are required. WithAuditLog defaults to NopAuditLog
// and WithLimiter defaults to ratelimit.Noop when omitted.
func New(opts ...Option) (Server, error) {
	srv := &server{
		audit:   NopAuditLog{},
		limiter: ratelimit.Noop{},
	}
	for _, opt := range opts {
		opt(srv)
	}

	if srv.cfg.InstitutionID == "" {
		return nil, errors.New("server: InstitutionID must not be empty")
	}
	if srv.indexer == nil {
		return nil, errors.New("server: TokenIndexer must not be nil")
	}
	if srv.source == nil {
		return nil, errors.New("server: RecordSource must not be nil")
	}
	if srv.tlsCfg == nil {
		return nil, errors.New("server: TLS config must not be nil")
	}
	if srv.cache == nil {
		return nil, errors.New("server: replay cache must not be nil")
	}

	maxRecv := srv.cfg.MaxRecvMessageBytes
	if maxRecv <= 0 {
		maxRecv = DefaultMaxRecvMessageBytes
	}

	srv.consent = consent.NewValidator(srv.cfg.InstitutionID, srv.cache)
	srv.grpcSrv = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(srv.tlsCfg)),
		grpc.MaxRecvMsgSize(maxRecv),
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

	srirachav1.RegisterSrirachaServiceServer(srv.grpcSrv, srv)
	return srv, nil
}

func (s *server) Serve(lis net.Listener) error {
	return s.grpcSrv.Serve(lis)
}

func (s *server) GracefulStop(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.grpcSrv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.grpcSrv.Stop()
		<-done
	}
}

// GetCapabilities implements SrirachaService.GetCapabilities.
func (s *server) GetCapabilities(ctx context.Context, _ *srirachav1.GetCapabilitiesRequest) (*srirachav1.GetCapabilitiesResponse, error) {
	modes := make([]srirachav1.MatchMode, 0, len(s.cfg.SupportedModes))
	for _, m := range s.cfg.SupportedModes {
		pm, err := MatchModeToProto(m)
		if err != nil {
			continue
		}
		modes = append(modes, pm)
	}

	resp := &srirachav1.GetCapabilitiesResponse{
		SpecVersion:      s.cfg.SpecVersion,
		FieldsetVersions: s.cfg.FieldSetVersions,
		SupportedFields:  s.cfg.SupportedFields,
		MatchModes:       modes,
		RateLimits: &srirachav1.RateLimits{
			QueriesPerMinute:  s.cfg.RateQueriesPerMinute,
			BulkRecordsPerDay: s.cfg.RateBulkRecordsPerDay,
		},
	}

	_, peerID, _ := s.peerIdentity(ctx)
	s.emitAudit(ctx, sriracha.AuditEvent{
		EventType:   sriracha.EventCapabilities,
		InitiatorID: peerID,
		TargetID:    s.cfg.InstitutionID,
	})

	return resp, nil
}

// Query implements SrirachaService.Query.
func (s *server) Query(ctx context.Context, req *srirachav1.QueryRequest) (*srirachav1.QueryResponse, error) {
	peerKey, peerID, err := s.peerIdentity(ctx)
	if err != nil {
		return nil, err
	}

	if req.Policy == nil {
		s.emitAudit(ctx, sriracha.AuditEvent{
			EventType:   sriracha.EventPolicyRejected,
			SessionID:   req.SessionId,
			InitiatorID: peerID,
			TargetID:    s.cfg.InstitutionID,
		})
		return nil, status.Error(codes.PermissionDenied, "consent policy is required")
	}

	if err := s.consent.Validate(req.Policy, peerKey, peerID); err != nil {
		s.emitAudit(ctx, sriracha.AuditEvent{
			EventType:   sriracha.EventPolicyRejected,
			SessionID:   req.SessionId,
			PolicyID:    req.Policy.PolicyId,
			InitiatorID: peerID,
			TargetID:    s.cfg.InstitutionID,
		})
		// The detailed reason is recorded in the audit event above; do not
		// leak issuer/target identities or replay state to the peer.
		return nil, status.Error(codes.PermissionDenied, "consent policy rejected")
	}

	if err := s.limiter.AllowQuery(ctx, peerID); err != nil {
		s.emitAudit(ctx, sriracha.AuditEvent{
			EventType:   sriracha.EventRateLimitHit,
			SessionID:   req.SessionId,
			PolicyID:    req.Policy.PolicyId,
			InitiatorID: peerID,
			TargetID:    s.cfg.InstitutionID,
		})
		return nil, status.Error(codes.ResourceExhausted, "query rate limit exceeded")
	}

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
	matchMode, err := ProtoToMatchMode(req.MatchMode)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unknown match mode")
	}

	resp := &srirachav1.QueryResponse{
		SessionId: req.SessionId,
		NotHeld:   notHeld,
		Status:    matchStatus,
	}

	if len(candidates) == 0 || matchStatus == srirachav1.MatchStatus_MATCH_STATUS_MULTIPLE_CANDIDATES {
		if len(candidates) > 0 {
			resp.Confidence = float32(candidates[0].Confidence)
		}
		s.emitAudit(ctx, sriracha.AuditEvent{
			EventType:       sriracha.EventQuery,
			SessionID:       req.SessionId,
			PolicyID:        req.Policy.PolicyId,
			InitiatorID:     peerID,
			TargetID:        s.cfg.InstitutionID,
			FieldSetVersion: req.FieldsetVersion,
			MatchMode:       matchMode,
			MatchStatus:     protoToMatchStatus(matchStatus),
			RecordCount:     1,
		})
		return resp, nil
	}

	best := candidates[0]
	resp.Confidence = float32(best.Confidence)

	record, err := s.source.Fetch(ctx, best.RecordID)
	if err != nil {
		return nil, toGRPCStatus(err)
	}

	resp.Fields, resp.NotFound = buildFieldValues(record, held)

	s.emitAudit(ctx, sriracha.AuditEvent{
		EventType:       sriracha.EventQuery,
		SessionID:       req.SessionId,
		PolicyID:        req.Policy.PolicyId,
		InitiatorID:     peerID,
		TargetID:        s.cfg.InstitutionID,
		FieldSetVersion: req.FieldsetVersion,
		MatchMode:       matchMode,
		MatchStatus:     protoToMatchStatus(matchStatus),
		RecordCount:     1,
	})
	return resp, nil
}

// BulkLink implements SrirachaService.BulkLink.
func (s *server) BulkLink(stream srirachav1.SrirachaService_BulkLinkServer) error {
	ctx := stream.Context()
	var (
		policyValidated bool
		sessionID       string
		policyID        string
		initiatorID     string
		recordCount     int
	)

	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if policyValidated {
				s.emitAudit(ctx, sriracha.AuditEvent{
					EventType:   sriracha.EventBulkClose,
					SessionID:   sessionID,
					PolicyID:    policyID,
					InitiatorID: initiatorID,
					TargetID:    s.cfg.InstitutionID,
					RecordCount: recordCount,
				})
			}
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
				s.emitAudit(ctx, sriracha.AuditEvent{
					EventType:   sriracha.EventPolicyRejected,
					SessionID:   batch.SessionId,
					InitiatorID: peerID,
					TargetID:    s.cfg.InstitutionID,
				})
				return status.Error(codes.PermissionDenied, "consent policy required on first batch")
			}

			if err := s.consent.Validate(batch.Policy, peerKey, peerID); err != nil {
				s.emitAudit(ctx, sriracha.AuditEvent{
					EventType:   sriracha.EventPolicyRejected,
					SessionID:   batch.SessionId,
					PolicyID:    batch.Policy.PolicyId,
					InitiatorID: peerID,
					TargetID:    s.cfg.InstitutionID,
				})
				return status.Error(codes.PermissionDenied, "consent policy rejected")
			}

			policyValidated = true
			sessionID = batch.SessionId
			policyID = batch.Policy.PolicyId
			initiatorID = peerID

			s.emitAudit(ctx, sriracha.AuditEvent{
				EventType:   sriracha.EventBulkOpen,
				SessionID:   sessionID,
				PolicyID:    policyID,
				InitiatorID: initiatorID,
				TargetID:    s.cfg.InstitutionID,
			})
		}

		recordCount += len(batch.TokenRecords)
		if cap := s.cfg.MaxBulkRecordsPerStream; cap > 0 && recordCount > cap {
			return status.Errorf(codes.ResourceExhausted,
				"bulk record cap exceeded: %d > %d", recordCount, cap)
		}

		result, err := s.processBatch(ctx, batch, initiatorID)
		if err != nil {
			return err
		}

		if err := stream.Send(result); err != nil {
			return err
		}
	}
}

func (s *server) processBatch(ctx context.Context, batch *srirachav1.BulkLinkRequest, initiatorID string) (*srirachav1.BulkLinkResponse, error) {
	if err := s.limiter.AllowBulk(ctx, initiatorID, len(batch.TokenRecords)); err != nil {
		return nil, status.Error(codes.ResourceExhausted, "bulk record rate limit exceeded")
	}

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

	return &srirachav1.BulkLinkResponse{
		SessionId: batch.SessionId,
		Entries:   entries,
	}, nil
}

func (s *server) matchOne(ctx context.Context, trBytes []byte, ref string) (*srirachav1.MatchResultEntry, error) {
	tr, err := ProtoToTokenRecord(trBytes)
	if err != nil {
		return &srirachav1.MatchResultEntry{
			RecordRef: ref,
			Status:    srirachav1.MatchStatus_MATCH_STATUS_NO_MATCH,
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

	if len(candidates) == 0 || matchStatus == srirachav1.MatchStatus_MATCH_STATUS_MULTIPLE_CANDIDATES {
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

func (s *server) emitAudit(ctx context.Context, ev sriracha.AuditEvent) {
	ev.Timestamp = time.Now()
	_ = s.audit.Append(ctx, ev)
}
