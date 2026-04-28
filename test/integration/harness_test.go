//go:build integration

package integration_test

import (
	"context"
	"crypto/ed25519"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	auditfile "go.sriracha.dev/audit/file"
	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/indexer"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/transport/client"
	srirachav1 "go.sriracha.dev/transport/proto/sriracha/v1"
	"go.sriracha.dev/transport/server"
)

// harnessReplayCache is a minimal in-test implementation of the replay.Cache
// interface that transport/server.WithReplayCache expects. We avoid importing
// transport/internal/replay (forbidden from outside transport/) by relying on
// Go's structural interface satisfaction: any type with the right Claim
// method satisfies the option's parameter type.
type harnessReplayCache struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newHarnessReplayCache() *harnessReplayCache {
	return &harnessReplayCache{seen: make(map[string]time.Time)}
}

// Claim mirrors transport/internal/replay.MemoryCache.Claim semantics: reject
// expired policies, otherwise return true on first use and false on replay.
func (c *harnessReplayCache) Claim(policyID string, expiresAt time.Time) bool {
	if !expiresAt.After(time.Now()) {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[policyID]; ok {
		return false
	}
	c.seen[policyID] = expiresAt
	return true
}

const (
	integrationSpecVersion     = "0.1.0"
	integrationFieldSetVersion = "0.1"
	integrationSecret          = "sriracha-integration-test-secret-v1"

	institutionA = "org.example.party-a"
	institutionB = "org.example.party-b"
)

// supportedFields advertises the canonical FieldSet's full path list. Keeping
// this in sync with fieldset.DefaultFieldSet ensures the requested-fields
// pathway in transport/server.Query treats them all as held.
var supportedFields = func() []string {
	fs := fieldset.DefaultFieldSet()
	out := make([]string, 0, len(fs.Fields))
	for _, spec := range fs.Fields {
		out = append(out, spec.Path.String())
	}
	return out
}()

// party is one institution: a running gRPC server with its own indexer,
// record source, audit log, and Ed25519 signing key. The leaf field carries
// the TLS materials and the institution-identifying CN.
type party struct {
	institution string
	leaf        *leaf
	listener    net.Listener
	server      server.Server
	idx         *indexer.Indexer
	source      *memRecordSource
	auditPath   string
	audit       sriracha.AuditLog
	signKey     ed25519.PrivateKey
}

// addr returns the dialable host:port of this party's gRPC server.
func (p *party) addr() string { return p.listener.Addr().String() }

// twoPartyHarness wires up institutions A and B with mutually-trusting mTLS,
// independent indexers seeded from the supplied corpora, and standard policy
// signing keys. Close (registered via t.Cleanup) gracefully stops both servers
// and verifies both audit chains.
type twoPartyHarness struct {
	pki *integrationPKI
	A   *party
	B   *party
}

// newTwoPartyHarness builds a fresh two-party setup. corpusA is loaded into
// party A's record source (and indexed); corpusB likewise for party B.
func newTwoPartyHarness(t testing.TB, corpusA, corpusB map[string]sriracha.RawRecord) *twoPartyHarness {
	t.Helper()

	pki := newIntegrationPKI(t)
	h := &twoPartyHarness{
		pki: pki,
		A:   newParty(t, pki, institutionA, corpusA),
		B:   newParty(t, pki, institutionB, corpusB),
	}
	t.Cleanup(func() { h.close(t) })
	return h
}

func newParty(t testing.TB, pki *integrationPKI, institution string, corpus map[string]sriracha.RawRecord) *party {
	t.Helper()

	leaf := pki.mintLeaf(t, institution)

	idx, err := indexer.New(indexer.NewMemoryStorage(), fieldset.DefaultFieldSet(), []byte(integrationSecret))
	require.NoError(t, err)

	source := newMemRecordSource(corpus)
	require.NoError(t, idx.Rebuild(context.Background(), source))

	auditPath := filepath.Join(t.TempDir(), "audit-"+institution+".jsonl")
	audit, err := auditfile.New(auditPath)
	require.NoError(t, err)

	srv, err := server.New(
		server.WithConfig(server.Config{
			InstitutionID:    institution,
			SpecVersion:      integrationSpecVersion,
			SupportedFields:  supportedFields,
			FieldSetVersions: []string{integrationFieldSetVersion},
			SupportedModes:   []sriracha.MatchMode{sriracha.Deterministic, sriracha.Probabilistic},
		}),
		server.WithTokenIndexer(idx),
		server.WithRecordSource(source),
		server.WithTLSConfig(leaf.server),
		server.WithReplayCache(newHarnessReplayCache()),
		server.WithAuditLog(audit),
	)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = srv.Serve(lis) }()

	return &party{
		institution: institution,
		leaf:        leaf,
		listener:    lis,
		server:      srv,
		idx:         idx,
		source:      source,
		auditPath:   auditPath,
		audit:       audit,
		signKey:     leaf.signKey,
	}
}

// close gracefully stops both servers and asserts both audit chains verify.
// Bounded shutdown context guards against a hung BulkLink stream blocking
// teardown beyond the test timeout.
func (h *twoPartyHarness) close(t testing.TB) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.A.server.GracefulStop(ctx)
	h.B.server.GracefulStop(ctx)

	verifyCtx := context.Background()
	require.NoError(t, h.A.audit.Verify(verifyCtx), "party A audit chain must verify")
	require.NoError(t, h.B.audit.Verify(verifyCtx), "party B audit chain must verify")

	require.NoError(t, h.A.idx.Close())
	require.NoError(t, h.B.idx.Close())
}

// dial returns a transport/client.Client whose mTLS identity is `from`'s leaf
// and whose target is `to`'s listening server. The returned client has already
// completed the GetCapabilities handshake.
func (h *twoPartyHarness) dial(t testing.TB, from, to *party) client.Client {
	t.Helper()
	tlsCfg := from.leaf.client.Clone()
	tlsCfg.ServerName = "127.0.0.1"
	c, err := client.New(context.Background(), client.Config{
		ServerAddr: to.addr(),
		TLSConfig:  tlsCfg,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// newPolicy issues a fresh, valid ConsentPolicy signed by `from` and naming
// `to` as the target. policyID must be unique within a single test to avoid
// replay-cache collisions across calls.
func (h *twoPartyHarness) newPolicy(from, to *party, policyID, purpose string) *srirachav1.ConsentPolicy {
	now := time.Now()
	p := &srirachav1.ConsentPolicy{
		PolicyId:  policyID,
		IssuerId:  from.institution,
		TargetId:  to.institution,
		Purpose:   purpose,
		IssuedAt:  now.Add(-time.Minute).Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(),
	}
	signPolicy(from.signKey, p)
	return p
}
