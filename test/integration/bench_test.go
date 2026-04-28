//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/token"
	"go.sriracha.dev/transport/client"
	srirachav1 "go.sriracha.dev/transport/proto/sriracha/v1"
	"go.sriracha.dev/transport/server"
)

// BenchmarkIntegration_Handshake measures the cost of a full client.New
// (TCP dial + TLS 1.3 mTLS handshake + GetCapabilities round-trip) against
// a freshly-prepared two-party setup. Each iteration opens a new client and
// closes it; the servers are reused across iterations.
func BenchmarkIntegration_Handshake(b *testing.B) {
	c := loadCorpus(b)
	h := newTwoPartyHarness(b, c.A, c.B)

	dialCfg := h.A.leaf.client.Clone()
	dialCfg.ServerName = "127.0.0.1"
	cfg := client.Config{ServerAddr: h.B.addr(), TLSConfig: dialCfg}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cl, err := client.New(context.Background(), cfg)
		if err != nil {
			b.Fatalf("client.New: %v", err)
		}
		_ = cl.Close()
	}
}

// BenchmarkIntegration_QueryDeterministic measures one Query RPC end-to-end:
// HMAC tokenisation of a known matched record, fresh ConsentPolicy signing,
// network round-trip, server-side index lookup, and field-value response.
// A fresh policy_id per iteration avoids replay-cache rejection.
func BenchmarkIntegration_QueryDeterministic(b *testing.B) {
	c := loadCorpus(b)
	aID, _ := c.pickIdenticalPair(b)

	h := newTwoPartyHarness(b, c.A, c.B)
	cl := h.dial(b, h.A, h.B)

	tokenizer, err := token.New([]byte(integrationSecret))
	require.NoError(b, err)
	defer tokenizer.Destroy()

	tr, err := tokenizer.TokenizeRecord(c.A[aID], fieldset.DefaultFieldSet())
	require.NoError(b, err)

	requestedFields := []string{
		sriracha.FieldNameGiven.String(),
		sriracha.FieldNameFamily.String(),
		sriracha.FieldDateBirth.String(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		policy := h.newPolicy(h.A, h.B, fmt.Sprintf("pol-bench-q-%d", i), "bench")
		req, err := client.NewQueryRequest(tr, integrationFieldSetVersion, requestedFields, policy, nil)
		if err != nil {
			b.Fatalf("NewQueryRequest: %v", err)
		}
		if _, err := cl.Query(context.Background(), req); err != nil {
			b.Fatalf("Query iteration %d: %v", i, err)
		}
	}
}

// BenchmarkIntegration_BulkLinkBloom measures the throughput of streaming
// the entire party-A corpus to party B as a single Bloom-mode BulkLink
// batch. Bytes-per-op is set to the full transmitted token-record payload
// size so Bencher reports a stable throughput metric.
func BenchmarkIntegration_BulkLinkBloom(b *testing.B) {
	c := loadCorpus(b)
	h := newTwoPartyHarness(b, c.A, c.B)
	cl := h.dial(b, h.A, h.B)

	tokenizer, err := token.New([]byte(integrationSecret))
	require.NoError(b, err)
	defer tokenizer.Destroy()

	fs := fieldset.DefaultFieldSet()
	aIDs := sortedKeys(c.A)
	tokenBytes := make([][]byte, 0, len(aIDs))
	var batchBytes int64
	for _, id := range aIDs {
		tr, err := tokenizer.TokenizeRecordBloom(c.A[id], fs)
		require.NoError(b, err)
		raw, err := server.TokenRecordToProto(tr)
		require.NoError(b, err)
		tokenBytes = append(tokenBytes, raw)
		batchBytes += int64(len(raw))
	}

	b.SetBytes(batchBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := cl.BulkLink(context.Background())
		if err != nil {
			b.Fatalf("BulkLink open: %v", err)
		}
		policy := h.newPolicy(h.A, h.B, fmt.Sprintf("pol-bench-bulk-%d", i), "bench")
		if err := stream.Send(&srirachav1.BulkLinkRequest{
			SessionId:    fmt.Sprintf("bulk-bench-%d", i),
			TokenRecords: tokenBytes,
			RecordRefs:   aIDs,
			Policy:       policy,
		}); err != nil {
			b.Fatalf("BulkLink send: %v", err)
		}
		if err := stream.CloseSend(); err != nil {
			b.Fatalf("BulkLink CloseSend: %v", err)
		}
		for {
			_, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatalf("BulkLink recv: %v", err)
			}
		}
	}
}
