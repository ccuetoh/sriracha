//go:build integration

package integration_test

import (
	"context"
	"errors"
	"io"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/token"
	srirachav1 "go.sriracha.dev/transport/proto/sriracha/v1"
	"go.sriracha.dev/transport/server"
)

// TestIntegration_BulkLinkBloom drives every record in party A's corpus
// through a probabilistic BulkLink stream against party B and asserts
// precision/recall against the labelled ground-truth pairs.
//
// Records are tokenised with TokenizeRecordBloom; the server's matcher uses
// the default Bloom Dice-coefficient scoring with the canonical FieldSet
// weights. Quality floors are intentionally below the reproducible peak so
// that incidental tokeniser/matcher tweaks (e.g. weight adjustments) do not
// flake the suite — tighten them once the floor is well-characterised.
func TestIntegration_BulkLinkBloom(t *testing.T) {
	t.Parallel()

	c := loadCorpus(t)
	t.Logf("loaded %s", c)

	h := newTwoPartyHarness(t, c.A, c.B)
	cl := h.dial(t, h.A, h.B)

	tokenizer, err := token.New([]byte(integrationSecret))
	require.NoError(t, err)
	defer tokenizer.Destroy()

	fs := fieldset.DefaultFieldSet()

	aIDs := sortedKeys(c.A)
	tokenBytes := make([][]byte, 0, len(aIDs))
	for _, id := range aIDs {
		tr, err := tokenizer.TokenizeRecordBloom(c.A[id], fs)
		require.NoError(t, err, "tokenize bloom for %s", id)
		raw, err := server.TokenRecordToProto(tr)
		require.NoError(t, err)
		tokenBytes = append(tokenBytes, raw)
	}

	stream, err := cl.BulkLink(context.Background())
	require.NoError(t, err)

	policy := h.newPolicy(h.A, h.B, "pol-bulk-bloom", "integration-test")
	require.NoError(t, stream.Send(&srirachav1.BulkLinkRequest{
		SessionId:    "bulk-bloom-session",
		TokenRecords: tokenBytes,
		RecordRefs:   aIDs,
		Policy:       policy,
	}))
	require.NoError(t, stream.CloseSend())

	var matched []matchedPair
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		for _, entry := range resp.Entries {
			switch entry.Status {
			case srirachav1.MatchStatus_MATCH_STATUS_MATCHED:
				bID := lookupRecordID(c.B, entry.Fields)
				if bID != "" {
					matched = append(matched, matchedPair{aID: entry.RecordRef, bID: bID})
				}
			case srirachav1.MatchStatus_MATCH_STATUS_MULTIPLE_CANDIDATES,
				srirachav1.MatchStatus_MATCH_STATUS_NO_MATCH,
				srirachav1.MatchStatus_MATCH_STATUS_BELOW_THRESHOLD,
				srirachav1.MatchStatus_MATCH_STATUS_UNSPECIFIED:
				// Counted as a non-match for precision/recall purposes.
			}
		}
	}

	assertMatchQuality(t, matched, c.truePairSet(), 0.90, 0.80)
}

func sortedKeys(m map[string]sriracha.RawRecord) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// lookupRecordID resolves the matched B-side record by reconciling the
// returned field values against B's local corpus. We compare on the
// configured quasi-identifier triple (DOB + family name + national ID),
// which is unique within the test fixture; the server's MatchResultEntry
// does not echo the matched RecordID directly.
func lookupRecordID(bCorpus map[string]sriracha.RawRecord, fields []*srirachav1.FieldValue) string {
	got := make(map[string]string, len(fields))
	for _, fv := range fields {
		got[fv.FieldPath] = fv.Value
	}
	wantDOB := got[sriracha.FieldDateBirth.String()]
	wantFamily := got[sriracha.FieldNameFamily.String()]
	wantNID := got[sriracha.FieldIdentifierNationalID.String()]
	for id, rec := range bCorpus {
		if rec[sriracha.FieldDateBirth] == wantDOB &&
			rec[sriracha.FieldNameFamily] == wantFamily &&
			rec[sriracha.FieldIdentifierNationalID] == wantNID {
			return id
		}
	}
	return ""
}
