//go:build integration

package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/token"
	"go.sriracha.dev/transport/client"
)

// TestIntegration_AuditChainIntact runs a mixed-activity scenario (one
// successful query + one rejected policy + one successful query) and
// confirms party B's audit log records each event with an intact SHA-256
// hash chain. The harness Close also calls Verify, but having an explicit
// pre-shutdown verification catches regressions where teardown happens to
// mask a chain break.
func TestIntegration_AuditChainIntact(t *testing.T) {
	t.Parallel()

	c := loadCorpus(t)
	h := newTwoPartyHarness(t, c.A, c.B)
	cl := h.dial(t, h.A, h.B)

	tokenizer, err := token.New([]byte(integrationSecret))
	require.NoError(t, err)
	defer tokenizer.Destroy()

	aID, _ := c.pickIdenticalPair(t)
	tr, err := tokenizer.TokenizeRecord(c.A[aID], fieldset.DefaultFieldSet())
	require.NoError(t, err)
	requestedFields := []string{sriracha.FieldNameGiven.String()}

	policyOK1 := h.newPolicy(h.A, h.B, "pol-audit-ok-1", "audit-test")
	req1, err := client.NewQueryRequest(tr, integrationFieldSetVersion, requestedFields, policyOK1, nil)
	require.NoError(t, err)
	_, err = cl.Query(context.Background(), req1)
	require.NoError(t, err)

	policyBad := h.newPolicy(h.A, h.B, "pol-audit-bad", "audit-test")
	policyBad.Signature = []byte("not-a-real-signature")
	reqBad, err := client.NewQueryRequest(tr, integrationFieldSetVersion, requestedFields, policyBad, nil)
	require.NoError(t, err)
	_, err = cl.Query(context.Background(), reqBad)
	require.Error(t, err, "tampered signature must be rejected")

	policyOK2 := h.newPolicy(h.A, h.B, "pol-audit-ok-2", "audit-test")
	req2, err := client.NewQueryRequest(tr, integrationFieldSetVersion, requestedFields, policyOK2, nil)
	require.NoError(t, err)
	_, err = cl.Query(context.Background(), req2)
	require.NoError(t, err)

	require.NoError(t, h.B.audit.Verify(context.Background()),
		"audit chain on responding party must verify after mixed activity")

	events := readAuditEvents(t, h.B.auditPath)
	assert.GreaterOrEqual(t, len(events), 4,
		"expected at least 4 events: handshake + 2 queries + 1 rejection (got %d)", len(events))
	for i, ev := range events {
		assert.NotEmpty(t, ev.EventID, "event %d missing EventID", i)
	}
}

// readAuditEvents tail-reads the JSONL audit log so the test can introspect
// individual events. The log is opened read-only; the writer (party B's
// audit log) keeps appending while we read.
func readAuditEvents(t *testing.T, path string) []sriracha.AuditEvent {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // path is t.TempDir-derived
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck // read-only

	var events []sriracha.AuditEvent
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev sriracha.AuditEvent
		require.NoError(t, json.Unmarshal(line, &ev), "audit line parse")
		events = append(events, ev)
	}
	require.NoError(t, sc.Err())
	return events
}
