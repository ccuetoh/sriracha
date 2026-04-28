//go:build integration

package integration_test

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

// labeledRecord is one corpus row with its ground-truth cluster identifier.
type labeledRecord struct {
	id        string
	clusterID string
	raw       sriracha.RawRecord
}

// truePair is a single ground-truth match between a record held by party A
// and a record held by party B.
type truePair struct {
	aID string
	bID string
}

// corpus holds the loaded fixture, already split between parties A and B.
// truePairs lists exactly the cross-party (a,b) matches expected from the
// ground-truth cluster IDs.
type corpus struct {
	A         map[string]sriracha.RawRecord
	B         map[string]sriracha.RawRecord
	clusterOf map[string]string
	truePairs []truePair
}

// loadCorpus reads testdata/febrl_small.csv and splits the rows between
// parties A and B by sorted-rec_id index parity (even → A, odd → B). True
// match pairs are derived from cluster_id co-membership across the two
// parties; intra-party duplicates within the same cluster are not paired.
func loadCorpus(t testing.TB) *corpus {
	t.Helper()
	path := filepath.Join("testdata", "febrl_small.csv")
	return loadCorpusFromFile(t, path)
}

func loadCorpusFromFile(t testing.TB, path string) *corpus {
	t.Helper()

	f, err := os.Open(path) //nolint:gosec // path is fixed test fixture
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck // read-only

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	header, err := r.Read()
	require.NoError(t, err, "read CSV header")

	colIdx := map[string]int{}
	for i, name := range header {
		colIdx[name] = i
	}
	for _, required := range []string{
		"rec_id", "cluster_id", "given_name", "surname",
		"date_of_birth", "suburb", "state", "postcode", "soc_sec_id",
	} {
		_, ok := colIdx[required]
		require.True(t, ok, "fixture missing required column %q", required)
	}

	rows, err := r.ReadAll()
	require.NoError(t, err)

	records := make([]labeledRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, labeledRecord{
			id:        row[colIdx["rec_id"]],
			clusterID: row[colIdx["cluster_id"]],
			raw: sriracha.RawRecord{
				sriracha.FieldNameGiven:            row[colIdx["given_name"]],
				sriracha.FieldNameFamily:           row[colIdx["surname"]],
				sriracha.FieldDateBirth:            row[colIdx["date_of_birth"]],
				sriracha.FieldAddressLocality:      row[colIdx["suburb"]],
				sriracha.FieldAddressAdminArea:     row[colIdx["state"]],
				sriracha.FieldAddressPostalCode:    row[colIdx["postcode"]],
				sriracha.FieldIdentifierNationalID: row[colIdx["soc_sec_id"]],
			},
		})
	}

	sort.Slice(records, func(i, j int) bool { return records[i].id < records[j].id })

	c := &corpus{
		A:         make(map[string]sriracha.RawRecord),
		B:         make(map[string]sriracha.RawRecord),
		clusterOf: make(map[string]string, len(records)),
	}
	clusterMembersA := make(map[string][]string)
	clusterMembersB := make(map[string][]string)
	for i, rec := range records {
		c.clusterOf[rec.id] = rec.clusterID
		if i%2 == 0 {
			c.A[rec.id] = rec.raw
			clusterMembersA[rec.clusterID] = append(clusterMembersA[rec.clusterID], rec.id)
		} else {
			c.B[rec.id] = rec.raw
			clusterMembersB[rec.clusterID] = append(clusterMembersB[rec.clusterID], rec.id)
		}
	}

	clusterIDs := make([]string, 0, len(clusterMembersA))
	for cid := range clusterMembersA {
		clusterIDs = append(clusterIDs, cid)
	}
	sort.Strings(clusterIDs)
	for _, cid := range clusterIDs {
		bMembers, ok := clusterMembersB[cid]
		if !ok {
			continue
		}
		aMembers := clusterMembersA[cid]
		sort.Strings(aMembers)
		sort.Strings(bMembers)
		for _, a := range aMembers {
			for _, b := range bMembers {
				c.truePairs = append(c.truePairs, truePair{aID: a, bID: b})
			}
		}
	}

	require.NotEmpty(t, c.A, "fixture produced empty party-A corpus")
	require.NotEmpty(t, c.B, "fixture produced empty party-B corpus")
	require.NotEmpty(t, c.truePairs, "fixture produced no true cross-party pairs")
	return c
}

// pickIdenticalPair returns one (aID, bID) pair from c.truePairs whose raw
// records are byte-identical across all fields. Used by the deterministic
// Query test, which requires exact-match payloads to produce a hit.
// Fails the test if no such pair exists.
func (c *corpus) pickIdenticalPair(t testing.TB) (string, string) {
	t.Helper()
	for _, p := range c.truePairs {
		if rawRecordsEqual(c.A[p.aID], c.B[p.bID]) {
			return p.aID, p.bID
		}
	}
	t.Fatalf("no byte-identical cross-party pair in fixture; have %d candidate pairs", len(c.truePairs))
	return "", ""
}

func rawRecordsEqual(a, b sriracha.RawRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || va != vb {
			return false
		}
	}
	return true
}

// truePairKey canonicalises an (a,b) pair to a string key for fast set
// membership lookups when computing precision/recall.
func truePairKey(aID, bID string) string {
	return aID + "\x00" + bID
}

// truePairSet returns the set of all true (a,b) pair keys.
func (c *corpus) truePairSet() map[string]struct{} {
	out := make(map[string]struct{}, len(c.truePairs))
	for _, p := range c.truePairs {
		out[truePairKey(p.aID, p.bID)] = struct{}{}
	}
	return out
}

// String returns a one-line summary suitable for t.Logf.
func (c *corpus) String() string {
	return fmt.Sprintf("corpus{|A|=%d, |B|=%d, true_pairs=%d}", len(c.A), len(c.B), len(c.truePairs))
}
