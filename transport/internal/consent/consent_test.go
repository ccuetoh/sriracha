package consent

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/transport/internal/replay"
	srirachav1 "go.sriracha.dev/transport/proto/srirachav1"
)

func newCache(t testing.TB) replay.Cache {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return replay.New(ctx)
}

func generateKeyPair(t testing.TB) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

func signPolicy(t testing.TB, p *srirachav1.ConsentPolicy, priv ed25519.PrivateKey) {
	t.Helper()
	msg := policyMessage(p)
	hash := sha256.Sum256(msg)
	p.Signature = ed25519.Sign(priv, hash[:])
}

func validPolicy(issuerID, targetID string) *srirachav1.ConsentPolicy {
	now := time.Now()
	return &srirachav1.ConsentPolicy{
		PolicyId:  "pol-" + issuerID,
		IssuerId:  issuerID,
		TargetId:  targetID,
		Purpose:   "testing",
		IssuedAt:  now.Add(-time.Minute).Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(),
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	const (
		issuerID = "org.example.a"
		targetID = "org.example.b"
	)

	pub, priv := generateKeyPair(t)

	cases := []struct {
		name    string
		setup   func(*srirachav1.ConsentPolicy)
		wantErr string
	}{
		{
			name:  "valid policy",
			setup: func(p *srirachav1.ConsentPolicy) { signPolicy(t, p, priv) },
		},
		{
			name:  "nil policy",
			setup: func(_ *srirachav1.ConsentPolicy) {},
		},
		{
			name: "expired",
			setup: func(p *srirachav1.ConsentPolicy) {
				p.ExpiresAt = time.Now().Add(-time.Second).Unix()
				signPolicy(t, p, priv)
			},
			wantErr: "expired",
		},
		{
			name: "not yet valid",
			setup: func(p *srirachav1.ConsentPolicy) {
				p.IssuedAt = time.Now().Add(time.Hour).Unix()
				signPolicy(t, p, priv)
			},
			wantErr: "future",
		},
		{
			name: "bad signature",
			setup: func(p *srirachav1.ConsentPolicy) {
				_, otherPriv := generateKeyPair(t)
				signPolicy(t, p, otherPriv)
			},
			wantErr: "invalid signature",
		},
		{
			name: "wrong issuer",
			setup: func(p *srirachav1.ConsentPolicy) {
				p.IssuerId = "org.example.other"
				signPolicy(t, p, priv)
			},
			wantErr: "issuer_id",
		},
		{
			name: "wrong target",
			setup: func(p *srirachav1.ConsentPolicy) {
				p.TargetId = "org.example.wrong"
				signPolicy(t, p, priv)
			},
			wantErr: "target_id",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := NewValidator(targetID, newCache(t))

			if tc.name == "nil policy" {
				err := v.Validate(nil, pub, issuerID)
				assert.ErrorContains(t, err, "nil")
				return
			}

			p := validPolicy(issuerID, targetID)
			tc.setup(p)

			err := v.Validate(p, pub, issuerID)

			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateReplay(t *testing.T) {
	t.Parallel()

	const (
		issuerID = "org.example.a"
		targetID = "org.example.b"
	)

	pub, priv := generateKeyPair(t)
	cache := newCache(t)
	v := NewValidator(targetID, cache)

	p := validPolicy(issuerID, targetID)
	signPolicy(t, p, priv)

	require.NoError(t, v.Validate(p, pub, issuerID))

	err := v.Validate(p, pub, issuerID)
	assert.ErrorContains(t, err, "replay")
}

func BenchmarkValidate(b *testing.B) {
	const (
		issuerID = "org.bench.a"
		targetID = "org.bench.b"
	)
	pub, priv := generateKeyPair(b)
	v := NewValidator(targetID, newCache(b))

	b.ResetTimer()
	for i := range b.N {
		p := &srirachav1.ConsentPolicy{
			PolicyId:  "pol-bench-" + strconv.Itoa(i),
			IssuerId:  issuerID,
			TargetId:  targetID,
			Purpose:   "bench",
			IssuedAt:  time.Now().Add(-time.Minute).Unix(),
			ExpiresAt: time.Now().Add(time.Hour).Unix(),
		}
		signPolicy(b, p, priv)
		_ = v.Validate(p, pub, issuerID)
	}
}
