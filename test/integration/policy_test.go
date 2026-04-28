//go:build integration

package integration_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"

	srirachav1 "go.sriracha.dev/transport/proto/sriracha/v1"
)

// signPolicy mirrors the canonicalization in transport/internal/consent.policyMessage
// (domain prefix + length-prefixed string fields + big-endian timestamps) so the
// signature verifies against the validator on the receiving server.
func signPolicy(priv ed25519.PrivateKey, p *srirachav1.ConsentPolicy) {
	msg := canonicalPolicyMessage(p)
	hash := sha256.Sum256(msg)
	p.Signature = ed25519.Sign(priv, hash[:])
}

func canonicalPolicyMessage(p *srirachav1.ConsentPolicy) []byte {
	const domain = "sriracha.consent.v1\x00"
	fields := []string{p.PolicyId, p.IssuerId, p.TargetId, p.Purpose}

	size := len(domain) + 8 + 8
	for _, f := range fields {
		size += 4 + len(f)
	}

	buf := make([]byte, 0, size)
	buf = append(buf, domain...)

	var lp [4]byte
	for _, f := range fields {
		binary.BigEndian.PutUint32(lp[:], uint32(len(f))) //nolint:gosec // policy field length bounded by validation
		buf = append(buf, lp[:]...)
		buf = append(buf, f...)
	}

	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(p.IssuedAt)) //nolint:gosec // bit-pattern serialisation; sign irrelevant
	buf = append(buf, ts[:]...)
	binary.BigEndian.PutUint64(ts[:], uint64(p.ExpiresAt)) //nolint:gosec // bit-pattern serialisation; sign irrelevant
	buf = append(buf, ts[:]...)

	return buf
}
