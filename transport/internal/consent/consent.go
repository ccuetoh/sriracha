package consent

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"go.sriracha.dev/transport/internal/replay"
	srirachav1 "go.sriracha.dev/transport/proto/srirachav1"
)

// Validator validates ConsentPolicy messages per spec Section 11.1.
type Validator struct {
	ownInstitutionID string
	cache            *replay.Cache
}

// NewValidator constructs a Validator for an institution that considers itself
// ownInstitutionID and tracks policy IDs with cache for replay prevention.
func NewValidator(ownInstitutionID string, cache *replay.Cache) *Validator {
	return &Validator{ownInstitutionID: ownInstitutionID, cache: cache}
}

// Validate enforces all six rules from spec Section 11.1:
//  1. Ed25519 signature over SHA256(policy_id||issuer_id||target_id||purpose||issued_at_be8||expires_at_be8)
//  2. issuer_id matches the first URI SAN of the peer certificate (peerID)
//  3. target_id matches this institution's own ID
//  4. issued_at <= now
//  5. expires_at > now
//  6. policy_id has not been seen in the replay cache
//
// peerKey is the Ed25519 public key extracted from the peer's mTLS certificate.
// peerID is the institution identifier from the peer's certificate SAN.
func (v *Validator) Validate(p *srirachav1.ConsentPolicy, peerKey ed25519.PublicKey, peerID string) error {
	if p == nil {
		return errors.New("consent: policy is nil")
	}

	now := time.Now()
	issuedAt := time.Unix(p.IssuedAt, 0)
	expiresAt := time.Unix(p.ExpiresAt, 0)

	// Rule 4: issued_at <= now
	if issuedAt.After(now) {
		return fmt.Errorf("consent: policy not yet valid: issued_at %d is in the future", p.IssuedAt)
	}

	// Rule 5: expires_at > now
	if !expiresAt.After(now) {
		return fmt.Errorf("consent: policy expired at %d", p.ExpiresAt)
	}

	// Rule 1: signature verification
	msg := policyMessage(p)
	hash := sha256.Sum256(msg)
	if !ed25519.Verify(peerKey, hash[:], p.Signature) {
		return errors.New("consent: invalid signature")
	}

	// Rule 2: issuer_id matches peer certificate identity
	if p.IssuerId != peerID {
		return fmt.Errorf("consent: issuer_id %q does not match peer identity %q", p.IssuerId, peerID)
	}

	// Rule 3: target_id matches this institution
	if p.TargetId != v.ownInstitutionID {
		return fmt.Errorf("consent: target_id %q does not match own institution %q", p.TargetId, v.ownInstitutionID)
	}

	// Rule 6: replay prevention
	if !v.cache.Claim(p.PolicyId, expiresAt) {
		return fmt.Errorf("consent: policy_id %q already used (replay detected)", p.PolicyId)
	}

	return nil
}

// policyMessage builds the canonical byte sequence to sign/verify:
// policy_id || issuer_id || target_id || purpose || issued_at (big-endian int64) || expires_at (big-endian int64)
func policyMessage(p *srirachav1.ConsentPolicy) []byte {
	var buf []byte
	buf = append(buf, []byte(p.PolicyId)...)
	buf = append(buf, []byte(p.IssuerId)...)
	buf = append(buf, []byte(p.TargetId)...)
	buf = append(buf, []byte(p.Purpose)...)

	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(p.IssuedAt))
	buf = append(buf, ts[:]...)
	binary.BigEndian.PutUint64(ts[:], uint64(p.ExpiresAt))
	buf = append(buf, ts[:]...)

	return buf
}
