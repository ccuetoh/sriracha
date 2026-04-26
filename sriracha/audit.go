package sriracha

import "time"

// AuditEventType identifies the kind of protocol event being recorded.
type AuditEventType int

const (
	// EventQuery records a single Query RPC call.
	EventQuery AuditEventType = iota
	// EventBulkOpen records the opening of a BulkLink streaming session.
	EventBulkOpen
	// EventBulkClose records the closing of a BulkLink streaming session.
	EventBulkClose
	// EventCapabilities records a GetCapabilities handshake.
	EventCapabilities
	// EventPolicyRejected records a request rejected due to an invalid or missing consent policy.
	EventPolicyRejected
	// EventRateLimitHit records a request rejected due to rate limiting.
	EventRateLimitHit
)

// MatchStatus describes the outcome of a token matching operation.
type MatchStatus int

const (
	// MatchStatusUnspecified indicates the status was not set.
	MatchStatusUnspecified MatchStatus = iota
	// MatchStatusMatched indicates a single high-confidence match was found.
	MatchStatusMatched
	// MatchStatusNoMatch indicates no candidates met the matching threshold.
	MatchStatusNoMatch
	// MatchStatusBelowThreshold indicates the best candidate fell below the confidence threshold.
	MatchStatusBelowThreshold
	// MatchStatusMultipleCandidates indicates more than one candidate exceeded the threshold.
	MatchStatusMultipleCandidates
)

// AuditEvent is a structured record of a single protocol interaction.
// TokenRecords and FieldValues are never included.
type AuditEvent struct {
	// EventID is a unique identifier for this event, set by the audit log implementation.
	EventID string
	// PreviousHash is the SHA-256 hash of the preceding event's raw JSON, forming a hash chain.
	PreviousHash [32]byte
	// Timestamp is when the event was recorded.
	Timestamp time.Time
	// SessionID is the session identifier from the originating request.
	SessionID string
	// EventType identifies the kind of protocol interaction recorded.
	EventType AuditEventType
	// InitiatorID is the institution ID of the requesting party.
	InitiatorID string
	// TargetID is the institution ID of the responding party.
	TargetID string
	// PolicyID is the consent policy identifier governing the request, if any.
	PolicyID string
	// FieldSetVersion is the field-set schema version used in the request.
	FieldSetVersion string
	// MatchMode is the matching algorithm mode used (deterministic or probabilistic).
	MatchMode MatchMode
	// RecordCount is the number of token records included in the request.
	RecordCount int
	// MatchStatus is the outcome of the matching operation.
	MatchStatus MatchStatus
}
