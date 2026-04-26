package sriracha

import "time"

// AuditEventType identifies the kind of protocol event being recorded.
type AuditEventType int

const (
	EventQuery AuditEventType = iota
	EventBulkOpen
	EventBulkClose
	EventCapabilities
	EventPolicyRejected
	EventRateLimitHit
)

// MatchStatus describes the outcome of a token matching operation.
type MatchStatus int

const (
	MatchStatusUnspecified MatchStatus = iota
	MatchStatusMatched
	MatchStatusNoMatch
	MatchStatusBelowThreshold
	MatchStatusMultipleCandidates
)

// AuditEvent is a structured record of a single protocol interaction.
// TokenRecords and FieldValues are never included.
type AuditEvent struct {
	EventID         string
	PreviousHash    [32]byte
	Timestamp       time.Time
	SessionID       string
	EventType       AuditEventType
	InitiatorID     string
	TargetID        string
	PolicyID        string
	FieldSetVersion string
	MatchMode       MatchMode
	RecordCount     int
	MatchStatus     MatchStatus
}
