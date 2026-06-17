package domain

import "time"

// ---------------------------------------------------------------------------
// Event envelope — the record written to the event store
// ---------------------------------------------------------------------------

// Event is the envelope stored in Postgres and published on NATS.
// AggregateID + Version together uniquely identify a position in a stream.
// GlobalSeq is a Postgres BIGSERIAL used by the projection worker to process
// events in total order across all aggregates.
type Event struct {
	ID          string    `json:"id"`
	AggregateID string    `json:"aggregate_id"`
	Type        string    `json:"type"`
	Version     int       `json:"version"`
	GlobalSeq   int64     `json:"global_seq"`
	Payload     []byte    `json:"payload"`
	OccurredAt  time.Time `json:"occurred_at"`
	Meta        Metadata  `json:"meta"`
}

// Metadata carries observability context threaded through a command chain.
type Metadata struct {
	CorrelationID string `json:"correlation_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`
	UserID        string `json:"user_id,omitempty"`
}

// ---------------------------------------------------------------------------
// Concrete payload types  (unmarshal Event.Payload based on Event.Type)
// ---------------------------------------------------------------------------

const (
	EventOrderCreated   = "order.created"
	EventOrderCancelled = "order.cancelled"
)

type OrderCreated struct {
	ID     string  `json:"id"`
	UserID string  `json:"user_id"`
	Item   string  `json:"item"`
	Total  float64 `json:"total"`
}

type OrderCancelled struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

type CreateOrder struct {
	UserID string  `json:"user_id"`
	Item   string  `json:"item"`
	Total  float64 `json:"total"`
}

type CancelOrder struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// ---------------------------------------------------------------------------
// Read model (projection stored in Redis)
// ---------------------------------------------------------------------------

type OrderView struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Item      string    `json:"item"`
	Total     float64   `json:"total"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
