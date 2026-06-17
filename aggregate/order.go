package aggregate

import (
	"encoding/json"
	"errors"

	"github.com/paulja/es-demo/domain"
)

// Order is the write-side aggregate. It holds no mutable state row in the
// database — current state is always derived by replaying its event stream.
type Order struct {
	ID      string
	UserID  string
	Item    string
	Total   float64
	Status  string
	Version int

	// uncommitted events raised during this command
	changes []domain.Event
}

// Changes returns events raised during this command that must be appended to
// the event store.
func (o *Order) Changes() []domain.Event {
	return o.changes
}

// Apply mutates aggregate state from a single event. It is called both when
// replaying history AND when raising new events, so state transitions live in
// exactly one place.
func (o *Order) Apply(e domain.Event) error {
	switch e.Type {
	case domain.EventOrderCreated:
		var p domain.OrderCreated
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return err
		}
		o.ID = p.ID
		o.UserID = p.UserID
		o.Item = p.Item
		o.Total = p.Total
		o.Status = "pending"

	case domain.EventOrderCancelled:
		o.Status = "cancelled"

	default:
		// unknown events are ignored — forward compatibility
	}

	o.Version = e.Version
	return nil
}

// ---------------------------------------------------------------------------
// Command methods — validate intent then raise events
// ---------------------------------------------------------------------------

// Create handles a CreateOrder command. The aggregate must be empty (version 0).
func (o *Order) Create(id string, cmd domain.CreateOrder) error {
	if o.Version != 0 {
		return errors.New("order already exists")
	}
	if cmd.UserID == "" {
		return errors.New("user_id is required")
	}
	if cmd.Item == "" {
		return errors.New("item is required")
	}
	if cmd.Total <= 0 {
		return errors.New("total must be greater than zero")
	}

	payload, err := json.Marshal(domain.OrderCreated{
		ID:     id,
		UserID: cmd.UserID,
		Item:   cmd.Item,
		Total:  cmd.Total,
	})
	if err != nil {
		return err
	}

	return o.raise(domain.Event{
		Type:    domain.EventOrderCreated,
		Payload: payload,
	})
}

// Cancel handles a CancelOrder command. The order must exist and not already
// be cancelled.
func (o *Order) Cancel(cmd domain.CancelOrder) error {
	if o.Version == 0 {
		return errors.New("order not found")
	}
	if o.Status == "cancelled" {
		return errors.New("order is already cancelled")
	}

	payload, err := json.Marshal(domain.OrderCancelled{
		ID:     o.ID,
		Reason: cmd.Reason,
	})
	if err != nil {
		return err
	}

	return o.raise(domain.Event{
		Type:    domain.EventOrderCancelled,
		Payload: payload,
	})
}

// raise applies an event to state and records it as an uncommitted change.
func (o *Order) raise(e domain.Event) error {
	e.Version = o.Version + 1
	if err := o.Apply(e); err != nil {
		return err
	}
	o.changes = append(o.changes, e)
	return nil
}
