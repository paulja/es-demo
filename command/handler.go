package command

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/paulja/es-demo/aggregate"
	"github.com/paulja/es-demo/domain"
	"github.com/paulja/es-demo/eventstore"
)

// Handler is the write-side command handler.
// It loads an aggregate by replaying events, executes the command, then
// appends new events to the store and publishes them on NATS.
type Handler struct {
	store *eventstore.Store
	nc    *nats.Conn
}

func NewHandler(store *eventstore.Store, nc *nats.Conn) *Handler {
	return &Handler{store: store, nc: nc}
}

// CreateOrder handles a CreateOrder command.
func (h *Handler) CreateOrder(ctx context.Context, cmd domain.CreateOrder) (string, error) {
	id := uuid.New().String()

	// Load aggregate — for a new order there are no prior events, so this
	// returns an empty aggregate at version 0.
	order, err := h.loadOrder(ctx, id)
	if err != nil {
		return "", fmt.Errorf("load order: %w", err)
	}

	// Execute command — raises events onto the aggregate's change list.
	if err := order.Create(id, cmd); err != nil {
		return "", fmt.Errorf("create order: %w", err)
	}

	if err := h.saveAndPublish(ctx, id, order, 0); err != nil {
		return "", err
	}

	return id, nil
}

// CancelOrder handles a CancelOrder command.
func (h *Handler) CancelOrder(ctx context.Context, cmd domain.CancelOrder) error {
	order, err := h.loadOrder(ctx, cmd.ID)
	if err != nil {
		return fmt.Errorf("load order: %w", err)
	}

	// Remember the version before the command so we can pass it as
	// expectedVersion to the optimistic concurrency check.
	expectedVersion := order.Version

	if err := order.Cancel(cmd); err != nil {
		return fmt.Errorf("cancel order: %w", err)
	}

	return h.saveAndPublish(ctx, cmd.ID, order, expectedVersion)
}

// loadOrder replays the event stream for the given aggregate ID.
func (h *Handler) loadOrder(ctx context.Context, id string) (*aggregate.Order, error) {
	events, err := h.store.Load(ctx, id, 0)
	if err != nil {
		return nil, err
	}

	order := &aggregate.Order{}
	for _, e := range events {
		if err := order.Apply(e); err != nil {
			return nil, fmt.Errorf("apply event %s: %w", e.Type, err)
		}
	}
	return order, nil
}

// saveAndPublish coordinates persisting and broadcasting uncommitted events.
// Save is fatal — if we can't write to the store the command has failed.
// Publish is best-effort — the projection worker catches up from Postgres on
// restart so a missed NATS message is not a data loss event.
func (h *Handler) saveAndPublish(ctx context.Context, aggregateID string, order *aggregate.Order, expectedVersion int) error {
	stamped, err := h.save(ctx, aggregateID, order.Changes(), expectedVersion)
	if err != nil {
		return err
	}
	h.publish(stamped)
	return nil
}

// save stamps events with IDs and timestamps then appends them to the event
// store. Returns the stamped events so publish receives their final form.
func (h *Handler) save(ctx context.Context, aggregateID string, changes []domain.Event, expectedVersion int) ([]domain.Event, error) {
	if len(changes) == 0 {
		return nil, nil
	}

	now := time.Now().UTC()
	for i := range changes {
		changes[i].ID = uuid.New().String()
		changes[i].AggregateID = aggregateID
		changes[i].OccurredAt = now
	}

	if err := h.store.Append(ctx, aggregateID, changes, expectedVersion); err != nil {
		return nil, fmt.Errorf("append events: %w", err)
	}

	return changes, nil
}

// publish broadcasts each event on NATS. Failures are silently swallowed —
// NATS delivery is an optimisation, not the source of truth.
func (h *Handler) publish(events []domain.Event) {
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			continue
		}
		_ = h.nc.Publish(e.Type, data)
	}
}
