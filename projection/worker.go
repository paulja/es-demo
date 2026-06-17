package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/paulja/es-demo/domain"
	"github.com/paulja/es-demo/eventstore"
	"github.com/redis/go-redis/v9"
)

const orderViewTTL = 0 // no expiry

// Worker subscribes to NATS event subjects and projects them into Redis.
// On startup it replays any missed events from Postgres using the global
// sequence pointer stored in Redis.
type Worker struct {
	store *eventstore.Store
	nc    *nats.Conn
	rdb   *redis.Client
}

func NewWorker(store *eventstore.Store, nc *nats.Conn, rdb *redis.Client) *Worker {
	return &Worker{store: store, nc: nc, rdb: rdb}
}

// Run starts the projection worker. It first catches up from Postgres, then
// subscribes to NATS for live events.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.catchUp(ctx); err != nil {
		return fmt.Errorf("catch-up replay: %w", err)
	}

	subjects := []string{
		domain.EventOrderCreated,
		domain.EventOrderCancelled,
	}
	subs := make([]*nats.Subscription, 0, len(subjects))
	for _, subj := range subjects {
		sub, err := w.nc.Subscribe(subj, w.handleMsg)
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", subj, err)
		}
		subs = append(subs, sub)
	}

	log.Println("projection worker: listening for events")
	<-ctx.Done()

	for _, sub := range subs {
		_ = sub.Unsubscribe()
	}
	return nil
}

// catchUp replays events from Postgres that arrived while the worker was down.
func (w *Worker) catchUp(ctx context.Context) error {
	seq, err := w.lastSeq(ctx)
	if err != nil {
		return err
	}

	events, err := w.store.LoadFrom(ctx, seq)
	if err != nil {
		return err
	}

	for _, e := range events {
		if err := w.project(ctx, e); err != nil {
			log.Printf("projection catch-up: skipping event %s: %v", e.ID, err)
		}
	}
	if len(events) > 0 {
		log.Printf("projection worker: replayed %d missed events", len(events))
	}
	return nil
}

func (w *Worker) handleMsg(msg *nats.Msg) {
	var e domain.Event
	if err := json.Unmarshal(msg.Data, &e); err != nil {
		log.Printf("projection worker: bad message: %v", err)
		return
	}
	if err := w.project(context.Background(), e); err != nil {
		log.Printf("projection worker: project %s: %v", e.Type, err)
	}
}

// project applies a single event to the Redis read model.
func (w *Worker) project(ctx context.Context, e domain.Event) error {
	switch e.Type {
	case domain.EventOrderCreated:
		return w.onOrderCreated(ctx, e)
	case domain.EventOrderCancelled:
		return w.onOrderCancelled(ctx, e)
	}
	return w.advanceSeq(ctx, e.GlobalSeq)
}

func (w *Worker) onOrderCreated(ctx context.Context, e domain.Event) error {
	var p domain.OrderCreated
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return err
	}

	view := domain.OrderView{
		ID:        p.ID,
		UserID:    p.UserID,
		Item:      p.Item,
		Total:     p.Total,
		Status:    "pending",
		CreatedAt: e.OccurredAt,
		UpdatedAt: e.OccurredAt,
	}
	data, err := json.Marshal(view)
	if err != nil {
		return err
	}

	pipe := w.rdb.Pipeline()
	pipe.Set(ctx, orderKey(p.ID), data, orderViewTTL)
	pipe.SAdd(ctx, userOrdersKey(p.UserID), p.ID)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}
	return w.advanceSeq(ctx, e.GlobalSeq)
}

func (w *Worker) onOrderCancelled(ctx context.Context, e domain.Event) error {
	var p domain.OrderCancelled
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return err
	}

	raw, err := w.rdb.Get(ctx, orderKey(p.ID)).Bytes()
	if err != nil {
		return fmt.Errorf("get order view %s: %w", p.ID, err)
	}

	var view domain.OrderView
	if err := json.Unmarshal(raw, &view); err != nil {
		return err
	}

	view.Status = "cancelled"
	view.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(view)
	if err != nil {
		return err
	}

	if err := w.rdb.Set(ctx, orderKey(p.ID), data, orderViewTTL).Err(); err != nil {
		return err
	}
	return w.advanceSeq(ctx, e.GlobalSeq)
}

// advanceSeq persists the last-seen global sequence so catch-up can resume
// from the right position after a restart.
func (w *Worker) advanceSeq(ctx context.Context, seq int64) error {
	if seq == 0 {
		return nil
	}
	return w.rdb.Set(ctx, "projection:last_seq", seq, 0).Err()
}

func (w *Worker) lastSeq(ctx context.Context) (int64, error) {
	val, err := w.rdb.Get(ctx, "projection:last_seq").Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

func orderKey(id string) string      { return "order:" + id }
func userOrdersKey(uid string) string { return "user_orders:" + uid }
