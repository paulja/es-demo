package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/paulja/es-demo/domain"
	"github.com/redis/go-redis/v9"
)

var ErrNotFound = errors.New("order not found")

// Handler serves read queries from the Redis projection store.
type Handler struct {
	rdb *redis.Client
}

func NewHandler(rdb *redis.Client) *Handler {
	return &Handler{rdb: rdb}
}

// GetOrder returns a single order view by ID.
func (h *Handler) GetOrder(ctx context.Context, id string) (*domain.OrderView, error) {
	raw, err := h.rdb.Get(ctx, orderKey(id)).Bytes()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}

	var view domain.OrderView
	if err := json.Unmarshal(raw, &view); err != nil {
		return nil, err
	}
	return &view, nil
}

// ListOrders returns all order views for a given user ID.
func (h *Handler) ListOrders(ctx context.Context, userID string) ([]domain.OrderView, error) {
	ids, err := h.rdb.SMembers(ctx, userOrdersKey(userID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis smembers: %w", err)
	}

	views := make([]domain.OrderView, 0, len(ids))
	for _, id := range ids {
		view, err := h.GetOrder(ctx, id)
		if err != nil {
			continue // projection may lag briefly; skip missing views
		}
		views = append(views, *view)
	}
	return views, nil
}

func orderKey(id string) string       { return "order:" + id }
func userOrdersKey(uid string) string { return "user_orders:" + uid }
