package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/paulja/es-demo/command"
	"github.com/paulja/es-demo/domain"
	"github.com/paulja/es-demo/eventstore"
	"github.com/paulja/es-demo/projection"
	"github.com/paulja/es-demo/query"
	"github.com/redis/go-redis/v9"
)

func main() {
	pgDSN := env("POSTGRES_DSN", "postgres://postgres:secret@localhost:5432/events?sslmode=disable")
	natsURL := env("NATS_URL", "nats://localhost:4222")
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	httpAddr := env("HTTP_ADDR", ":8080")

	// ── Infrastructure ────────────────────────────────────────────────────
	store, err := eventstore.New(pgDSN)
	if err != nil {
		log.Fatalf("eventstore: %v", err)
	}
	log.Println("connected to postgres")

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	defer nc.Close()
	log.Println("connected to nats")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()
	log.Println("connected to redis")

	// ── Handlers ──────────────────────────────────────────────────────────
	cmdHandler := command.NewHandler(store, nc)
	qryHandler := query.NewHandler(rdb)

	// ── Projection worker ─────────────────────────────────────────────────
	worker := projection.NewWorker(store, nc, rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("projection worker stopped: %v", err)
		}
	}()

	// ── HTTP routes ───────────────────────────────────────────────────────
	mux := http.NewServeMux()

	// POST /orders — create order
	// DELETE /orders/{id} — cancel order
	// GET /orders/{id} — get single order
	// GET /orders?user_id=… — list orders for user
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var cmd domain.CreateOrder
			if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			id, err := cmdHandler.CreateOrder(r.Context(), cmd)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnprocessableEntity)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]string{"id": id})

		case http.MethodGet:
			userID := r.URL.Query().Get("user_id")
			if userID == "" {
				http.Error(w, "user_id query param required", http.StatusBadRequest)
				return
			}
			orders, err := qryHandler.ListOrders(r.Context(), userID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(orders)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/orders/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/orders/")
		if id == "" {
			http.Error(w, "order id required", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			order, err := qryHandler.GetOrder(r.Context(), id)
			if errors.Is(err, query.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(order)

		case http.MethodDelete:
			var cmd domain.CancelOrder
			cmd.ID = id
			_ = json.NewDecoder(r.Body).Decode(&cmd) // reason is optional
			cmd.ID = id                               // ensure ID from path wins

			if err := cmdHandler.CancelOrder(r.Context(), cmd); err != nil {
				if errors.Is(err, eventstore.ErrVersionConflict) {
					http.Error(w, "conflict: please retry", http.StatusConflict)
					return
				}
				http.Error(w, err.Error(), http.StatusUnprocessableEntity)
				return
			}
			w.WriteHeader(http.StatusAccepted)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// GET /events?aggregate_id=… — inspect raw event stream (demo/debug)
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		aggID := r.URL.Query().Get("aggregate_id")
		if aggID == "" {
			http.Error(w, "aggregate_id query param required", http.StatusBadRequest)
			return
		}
		events, err := store.Load(r.Context(), aggID, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(events)
	})

	log.Printf("listening on %s", httpAddr)
	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
