package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/paulja/es-demo/domain"
)

// ErrVersionConflict is returned when two concurrent commands attempt to write
// at the same aggregate version (optimistic concurrency violation).
var ErrVersionConflict = errors.New("version conflict: aggregate was modified concurrently")

// Store is the Postgres-backed append-only event store.
type Store struct {
	db *sql.DB
}

// New opens a Postgres connection and ensures the schema exists.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	s := &Store{db: db}
	return s, s.migrate()
}

// migrate creates the events and snapshots tables if they do not exist.
func (s *Store) migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			global_seq   BIGSERIAL PRIMARY KEY,
			id           TEXT        NOT NULL UNIQUE,
			aggregate_id TEXT        NOT NULL,
			type         TEXT        NOT NULL,
			version      INT         NOT NULL,
			payload      JSONB       NOT NULL,
			occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			meta         JSONB       NOT NULL DEFAULT '{}',
			UNIQUE (aggregate_id, version)
		);
		CREATE INDEX IF NOT EXISTS events_aggregate_id_idx ON events (aggregate_id);

		CREATE TABLE IF NOT EXISTS snapshots (
			aggregate_id TEXT        PRIMARY KEY,
			version      INT         NOT NULL,
			state        JSONB       NOT NULL,
			taken_at     TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	return err
}

// Load returns all events for the given aggregate ID in version order,
// starting after afterVersion (pass 0 to load from the beginning).
func (s *Store) Load(ctx context.Context, aggregateID string, afterVersion int) ([]domain.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT global_seq, id, aggregate_id, type, version, payload, occurred_at, meta
		FROM events
		WHERE aggregate_id = $1 AND version > $2
		ORDER BY version ASC
	`, aggregateID, afterVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		var e domain.Event
		var metaJSON []byte
		if err := rows.Scan(
			&e.GlobalSeq, &e.ID, &e.AggregateID, &e.Type,
			&e.Version, &e.Payload, &e.OccurredAt, &metaJSON,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(metaJSON, &e.Meta); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// Append writes new events to the store. The expectedVersion must equal the
// aggregate's current highest version; if another writer has already written
// at that version, ErrVersionConflict is returned.
func (s *Store) Append(ctx context.Context, aggregateID string, events []domain.Event, expectedVersion int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Optimistic concurrency check
	var current int
	err = tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE aggregate_id = $1`,
		aggregateID,
	).Scan(&current)
	if err != nil {
		return err
	}
	if current != expectedVersion {
		return fmt.Errorf("%w: expected %d got %d", ErrVersionConflict, expectedVersion, current)
	}

	for _, e := range events {
		metaJSON, err := json.Marshal(e.Meta)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO events (id, aggregate_id, type, version, payload, occurred_at, meta)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, e.ID, aggregateID, e.Type, e.Version, e.Payload,
			e.OccurredAt, metaJSON)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// LoadFrom returns all events with global_seq > afterSeq, used by the
// projection worker to catch up after a restart.
func (s *Store) LoadFrom(ctx context.Context, afterSeq int64) ([]domain.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT global_seq, id, aggregate_id, type, version, payload, occurred_at, meta
		FROM events
		WHERE global_seq > $1
		ORDER BY global_seq ASC
	`, afterSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		var e domain.Event
		var metaJSON []byte
		if err := rows.Scan(
			&e.GlobalSeq, &e.ID, &e.AggregateID, &e.Type,
			&e.Version, &e.Payload, &e.OccurredAt, &metaJSON,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(metaJSON, &e.Meta); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// Snapshot helpers --------------------------------------------------------

type Snapshot struct {
	AggregateID string
	Version     int
	State       []byte
	TakenAt     time.Time
}

// SaveSnapshot upserts a snapshot for the given aggregate.
func (s *Store) SaveSnapshot(ctx context.Context, snap Snapshot) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO snapshots (aggregate_id, version, state, taken_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (aggregate_id) DO UPDATE
			SET version = EXCLUDED.version,
			    state   = EXCLUDED.state,
			    taken_at = now()
	`, snap.AggregateID, snap.Version, snap.State)
	return err
}

// LoadSnapshot returns the latest snapshot for an aggregate, if one exists.
func (s *Store) LoadSnapshot(ctx context.Context, aggregateID string) (*Snapshot, error) {
	var snap Snapshot
	err := s.db.QueryRowContext(ctx, `
		SELECT aggregate_id, version, state, taken_at
		FROM snapshots WHERE aggregate_id = $1
	`, aggregateID).Scan(&snap.AggregateID, &snap.Version, &snap.State, &snap.TakenAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &snap, nil
}
