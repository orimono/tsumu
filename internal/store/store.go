package store

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/orimono/ito"
)

const createTable = `
CREATE TABLE IF NOT EXISTS telemetry (
	id        BIGSERIAL PRIMARY KEY,
	node_id   TEXT        NOT NULL,
	type      TEXT        NOT NULL,
	ts        BIGINT      NOT NULL,
	payload   JSONB       NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_node_type_ts ON telemetry (node_id, type, ts DESC);
`

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, createTable)
	return err
}

func (s *Store) Save(ctx context.Context, t ito.Telemetry) error {
	payload, err := json.Marshal(t.Payload)
	if err != nil {
		return err
	}

	_, err = s.pool.Exec(ctx,
		`INSERT INTO telemetry (node_id, type, ts, payload) VALUES ($1, $2, $3, $4)`,
		t.NodeID, t.Type, t.Timestamp, payload,
	)
	return err
}
