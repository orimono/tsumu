package store

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/orimono/ito"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
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
