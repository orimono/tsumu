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

// Query returns telemetry in [fromTs, toTs] (Unix ns), resampling to at most
// maxPoints via last-value bucketing only when the raw count exceeds maxPoints.
func (s *Store) Query(ctx context.Context, nodeID, typ string, fromTs, toTs int64, maxPoints int) ([]ito.Telemetry, error) {
	// Probe: fetch up to maxPoints+1 raw rows. If fewer than maxPoints+1 come
	// back, the data is already sparse enough — return as-is.
	raw, err := s.queryRaw(ctx, nodeID, typ, fromTs, toTs, maxPoints+1)
	if err != nil {
		return nil, err
	}
	if len(raw) <= maxPoints {
		return raw, nil
	}

	// More points than maxPoints exist — resample.
	stepNs := max((toTs-fromTs)/int64(maxPoints), 1)
	return s.queryResampled(ctx, nodeID, typ, fromTs, toTs, stepNs)
}

func (s *Store) queryRaw(ctx context.Context, nodeID, typ string, fromTs, toTs int64, limit int) ([]ito.Telemetry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT node_id, type, ts, payload
		FROM telemetry
		WHERE node_id = $1 AND type = $2
		  AND ts >= $3 AND ts <= $4
		ORDER BY ts ASC
		LIMIT $5
	`, nodeID, typ, fromTs, toTs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTelemetry(rows)
}

func (s *Store) queryResampled(ctx context.Context, nodeID, typ string, fromTs, toTs, stepNs int64) ([]ito.Telemetry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT node_id, type, bucket AS ts, payload
		FROM (
			SELECT DISTINCT ON ((ts / $5) * $5)
				node_id, type,
				(ts / $5) * $5 AS bucket,
				ts, payload
			FROM telemetry
			WHERE node_id = $1 AND type = $2
			  AND ts >= $3 AND ts <= $4
			ORDER BY (ts / $5) * $5, ts DESC
		) sub
		ORDER BY ts ASC
	`, nodeID, typ, fromTs, toTs, stepNs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTelemetry(rows)
}

func scanTelemetry(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]ito.Telemetry, error) {
	result := make([]ito.Telemetry, 0)
	for rows.Next() {
		var (
			t          ito.Telemetry
			payloadRaw []byte
		)
		if err := rows.Scan(&t.NodeID, &t.Type, &t.Timestamp, &payloadRaw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payloadRaw, &t.Payload); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}
