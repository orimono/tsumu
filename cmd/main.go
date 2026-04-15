package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/orimono/tsumu/internal/config"
	"github.com/orimono/tsumu/internal/consumer"
	"github.com/orimono/tsumu/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.MustLoad()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	st := store.New(pool)

	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		slog.Error("failed to connect to nats", "err", err)
		os.Exit(1)
	}
	defer nc.Drain()

	nc.Subscribe("orimono.tsumu.history", func(msg *nats.Msg) {
		var req struct {
			NodeID    string `json:"node_id"`
			Type      string `json:"type"`
			FromTs    int64  `json:"from_ts"`   // Unix seconds
			ToTs      int64  `json:"to_ts"`     // Unix seconds
			MaxPoints int    `json:"max_points"` // default 300, max 1000
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			msg.Respond([]byte(`{"error":"invalid request"}`))
			return
		}
		if req.NodeID == "" || req.Type == "" {
			msg.Respond([]byte(`{"error":"node_id and type required"}`))
			return
		}
		if req.FromTs <= 0 || req.ToTs <= 0 || req.ToTs <= req.FromTs {
			msg.Respond([]byte(`{"error":"valid from_ts and to_ts required"}`))
			return
		}
		if req.MaxPoints <= 0 || req.MaxPoints > 1000 {
			req.MaxPoints = 300
		}

		// Convert Unix seconds → nanoseconds (internal ts unit).
		fromNs := req.FromTs * int64(time.Second)
		toNs := req.ToTs * int64(time.Second)

		items, err := st.Query(ctx, req.NodeID, req.Type, fromNs, toNs, req.MaxPoints)
		if err != nil {
			slog.Error("history query failed", "node_id", req.NodeID, "type", req.Type, "err", err)
			msg.Respond([]byte(`{"error":"query failed"}`))
			return
		}

		resp, _ := json.Marshal(map[string]any{
			"items":      items,
			"from_ts":    req.FromTs,
			"to_ts":      req.ToTs,
			"max_points": req.MaxPoints,
		})
		msg.Respond(resp)
	})

	cons, err := consumer.New(nc, st, consumer.Config{
		StreamName: cfg.StreamName,
		Subject:    cfg.Subject,
		Consumer:   cfg.Consumer,
	})
	if err != nil {
		slog.Error("failed to create consumer", "err", err)
		os.Exit(1)
	}

	if err := cons.EnsureStream(ctx); err != nil {
		slog.Error("failed to ensure stream", "err", err)
		os.Exit(1)
	}

	slog.Info("tsumu started", "nats", cfg.NatsURL, "stream", cfg.StreamName)

	if err := cons.Run(ctx); err != nil {
		slog.Error("consumer error", "err", err)
		os.Exit(1)
	}
}
