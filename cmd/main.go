package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
	if err := st.Migrate(ctx); err != nil {
		slog.Error("failed to run migrations", "err", err)
		os.Exit(1)
	}

	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		slog.Error("failed to connect to nats", "err", err)
		os.Exit(1)
	}
	defer nc.Drain()

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
