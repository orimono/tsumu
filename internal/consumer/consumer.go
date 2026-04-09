package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/orimono/ito"
	"github.com/orimono/tsumu/internal/store"
)

type Config struct {
	StreamName string
	Subject    string
	Consumer   string
}

type Consumer struct {
	js    jetstream.JetStream
	store *store.Store
	cfg   Config
}

func New(nc *nats.Conn, store *store.Store, cfg Config) (*Consumer, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return &Consumer{js: js, store: store, cfg: cfg}, nil
}

// EnsureStream creates the stream if it doesn't exist.
func (c *Consumer) EnsureStream(ctx context.Context) error {
	_, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     c.cfg.StreamName,
		Subjects: []string{c.cfg.Subject},
	})
	return err
}

func (c *Consumer) Run(ctx context.Context) error {
	stream, err := c.js.Stream(ctx, c.cfg.StreamName)
	if err != nil {
		return err
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       c.cfg.Consumer,
		FilterSubject: c.cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var t ito.Telemetry
		if err := json.Unmarshal(msg.Data(), &t); err != nil {
			slog.Warn("failed to unmarshal telemetry", "err", err)
			msg.Nak()
			return
		}

		if err := c.store.Save(ctx, t); err != nil {
			slog.Error("failed to save telemetry", "node_id", t.NodeID, "type", t.Type, "err", err)
			msg.Nak()
			return
		}

		slog.Debug("saved telemetry", "node_id", t.NodeID, "type", t.Type)
		msg.Ack()
	})
	if err != nil {
		return err
	}

	<-ctx.Done()
	cc.Stop()
	return nil
}
