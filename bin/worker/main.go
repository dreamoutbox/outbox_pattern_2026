package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/dreamoutbox/outbox_pattern_2026/repository"
	"github.com/dreamoutbox/outbox_pattern_2026/shared"

	_ "github.com/lib/pq"
)

func main() {
	db, cleanup := shared.GetDB()
	defer cleanup()

	// Worker
	worker := &SimpleOutboxWorker{
		DB: db,
		// Broker:         &Broker{},
		TopicForEvents: map[string]string{"OrderCreated": "orders.created"},
		BatchSize:      10,
		PollInterval:   1 * time.Second,
		MaxAttempts:    5,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup graceful shutdown on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Printf(
		"starting outbox worker with config: batch_size=%d poll_interval=%s max_attempts=%d\n",
		worker.BatchSize, worker.PollInterval, worker.MaxAttempts,
	)

	go worker.Start(ctx)

	<-ctx.Done()
	stop()
}

// SimpleOutboxWorker polls the outbox table and publishes events.
type SimpleOutboxWorker struct {
	DB *sql.DB
	// Broker         Broker
	TopicForEvents map[string]string // mapping event_type -> topic
	BatchSize      int
	PollInterval   time.Duration
	MaxAttempts    int
}

func (w *SimpleOutboxWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := w.processOnce(ctx); err != nil {
				log.Printf("outbox worker process error: %v", err)
			}
		}
	}
}

func (w *SimpleOutboxWorker) processOnce(ctx context.Context) error {
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	events, err := repository.FetchPendingOutbox(ctx, tx, w.BatchSize)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if len(events) == 0 {
		_ = tx.Rollback()
		return nil
	}

	for _, ev := range events {
		fmt.Printf("Start Processing outbox event: id=%s type=%s attempts=%d\n", ev.ID, ev.EventType, ev.Attempts)

		// If we've tried too many times, skip or move to poison queue / monitoring
		if ev.Attempts >= w.MaxAttempts {
			// increment attempts so we don't loop forever; record an error
			_ = repository.MarkOutboxFailed(ctx, tx, ev.ID, "max attempts exceeded")
			continue
		}

		topic := w.TopicForEvents[ev.EventType]
		if topic == "" {
			// unknown event type: mark as failed (or route to dead-letter)
			_ = repository.MarkOutboxFailed(ctx, tx, ev.ID, "unknown event type")
			continue
		}

		// publish outside of tx? We are currently in a transaction with the row locked.
		// We will commit after a successful publish and then mark processed in another tx OR
		// we can publish while holding the lock and mark processed in the same tx -- but
		// publishing is external IO (blocking), so best is:
		// - publish (outside DB tx)
		// - if success, start a new tx to mark processed (optimistic)
		// For simplicity here we publish while still inside the tx but commit after success.
		// This is acceptable but increases lock duration.

		// if err := w.Broker.Publish(ctx, topic, ev.AggregateID, ev.Payload); err != nil {
		// 	// record failure (attempts++) and continue to next event
		// 	_ = repository.MarkOutboxFailed(ctx, tx, ev.ID, err.Error())
		// 	continue
		// }

		// mark processed
		if err := repository.MarkOutboxProcessed(ctx, tx, ev.ID); err != nil {
			_ = repository.MarkOutboxFailed(ctx, tx, ev.ID, err.Error())
			fmt.Printf("Failed to mark outbox event processed: id=%s error=%v\n", ev.ID, err)
			continue
		}

		fmt.Printf("Processed outbox event: id=%s type=%s topic=%s\n", ev.ID, ev.EventType, topic)
	}

	return tx.Commit()
}
