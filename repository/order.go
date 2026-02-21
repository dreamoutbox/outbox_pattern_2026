package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/dreamoutbox/outbox_pattern_2026/models"
	"github.com/google/uuid"
)

// CreateOrderWithOutbox creates an order and writes an outbox event in the same transaction.
func CreateOrderWithOutbox(ctx context.Context, db *sql.DB, order models.Order, eventType string, eventPayload interface{}) (uuid.UUID, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return uuid.Nil, err
	}
	defer func() {
		// ensure rollback if not committed
		_ = tx.Rollback()
	}()

	// insert order
	if order.ID == uuid.Nil {
		order.ID = uuid.New()
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO orders (id, user_id, amount, status, created_at) VALUES ($1,$2,$3,$4,$5)`,
		order.ID, order.UserID, order.Amount, order.Status, time.Now().UTC(),
	)
	if err != nil {
		return uuid.Nil, err
	}

	// prepare outbox payload
	payloadBytes, err := json.Marshal(eventPayload)
	if err != nil {
		return uuid.Nil, err
	}
	outboxID := uuid.New()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (id, aggregate_id, event_type, payload, created_at) VALUES ($1,$2,$3,$4,$5)`,
		outboxID, order.ID, eventType, payloadBytes, time.Now().UTC(),
	)
	if err != nil {
		return uuid.Nil, err
	}

	if err := tx.Commit(); err != nil {
		return uuid.Nil, err
	}
	return outboxID, nil
}

// FetchPendingOutbox selects a batch of pending events and returns them locked for processing.
// It uses SKIP LOCKED so multiple workers can run concurrently.
func FetchPendingOutbox(ctx context.Context, tx *sql.Tx, limit int) ([]models.OutboxEvent, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, aggregate_id, event_type, payload, created_at, attempts
		FROM outbox
		WHERE processed_at IS NULL
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []models.OutboxEvent
	for rows.Next() {
		var e models.OutboxEvent
		var payload []byte
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.EventType, &payload, &e.CreatedAt, &e.Attempts); err != nil {
			return nil, err
		}
		e.Payload = json.RawMessage(payload)
		res = append(res, e)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return res, nil
}

// MarkOutboxProcessed marks event as processed_at = now()
func MarkOutboxProcessed(ctx context.Context, tx *sql.Tx, id uuid.UUID) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE outbox SET processed_at = now() WHERE id = $1`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("no row updated when marking outbox processed")
	}
	return nil
}

// MarkOutboxFailed increments attempts and stores last_error
func MarkOutboxFailed(ctx context.Context, tx *sql.Tx, id uuid.UUID, errStr string) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE outbox SET attempts = attempts + 1, last_error = $1 WHERE id = $2`, errStr, id)
	return err
}
