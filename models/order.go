package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Order struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	Amount    float64
	Status    string
	CreatedAt time.Time
}

type OutboxEvent struct {
	ID          uuid.UUID       `json:"id"`
	AggregateID uuid.UUID       `json:"aggregate_id"`
	EventType   string          `json:"event_type"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	Attempts    int             `json:"attempts"`
}
