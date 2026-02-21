package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/dreamoutbox/outbox_pattern_2026/models"
	"github.com/dreamoutbox/outbox_pattern_2026/repository"
	"github.com/dreamoutbox/outbox_pattern_2026/shared"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type Broker struct{}

func (b *Broker) Publish(ctx context.Context, topic string, key uuid.UUID, payload []byte) error {
	// stub: replace with Kafka/Rabbit publish logic
	log.Printf("[broker] publish to %s key=%s payload=%s", topic, key.String(), string(payload))
	return nil
}

func main() {
	db, cleanup := shared.GetDB()
	defer cleanup()

	// HTTP handler that creates an order and an outbox event in same transaction
	http.HandleFunc("/create-order", func(w http.ResponseWriter, r *http.Request) {
		userID := uuid.New()
		order := models.Order{
			UserID: userID,
			Amount: 42.5,
			Status: "created",
		}
		eventPayload := map[string]interface{}{
			"order_id":   nil, // will be set after creation
			"user_id":    userID,
			"amount":     order.Amount,
			"created_at": time.Now().UTC(),
		}

		id, err := repository.CreateOrderWithOutbox(r.Context(), db, order, "OrderCreated", eventPayload)
		if err != nil {
			http.Error(w, fmt.Sprintf("create order: %v", err), http.StatusInternalServerError)
			return
		}

		fmt.Printf("Created order with outbox event: order_id=%s outbox_id=%s\n", order.ID, id)

		// respond with the outbox id for debugging
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"outbox_id": id.String()})
	})

	log.Println("listening :3000")
	log.Fatal(http.ListenAndServe(":3000", nil))
}
