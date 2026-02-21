#!/bin/bash

# curl to /create-order endpoint to create an order and an outbox event
curl -X POST -H "Content-Type: application/json" \
    -d '{"customer_id": 123, "amount": 49.99}' \
    http://localhost:3000/create-order
