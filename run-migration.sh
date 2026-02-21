#!/bin/bash

# run migrations at migrations/v1.sql
PGPASSWORD=example psql -h localhost -U postgres -d postgres -f migrations/v1.sql
