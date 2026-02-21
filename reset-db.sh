#!/bin/bash

# drop all tables in postgres
PGPASSWORD=example psql \
    -h localhost \
    -U postgres \
    -d postgres \
    -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
