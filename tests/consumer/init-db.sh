#!/bin/sh
set -eu
if [ "$POSTGRES_DB" = hq_db ]; then
  psql -v ON_ERROR_STOP=1 -U postgres -d hq_db -f /migrations/001_hq.sql
  psql -v ON_ERROR_STOP=1 -U postgres -d hq_db -c "ALTER ROLE hq_app PASSWORD 'chungtest'"
else
  psql -v ON_ERROR_STOP=1 -U postgres -d "$POSTGRES_DB" -v dc_id="$DC_ID" -f /migrations/002_dc.sql
  psql -v ON_ERROR_STOP=1 -U postgres -d "$POSTGRES_DB" -c "ALTER ROLE fdw_reader PASSWORD 'chungtest'; ALTER ROLE debezium PASSWORD 'chungtest'"
fi
