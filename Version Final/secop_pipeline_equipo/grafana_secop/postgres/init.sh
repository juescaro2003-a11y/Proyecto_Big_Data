#!/bin/bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set=reader_password="$GRAFANA_DB_PASSWORD" <<'SQL'
CREATE ROLE grafana_lectura LOGIN PASSWORD :'reader_password';
ALTER ROLE grafana_lectura SET statement_timeout = '300s';
ALTER ROLE grafana_lectura SET default_transaction_read_only = on;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
