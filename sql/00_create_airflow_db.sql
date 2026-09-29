-- Runs first (alphabetical order in docker-entrypoint-initdb.d).
-- The main POSTGRES_DB (fleetiq) is created by the postgres image itself;
-- this adds a second database to host Airflow's own metadata so we only
-- need a single Postgres service in docker-compose.
CREATE DATABASE airflow;
