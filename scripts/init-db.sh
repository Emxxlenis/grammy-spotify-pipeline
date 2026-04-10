#!/bin/bash
# Runs automatically on the FIRST start of the PostgreSQL container.
# Creates the two additional databases needed by the project.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE grammys_db;
    GRANT ALL PRIVILEGES ON DATABASE grammys_db TO airflow;

    CREATE DATABASE data_warehouse;
    GRANT ALL PRIVILEGES ON DATABASE data_warehouse TO airflow;
EOSQL

echo "Databases grammys_db and data_warehouse created successfully."
