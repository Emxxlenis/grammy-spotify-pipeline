# Workshop 2 — ETL Pipeline: Spotify × Grammy Awards

End-to-end ETL pipeline built with **Apache Airflow**, **PostgreSQL**, and **Streamlit**. Extracts data from a Spotify CSV and a Grammy Awards database, cleans and merges both datasets, loads the result into a star-schema Data Warehouse, and visualizes insights through an interactive dashboard.

---

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.3 |
| Storage | PostgreSQL 15 |
| Transformation | Python 3.12 + pandas |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker + Docker Compose |
| Cloud storage | Google Drive API (OAuth2) |

---

## Project Structure

```
workshop2/
├── dags/
│   └── workshop2_etl_dag.py      # Airflow DAG — 8 tasks
├── scripts/
│   ├── extract.py                 # Extract from CSV and PostgreSQL
│   ├── clean.py                   # Cleaning and normalization
│   ├── transform.py               # Dataset merge
│   ├── load.py                    # DW load + Google Drive upload
│   ├── create_schema.sql          # Star schema DDL
│   ├── create_grammys_table.sql   # Source table DDL
│   ├── init-db.sh                 # Creates grammys_db and data_warehouse
│   └── generate_token.py          # OAuth2 token generator for Google Drive
├── dashboard/
│   ├── app.py                     # Streamlit dashboard
│   ├── Dockerfile
│   └── requirements.txt
├── data/
│   ├── spotify_dataset.csv        # Spotify source data
│   └── the_grammy_awards.csv      # Grammy Awards source data
├── credentials/                   # Git-ignored — put credentials here
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env
```

---

## Architecture

```
[spotify_dataset.csv] ──────────────────────────────────┐
                                                          ▼
[the_grammy_awards.csv] → [grammys_db] → [Airflow DAG] → [data_warehouse] → [Streamlit :8501]
                                                    │
                                                    └──────────────────────→ [Google Drive]
```

### Databases (all on the same PostgreSQL instance)

| Database | Purpose |
|---|---|
| `airflow` | Airflow internal metadata |
| `grammys_db` | Source OLTP database (simulates a production system) |
| `data_warehouse` | Analytical destination — star schema |

---

## DAG — Task Flow

```
extract_spotify_csv ────────────────────────────────┐
                                                     ├─→ clean_datasets → transform_and_merge ─→ load_csv_output → store_google_drive
load_grammys_source_db → extract_grammys_db ────────┘                                     │
                                                                                            └─→ load_data_warehouse
```

| Task | Description |
|---|---|
| `extract_spotify_csv` | Reads Spotify CSV into staging |
| `load_grammys_source_db` | Loads Grammy CSV into grammys_db |
| `extract_grammys_db` | Extracts Grammy data from PostgreSQL |
| `clean_datasets` | Deduplicates, fills nulls, normalizes artist keys |
| `transform_and_merge` | LEFT JOIN Spotify ← Grammy on `artist_key` |
| `load_csv_output` | Saves merged CSV to `/data/output/` |
| `store_google_drive` | Uploads CSV to Google Drive (optional) |
| `load_data_warehouse` | Loads star schema with TRUNCATE + RELOAD |

---

## Star Schema

```
                  ┌─────────────┐
                  │  dim_time   │
                  │  time_id PK │
                  │  year       │
                  └──────┬──────┘
                         │
┌─────────────┐   ┌──────┴──────────┐   ┌─────────────────┐
│  dim_artist │   │   fact_music    │   │   dim_track      │
│  artist_id  │◄──│  fact_id PK     │──►│  track_id PK     │
│  artist_name│   │  artist_id FK   │   │  track_name      │
└─────────────┘   │  track_id FK    │   │  album_name      │
                  │  time_id FK     │   │  genre           │
                  │  award_id FK    │   │  danceability    │
                  │  popularity     │   │  energy ...      │
                  │  is_winner      │   └─────────────────┘
                  │  is_nominated   │
                  └──────┬──────────┘
                         │
                  ┌──────┴──────┐
                  │  dim_award  │
                  │  award_id PK│
                  │  category   │
                  │  ceremony   │
                  │  nominee    │
                  └─────────────┘
```

**Fact grain:** one row per `(artist × track × award year × award category)`

**Load strategy:** TRUNCATE + RELOAD — the DAG can be re-triggered any number of times without duplicating data.

---

## Setup & Execution

### Prerequisites
- Docker Engine + Docker Compose v2
- Python 3.11+ (only for generating the Google Drive token)

### 1. Clone and configure

```bash
git clone <repo-url>
cd workshop2
mkdir -p credentials logs plugins
```

Create a `.env` file:
```env
AIRFLOW_UID=50000
GOOGLE_CREDENTIALS_PATH=/opt/airflow/credentials/service_account.json
GOOGLE_DRIVE_FOLDER_ID=<your-folder-id>
```

### 2. Start all services

```bash
docker compose up -d
```

Wait ~60 seconds for PostgreSQL and Airflow to become healthy.

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Streamlit Dashboard | http://localhost:8501 | — |

### 3. Run the pipeline

**Via Airflow UI:**
1. Go to http://localhost:8080
2. Toggle ON the `workshop2_etl_pipeline` DAG
3. Click the ▶ (Trigger DAG) button

**Via CLI:**
```bash
docker exec workshop2_airflow_scheduler airflow dags trigger workshop2_etl_pipeline
```

### 4. Verify the Data Warehouse

```bash
docker exec -it workshop2_postgres psql -U airflow -d data_warehouse -c "
  SELECT
    (SELECT COUNT(*) FROM dim_artist) AS artists,
    (SELECT COUNT(*) FROM dim_track)  AS tracks,
    (SELECT COUNT(*) FROM dim_time)   AS years,
    (SELECT COUNT(*) FROM dim_award)  AS awards,
    (SELECT COUNT(*) FROM fact_music) AS facts;
"
```

Expected output: ~17,649 artists | ~89,741 tracks | 62 years | ~1,158 awards | ~101,872 facts

### 5. Open the dashboard

Go to http://localhost:8501 — it loads automatically once the DAG completes.

---

## Google Drive Setup (optional)

The `store_google_drive` task uploads the merged CSV to a Google Drive folder.

### Steps

1. **Create an OAuth 2.0 Client ID** in [Google Cloud Console](https://console.cloud.google.com/):
   - APIs & Services → Credentials → Create Credentials → OAuth 2.0 Client ID
   - Application type: **Desktop app**
   - Download the JSON → save as `credentials/oauth_credentials.json`

2. **Generate the access token** (run once, outside Docker):
   ```bash
   pip install google-auth-oauthlib
   python3 scripts/generate_token.py
   ```
   A browser window opens. Authorize with your Google account. This creates `credentials/token.json`.

3. **Configure `.env`** with your Drive folder ID (from the URL):
   ```env
   GOOGLE_DRIVE_FOLDER_ID=<folder-id-from-url>
   ```

4. Re-trigger the DAG. The task uploads on first run and updates the file on subsequent runs.

> **Note:** Service Accounts cannot upload to personal Google Drive (no storage quota). This project uses OAuth2 user credentials stored in `token.json`, which count against your personal Drive quota.

---

## Key EDA Findings

| Finding | Impact on Pipeline |
|---|---|
| Grammy dataset contains **only winners** (`winner=True` for all rows) | `is_nominated` and `is_winner` are equivalent for Grammy-linked artists |
| Spotify artists use `";"` as collaboration separator | Only the first artist is used for the cross-dataset join |
| 24,259 duplicate `track_id` entries (same track, multiple genres) | Deduplicated by `track_id` before loading `dim_track` |
| Grammy data spans **1958–2019**; Spotify skews more recent | The join only matches artists present in both datasets |
| `artist` field is null in some Grammy rows | Fallback to `nominee` field when `artist` is missing |

---

## Design Decisions

| Decision | Reason |
|---|---|
| Star schema over snowflake | Simpler queries, better analytical performance, industry standard for DWs |
| `TEXT` instead of `VARCHAR(n)` | Prevents `StringDataRightTruncation` on long track or album names |
| LEFT JOIN (Spotify → Grammy) | Preserves all Spotify tracks; Grammy data is added when available |
| `artist_key` normalization | Case-insensitive matching across datasets with inconsistent casing |
| SQLAlchemy removed from `requirements.txt` | Airflow manages its own SQLAlchemy version; a second install causes ORM conflicts |
| OAuth2 over Service Account for Drive | Service Accounts have no personal Drive quota; OAuth2 uses the user's quota |
| TRUNCATE + RELOAD | Idempotent loads — safe to re-run the DAG without duplicating data |
| LocalExecutor over CeleryExecutor | Sufficient for a single-node setup; Celery adds unnecessary complexity |

---

## Stopping the Environment

```bash
docker compose down        # stop containers (data persisted in postgres_data volume)
docker compose down -v     # stop and delete all data
```
