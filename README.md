# Spotify × Grammy Analytics Warehouse

An orchestrated pipeline that joins a Spotify catalog with the Grammy Awards, loads a star-schema warehouse, and answers a concrete question: does the award show up in popularity.

**Demo:** [public walkthrough](https://emxxlenis.github.io/grammy-spotify-pipeline/)

The interactive Streamlit dashboard runs locally, on top of PostgreSQL, after the Airflow DAG finishes. The public page publishes the aggregates from the same cleaning and join logic, so it can be opened without starting the stack.

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.3 |
| Warehouse | PostgreSQL 15 |
| Transform | Python 3.12 + pandas |
| Dashboard | Streamlit + Plotly |
| Runtime | Docker Compose |
| Optional export | Google Drive, OAuth2 |

## Architecture

```text
spotify_dataset.csv ─────────────────────────────┐
                                                  ▼
the_grammy_awards.csv → grammys_db → Airflow DAG → data_warehouse → Streamlit :8501
                                              │
                                              └──────────────────→ Google Drive (optional)
```

Three databases share one PostgreSQL instance:

| Database | Role |
|---|---|
| `airflow` | Scheduler metadata |
| `grammys_db` | Source system for the awards |
| `data_warehouse` | Star schema used by the dashboard |

## DAG

`dags/music_analytics_dag.py`, id `music_analytics_pipeline`.

```text
extract_spotify_csv ─────────────────────────┐
                                             ├─→ clean_datasets → transform_and_merge ─→ load_csv_output → store_google_drive
load_grammys_source_db → extract_grammys_db ─┘                                     │
                                                                                     └─→ load_data_warehouse
```

Fact grain: one row per artist, track, award year, and award category. The load is truncate-and-reload, so rerunning the DAG does not duplicate facts.

The join is a left join from Spotify to the awards on a normalized artist key. Collaboration credits use the first artist. Tracks with no award stay in the warehouse.

After a successful load, the warehouse is on the order of 17,600 artists, 89,741 tracks, 62 award years, and about 101,872 fact rows. Awards in the source cover 1958–2019, and every row in that source is a winner.

## Run it

Requirements: Docker Engine and Docker Compose v2.

```bash
git clone https://github.com/Emxxlenis/grammy-spotify-pipeline.git
cd grammy-spotify-pipeline
mkdir -p credentials logs plugins
cp .env.example .env
docker compose up -d
```

Change `POSTGRES_PASSWORD`, `AIRFLOW_FERNET_KEY`, and `AIRFLOW_ADMIN_PASSWORD` before using this anywhere other than a local machine. Drive upload also needs `GOOGLE_DRIVE_FOLDER_ID`.

| Service | URL | Default login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Dashboard | http://localhost:8501 | — |

Turn on `music_analytics_pipeline` in the Airflow UI and trigger it, or:

```bash
docker exec music_dw_airflow_scheduler airflow dags trigger music_analytics_pipeline
```

Check the load:

```bash
docker exec -it music_dw_postgres psql -U airflow -d data_warehouse -c "
  SELECT
    (SELECT COUNT(*) FROM dim_artist) AS artists,
    (SELECT COUNT(*) FROM dim_track)  AS tracks,
    (SELECT COUNT(*) FROM fact_music) AS facts;
"
```

The dashboard at http://localhost:8501 reads `fact_music` directly. It stays empty until the DAG has loaded the warehouse.

Stop the stack with `docker compose down`. Add `-v` to drop the database volume too.

## Google Drive export

`store_google_drive` is optional. It uploads the merged CSV with a user OAuth token, because a service account has no quota on a personal Drive.

1. Create a desktop OAuth client in Google Cloud and save it as `credentials/oauth_credentials.json`.
2. `pip install google-auth-oauthlib` and run `python3 scripts/generate_token.py`.
3. Set `GOOGLE_DRIVE_FOLDER_ID` in `.env` and trigger the DAG again.

Credentials stay out of git.

## Decisions worth keeping

| Decision | Why |
|---|---|
| Star schema | The dashboard queries stay short and stable |
| Left join from Spotify | The catalog is complete even when an artist never won |
| Normalized `artist_key` | The two sources do not agree on casing |
| Truncate and reload | A rerun replaces the warehouse instead of appending to it |
| LocalExecutor | One node is enough; Celery adds a broker this pipeline does not need |
| Flag the Grammy file as winners-only | `is_nominated` and `is_winner` match for linked artists, and the model should not pretend there are nominees in the source |
