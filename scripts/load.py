from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def export_dataframe_to_csv(df: pd.DataFrame, output_path: str | Path) -> str:
    """Save a DataFrame as CSV. Returns the file path."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("CSV saved at %s (%s rows)", path, f"{len(df):,}")
    return str(path)


# ---------------------------------------------------------------------------
# Google Drive upload
# ---------------------------------------------------------------------------

def upload_to_google_drive(filepath: str | Path, folder_id: str, credentials_path: str) -> str | None:
    """
    Upload a file to Google Drive using OAuth2 user credentials.

    Setup (one-time):
        1. Create an OAuth 2.0 Client ID (Desktop app) in Google Cloud Console.
        2. Download the JSON as credentials/oauth_credentials.json.
        3. Run scripts/generate_token.py to generate credentials/token.json.
        4. The credentials/ volume is already mounted in docker-compose.yml.

    Returns:
        Google Drive file ID, or None on failure.
    """
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        token_path = Path(credentials_path).parent / "token.json"

        if not token_path.exists():
            logger.warning(
                "token.json not found at %s. Run scripts/generate_token.py to create it. "
                "CSV is available locally at %s.", token_path, filepath,
            )
            return None

        token_data = json.loads(token_path.read_text())
        creds = Credentials(
            token=token_data.get("token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri=token_data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=token_data.get("client_id"),
            client_secret=token_data.get("client_secret"),
            scopes=token_data.get("scopes"),
        )

        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_data["token"] = creds.token
            token_path.write_text(json.dumps(token_data, indent=2))

        service  = build("drive", "v3", credentials=creds)
        filename = Path(filepath).name
        media    = MediaFileUpload(str(filepath), mimetype="text/csv", resumable=True)

        existing = service.files().list(
            q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
            fields="files(id)",
        ).execute().get("files", [])

        if existing:
            uploaded = service.files().update(
                fileId=existing[0]["id"],
                media_body=media,
                fields="id, webViewLink",
            ).execute()
            logger.info("File updated on Google Drive: %s", uploaded.get("webViewLink"))
        else:
            uploaded = service.files().create(
                body={"name": filename, "parents": [folder_id]},
                media_body=media,
                fields="id, webViewLink",
            ).execute()
            logger.info("File uploaded to Google Drive: %s", uploaded.get("webViewLink"))

        return uploaded.get("id")

    except FileNotFoundError:
        logger.warning("Credentials file not found at %s.", credentials_path)
        return None
    except Exception as exc:
        logger.error("Google Drive upload failed: %s. CSV available at %s.", exc, filepath)
        return None


# ---------------------------------------------------------------------------
# Star Schema — Data Warehouse
# ---------------------------------------------------------------------------

def _run_sql_file(conn_uri: str, sql_file_path: str | Path) -> None:
    """Execute a SQL file statement by statement."""
    sql_text = Path(sql_file_path).read_text(encoding="utf-8")
    engine = create_engine(conn_uri)
    with engine.begin() as conn:
        for statement in sql_text.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))


def load_star_schema(conn_uri: str, merged_df: pd.DataFrame, schema_sql_path: str | Path) -> dict:
    """
    Load the merged dataset into the star schema Data Warehouse.

    Strategy: TRUNCATE + RELOAD (idempotent — safe to run multiple times).

    Note: The Grammy Awards dataset contains ONLY winners (winner=True),
    so is_winner and is_nominated are equivalent for Grammy-linked artists.
    """
    logger.info("Starting Data Warehouse load (star schema)")
    _run_sql_file(conn_uri, schema_sql_path)

    engine = create_engine(conn_uri)
    work   = merged_df.copy()

    work["award_year"]    = pd.to_numeric(work["award_year"], errors="coerce").fillna(0).astype(int)
    work["winner"]        = work["winner"].fillna(False).astype(bool)
    work["artists_clean"] = work["artist_primary"].fillna("Unknown Artist")

    dim_artist = (
        work[["artists_clean"]]
        .drop_duplicates()
        .rename(columns={"artists_clean": "artist_name"})
        .sort_values("artist_name")
    )

    track_cols = [
        "track_id", "track_name", "album_name", "track_genre",
        "explicit", "duration_ms", "danceability", "energy",
        "loudness", "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo",
    ]
    dim_track = (
        work[track_cols]
        .drop_duplicates(subset=["track_id"])
        .rename(columns={"track_genre": "genre"})
    )
    dim_track["explicit"] = dim_track["explicit"].map(
        {True: True, False: False, "True": True, "False": False}
    ).fillna(False)

    dim_time = (
        work[work["award_year"] > 0][["award_year"]]
        .drop_duplicates()
        .rename(columns={"award_year": "year"})
        .sort_values("year")
    )

    dim_award = (
        work[["award_category", "ceremony_title", "nominee_name"]]
        .dropna(subset=["award_category"])
        .drop_duplicates()
        .rename(columns={"award_category": "category"})
        .sort_values(["category", "ceremony_title", "nominee_name"])
    )

    with engine.begin() as conn:
        conn.execute(text(
            "TRUNCATE TABLE fact_music, dim_award, dim_time, dim_track, dim_artist "
            "RESTART IDENTITY CASCADE"
        ))
        dim_artist.to_sql("dim_artist", conn, if_exists="append", index=False, method="multi")
        dim_track.to_sql("dim_track",   conn, if_exists="append", index=False, method="multi")
        dim_time.to_sql("dim_time",     conn, if_exists="append", index=False, method="multi")
        if not dim_award.empty:
            dim_award.to_sql("dim_award", conn, if_exists="append", index=False, method="multi")

        artist_db = pd.read_sql("SELECT artist_id, artist_name FROM dim_artist", conn)
        time_db   = pd.read_sql("SELECT time_id, year FROM dim_time", conn)
        award_db  = pd.read_sql(
            "SELECT award_id, category, ceremony_title, nominee_name FROM dim_award", conn
        )

    fact = work.merge(artist_db, left_on="artists_clean", right_on="artist_name", how="left")
    fact = fact.merge(time_db, left_on="award_year", right_on="year", how="left")

    if not award_db.empty:
        fact = fact.merge(
            award_db,
            left_on=["award_category", "ceremony_title", "nominee_name"],
            right_on=["category", "ceremony_title", "nominee_name"],
            how="left",
        )
    else:
        fact["award_id"] = None

    fact_final = fact[["artist_id", "track_id", "time_id", "award_id", "popularity", "winner"]].copy()
    fact_final["is_winner"]   = fact_final["winner"].fillna(False).astype(bool)
    fact_final["is_nominated"] = fact_final["award_id"].notna()
    fact_final = fact_final.drop(columns=["winner"])

    with engine.begin() as conn:
        fact_final.to_sql("fact_music", conn, if_exists="append", index=False, method="multi")

    stats = {
        "dim_artist": int(len(dim_artist)),
        "dim_track":  int(len(dim_track)),
        "dim_time":   int(len(dim_time)),
        "dim_award":  int(len(dim_award)),
        "fact_music": int(len(fact_final)),
    }
    logger.info("Data Warehouse loaded: %s", stats)
    return stats
