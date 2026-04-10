from __future__ import annotations

from pathlib import Path

import pandas as pd


def extract_spotify_csv(input_path: str | Path) -> pd.DataFrame:
    """Read the Spotify dataset from a CSV file."""
    return pd.read_csv(input_path)


def load_grammys_csv_to_source_db(
    csv_path: str | Path,
    grammys_conn_uri: str,
    table_name: str = "grammy_awards",
) -> int:
    """
    Load the Grammy Awards CSV into the source database (grammys_db).
    Simulates data that already exists in a production OLTP database.
    Returns the number of rows written.
    """
    from sqlalchemy import create_engine

    df = pd.read_csv(csv_path)
    cols = ["year", "title", "category", "nominee", "artist", "workers", "img", "winner"]
    df = df[cols]
    df["winner"] = df["winner"].map({"True": True, "False": False, True: True, False: False})

    engine = create_engine(grammys_conn_uri)
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False, method="multi")
    return len(df)


def extract_grammys_from_db(
    grammys_conn_uri: str,
    table_name: str = "grammy_awards",
) -> pd.DataFrame:
    """Extract Grammy Awards data from the source database (grammys_db)."""
    from sqlalchemy import create_engine

    engine = create_engine(grammys_conn_uri)
    with engine.begin() as conn:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)
