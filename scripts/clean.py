from __future__ import annotations

import re

import pandas as pd


def _normalize_artist(value: str) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", value.strip().lower())


def _primary_artist(artists_value: str) -> str:
    """Return the first artist from a semicolon-separated collaborations string."""
    if not isinstance(artists_value, str):
        return ""
    return artists_value.split(";")[0].strip()


def clean_spotify(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned = cleaned.drop_duplicates(subset=["track_id"])
    cleaned["artists"]    = cleaned["artists"].fillna("Unknown Artist")
    cleaned["album_name"] = cleaned["album_name"].fillna("Unknown Album")
    cleaned["track_name"] = cleaned["track_name"].fillna("Unknown Track")
    cleaned["track_genre"] = cleaned["track_genre"].fillna("unknown")
    cleaned["artist_primary"] = cleaned["artists"].map(_primary_artist)
    cleaned["artist_key"]     = cleaned["artist_primary"].map(_normalize_artist)
    return cleaned


def clean_grammys(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned = cleaned.drop_duplicates()
    cleaned["winner"]   = cleaned["winner"].fillna(False).astype(bool)
    cleaned["artist"]   = cleaned["artist"].fillna(cleaned["nominee"]).fillna("Unknown Artist")
    cleaned["category"] = cleaned["category"].fillna("Unknown Category")
    cleaned["title"]    = cleaned["title"].fillna("Unknown Ceremony")
    cleaned["nominee"]  = cleaned["nominee"].fillna("Unknown Nominee")
    cleaned["year"]     = pd.to_numeric(cleaned["year"], errors="coerce").fillna(0).astype(int)
    cleaned["artist_key"] = cleaned["artist"].map(_normalize_artist)
    return cleaned
