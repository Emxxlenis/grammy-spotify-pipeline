from __future__ import annotations

import pandas as pd


def merge_spotify_grammys(spotify_df: pd.DataFrame, grammys_df: pd.DataFrame) -> pd.DataFrame:
    """
    LEFT JOIN Spotify tracks with Grammy Awards on normalized artist key.
    Spotify is the left table — all tracks are preserved even without a Grammy match.
    """
    grammys_subset = (
        grammys_df[["artist_key", "year", "category", "title", "nominee", "winner"]]
        .copy()
        .rename(columns={
            "year":     "award_year",
            "category": "award_category",
            "title":    "ceremony_title",
            "nominee":  "nominee_name",
        })
    )

    merged = spotify_df.merge(grammys_subset, on="artist_key", how="left")
    merged["award_year"] = merged["award_year"].fillna(0).astype(int)
    merged["winner"]     = merged["winner"].fillna(False).astype(bool)
    return merged
