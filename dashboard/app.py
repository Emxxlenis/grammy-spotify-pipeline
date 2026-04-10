"""
Spotify × Grammy Awards — ETL Dashboard
Workshop 2 | Universidad Autónoma de Occidente

Reads directly from the data_warehouse PostgreSQL database.
Access at: http://localhost:8501
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Spotify × Grammy Dashboard",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

DB_URI = (
    f"postgresql+psycopg2://{os.getenv('DB_USER', 'airflow')}:"
    f"{os.getenv('DB_PASSWORD', 'airflow')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'data_warehouse')}"
)


@st.cache_resource
def get_engine():
    return create_engine(DB_URI)


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


# Guard: ensure the DW has data before rendering
try:
    total = int(query("SELECT COUNT(*) AS n FROM fact_music")["n"].iloc[0])
    if total == 0:
        st.warning("The Data Warehouse is empty. Run the Airflow DAG first.")
        st.stop()
except Exception as e:
    st.error(f"Could not connect to the Data Warehouse: {e}")
    st.info("Make sure the ETL pipeline has run successfully.")
    st.stop()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🎵 Spotify × Grammy Awards Dashboard")
st.markdown("**Workshop 2 — ETL Pipeline** | Spotify + Grammy Awards (1958–2019)")
st.divider()

# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------
kpis = query("""
    SELECT
        COUNT(DISTINCT fm.track_id)                                          AS total_tracks,
        COUNT(DISTINCT fm.artist_id)                                         AS total_artists,
        COUNT(DISTINCT CASE WHEN fm.is_winner THEN fm.artist_id END)         AS grammy_winners,
        ROUND(AVG(CASE WHEN fm.is_winner     THEN fm.popularity END)::numeric, 1) AS avg_pop_winner,
        ROUND(AVG(CASE WHEN NOT fm.is_winner THEN fm.popularity END)::numeric, 1) AS avg_pop_non_winner,
        COUNT(DISTINCT CASE WHEN fm.is_winner THEN dt.year END)              AS grammy_years
    FROM fact_music fm
    LEFT JOIN dim_time dt ON dt.time_id = fm.time_id
""").iloc[0]

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Unique Tracks",          f"{int(kpis['total_tracks']):,}")
c2.metric("Unique Artists",         f"{int(kpis['total_artists']):,}")
c3.metric("Grammy-Winning Artists", f"{int(kpis['grammy_winners']):,}")
c4.metric("Avg Popularity (Winners)",     f"{kpis['avg_pop_winner']}")
c5.metric("Avg Popularity (No Grammy)",   f"{kpis['avg_pop_non_winner']}")
c6.metric("Grammy Years Covered",   f"{int(kpis['grammy_years'])}")
st.divider()

# ---------------------------------------------------------------------------
# Row 1: Top artists | Grammy wins by year
# ---------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 10 Artists by Spotify Popularity")
    data = query("""
        SELECT da.artist_name,
               ROUND(AVG(fm.popularity)::numeric, 1) AS avg_popularity,
               COUNT(DISTINCT fm.track_id)            AS total_tracks
        FROM fact_music fm
        JOIN dim_artist da ON da.artist_id = fm.artist_id
        WHERE fm.popularity > 0
        GROUP BY da.artist_name
        ORDER BY avg_popularity DESC
        LIMIT 10
    """)
    fig = px.bar(
        data, x="avg_popularity", y="artist_name", orientation="h",
        color="avg_popularity", color_continuous_scale="Teal", text="avg_popularity",
        labels={"avg_popularity": "Avg Popularity", "artist_name": "Artist"},
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False,
                      yaxis={"categoryorder": "total ascending"})
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Grammy Awards per Year (1958–2019)")
    data = query("""
        SELECT dt.year, COUNT(*) AS total_wins
        FROM fact_music fm
        JOIN dim_time dt ON dt.time_id = fm.time_id
        WHERE fm.is_winner = TRUE AND dt.year > 0
        GROUP BY dt.year
        ORDER BY dt.year
    """)
    fig2 = px.line(
        data, x="year", y="total_wins", markers=True,
        labels={"year": "Year", "total_wins": "Awards Given"},
        color_discrete_sequence=["#f0a500"],
    )
    fig2.update_traces(line_width=2)
    fig2.update_layout(hovermode="x unified")
    st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 2: Popularity distribution | Top Grammy categories
# ---------------------------------------------------------------------------
col3, col4 = st.columns(2)

with col3:
    st.subheader("Spotify Popularity: Grammy Winners vs Non-Winners")
    data = query("""
        SELECT CASE WHEN fm.is_winner THEN 'Grammy Winner' ELSE 'No Grammy' END AS status,
               fm.popularity
        FROM fact_music fm
        WHERE fm.popularity > 0
    """)
    fig3 = px.violin(
        data, x="status", y="popularity", color="status", box=True, points=False,
        color_discrete_map={"Grammy Winner": "#f0a500", "No Grammy": "#636efa"},
        labels={"status": "", "popularity": "Popularity (0–100)"},
    )
    fig3.update_layout(showlegend=False)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("Top 15 Grammy Categories by Award Count")
    data = query("""
        SELECT da.category, COUNT(*) AS total_awards
        FROM fact_music fm
        JOIN dim_award da ON da.award_id = fm.award_id
        WHERE fm.is_winner = TRUE
        GROUP BY da.category
        ORDER BY total_awards DESC
        LIMIT 15
    """)
    fig4 = px.bar(
        data, x="total_awards", y="category", orientation="h",
        color="total_awards", color_continuous_scale="Oranges",
        labels={"total_awards": "Awards", "category": "Category"},
    )
    fig4.update_layout(coloraxis_showscale=False, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig4, use_container_width=True)

# ---------------------------------------------------------------------------
# Row 3: Audio features radar | Grammy wins vs popularity scatter
# ---------------------------------------------------------------------------
st.divider()
col5, col6 = st.columns(2)

with col5:
    st.subheader("Audio Features: Grammy Winners vs Non-Winners")
    data = query("""
        SELECT
            CASE WHEN fm.is_winner THEN 'Grammy Winner' ELSE 'No Grammy' END AS grupo,
            ROUND(AVG(dt.danceability)::numeric, 3)  AS danceability,
            ROUND(AVG(dt.energy)::numeric, 3)        AS energy,
            ROUND(AVG(dt.acousticness)::numeric, 3)  AS acousticness,
            ROUND(AVG(dt.valence)::numeric, 3)       AS valence,
            ROUND(AVG(dt.speechiness)::numeric, 3)   AS speechiness,
            ROUND(AVG(dt.liveness)::numeric, 3)      AS liveness
        FROM fact_music fm
        JOIN dim_track dt ON dt.track_id = fm.track_id
        GROUP BY grupo
    """)
    features       = ["danceability", "energy", "acousticness", "valence", "speechiness", "liveness"]
    feature_labels = ["Danceability", "Energy", "Acousticness", "Valence", "Speechiness", "Liveness"]
    colors         = {"Grammy Winner": "#f0a500", "No Grammy": "#636efa"}
    fig5 = go.Figure()
    for _, row in data.iterrows():
        values = [float(row[f]) for f in features] + [float(row[features[0]])]
        fig5.add_trace(go.Scatterpolar(
            r=values,
            theta=feature_labels + [feature_labels[0]],
            fill="toself",
            name=row["grupo"],
            line_color=colors.get(row["grupo"], "#999"),
            opacity=0.7,
        ))
    fig5.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        showlegend=True,
    )
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    st.subheader("Artists: Grammy Wins vs Spotify Popularity")
    data = query("""
        SELECT da.artist_name,
               COUNT(CASE WHEN fm.is_winner THEN 1 END) AS grammy_wins,
               ROUND(AVG(fm.popularity)::numeric, 1)     AS avg_popularity,
               COUNT(DISTINCT fm.track_id)               AS total_tracks
        FROM fact_music fm
        JOIN dim_artist da ON da.artist_id = fm.artist_id
        WHERE fm.is_winner = TRUE
        GROUP BY da.artist_name
        HAVING COUNT(CASE WHEN fm.is_winner THEN 1 END) >= 2
        ORDER BY grammy_wins DESC
        LIMIT 50
    """)
    if not data.empty:
        fig6 = px.scatter(
            data, x="grammy_wins", y="avg_popularity",
            size="total_tracks", hover_name="artist_name",
            color="avg_popularity", color_continuous_scale="Viridis",
            labels={
                "grammy_wins":    "Grammy Wins",
                "avg_popularity": "Avg Spotify Popularity",
                "total_tracks":   "Tracks on Spotify",
            },
        )
        fig6.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig6, use_container_width=True)
    else:
        st.info("Not enough data for this chart.")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.divider()
st.caption("Workshop 2 — ETL with Apache Airflow | Universidad Autónoma de Occidente")
