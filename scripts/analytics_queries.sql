-- ===============================================
-- KPI 1: Top 10 artists by average popularity
-- ===============================================
SELECT
    da.artist_name,
    ROUND(AVG(fm.popularity)::numeric, 2) AS avg_popularity,
    COUNT(*) AS tracks_considered
FROM fact_music fm
JOIN dim_artist da ON da.artist_id = fm.artist_id
GROUP BY da.artist_name
ORDER BY avg_popularity DESC, tracks_considered DESC
LIMIT 10;

-- ===============================================
-- KPI 2: Winners vs nominees count
-- ===============================================
SELECT
    SUM(CASE WHEN fm.is_winner THEN 1 ELSE 0 END) AS winner_rows,
    SUM(CASE WHEN fm.is_nominated THEN 1 ELSE 0 END) AS nominated_rows
FROM fact_music fm;

-- ===============================================
-- KPI 3: Popularity by Grammy status
-- ===============================================
SELECT
    CASE
        WHEN fm.is_winner THEN 'Winner'
        WHEN fm.is_nominated THEN 'Nominated'
        ELSE 'No Grammy'
    END AS grammy_status,
    ROUND(AVG(fm.popularity)::numeric, 2) AS avg_popularity,
    COUNT(*) AS rows_count
FROM fact_music fm
GROUP BY grammy_status
ORDER BY avg_popularity DESC;

-- ===============================================
-- Chart 1: Trend by year (nominations vs winners)
-- ===============================================
SELECT
    dt.year,
    SUM(CASE WHEN fm.is_nominated THEN 1 ELSE 0 END) AS nominations,
    SUM(CASE WHEN fm.is_winner THEN 1 ELSE 0 END) AS winners
FROM fact_music fm
JOIN dim_time dt ON dt.time_id = fm.time_id
WHERE dt.year > 0
GROUP BY dt.year
ORDER BY dt.year;

-- ===============================================
-- Chart 2: Top categories by winner count
-- ===============================================
SELECT
    da2.category,
    COUNT(*) AS winner_count
FROM fact_music fm
JOIN dim_award da2 ON da2.award_id = fm.award_id
WHERE fm.is_winner = TRUE
GROUP BY da2.category
ORDER BY winner_count DESC
LIMIT 15;

-- ===============================================
-- Chart 3: Genre vs popularity for nominated tracks
-- ===============================================
SELECT
    dt2.genre,
    ROUND(AVG(fm.popularity)::numeric, 2) AS avg_popularity,
    COUNT(*) AS track_rows
FROM fact_music fm
JOIN dim_track dt2 ON dt2.track_id = fm.track_id
WHERE fm.is_nominated = TRUE
GROUP BY dt2.genre
HAVING COUNT(*) >= 30
ORDER BY avg_popularity DESC
LIMIT 20;
