-- vacancy_rent_lag.sql
-- Explores the relationship between vacancy rate level and rent growth
-- across CMAs.  With two time points we measure: did cities with lower
-- Oct-24 vacancy see higher rent growth by Oct-25?

WITH cma_totals AS (
    SELECT
        c.name           AS city,
        c.province,
        r.survey_date,
        r.vacancy_rate,
        a.avg_rent
    FROM vacancy_rates r
    JOIN cities    c ON c.id          = r.city_id
    JOIN avg_rents a ON a.city_id     = r.city_id
                    AND a.survey_date = r.survey_date
                    AND a.bedroom_type = r.bedroom_type
                    AND a.zone        = r.zone
    WHERE r.is_cma_total = TRUE
      AND r.bedroom_type = 'Total'
),
pivoted AS (
    SELECT
        city,
        province,
        MAX(vacancy_rate) FILTER (WHERE survey_date = '2024-10-01') AS vacancy_oct24,
        MAX(vacancy_rate) FILTER (WHERE survey_date = '2025-10-01') AS vacancy_oct25,
        MAX(avg_rent)     FILTER (WHERE survey_date = '2024-10-01') AS rent_oct24,
        MAX(avg_rent)     FILTER (WHERE survey_date = '2025-10-01') AS rent_oct25
    FROM cma_totals
    GROUP BY city, province
)
SELECT
    city,
    province,
    vacancy_oct24,
    vacancy_oct25,
    ROUND((vacancy_oct25 - vacancy_oct24)::numeric, 1)            AS vacancy_change,
    rent_oct24,
    rent_oct25,
    ROUND(((rent_oct25 - rent_oct24)::numeric / rent_oct24 * 100), 2) AS rent_yoy_pct,
    -- Flag markets where vacancy fell AND rent rose (demand-driven tightening)
    CASE
        WHEN vacancy_oct25 < vacancy_oct24 AND rent_oct25 > rent_oct24 THEN 'tightening'
        WHEN vacancy_oct25 > vacancy_oct24 AND rent_oct25 > rent_oct24 THEN 'loosening+rising'
        WHEN vacancy_oct25 < vacancy_oct24 AND rent_oct25 < rent_oct24 THEN 'tightening+falling'
        ELSE 'loosening'
    END AS market_dynamic
FROM pivoted
ORDER BY rent_yoy_pct DESC;
