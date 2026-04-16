-- bedroom_breakdown.sql
-- Rent growth by bedroom type for each CMA, ordered by overall rent level.
-- Useful for identifying which unit sizes are driving city-level averages.

WITH rents AS (
    SELECT
        c.name           AS city,
        a.bedroom_type,
        a.survey_date,
        a.avg_rent
    FROM avg_rents a
    JOIN cities c ON c.id = a.city_id
    WHERE a.is_cma_total = TRUE
      AND a.bedroom_type != 'Total'
),
pivoted AS (
    SELECT
        city,
        bedroom_type,
        MAX(avg_rent) FILTER (WHERE survey_date = '2024-10-01') AS rent_oct24,
        MAX(avg_rent) FILTER (WHERE survey_date = '2025-10-01') AS rent_oct25
    FROM rents
    GROUP BY city, bedroom_type
)
SELECT
    city,
    bedroom_type,
    rent_oct24,
    rent_oct25,
    rent_oct25 - rent_oct24                                              AS abs_change,
    ROUND(((rent_oct25 - rent_oct24)::numeric / rent_oct24 * 100), 2)   AS pct_change,
    RANK() OVER (
        PARTITION BY bedroom_type
        ORDER BY (rent_oct25 - rent_oct24)::float / rent_oct24 DESC
    ) AS growth_rank_within_type
FROM pivoted
WHERE rent_oct24 IS NOT NULL AND rent_oct25 IS NOT NULL
ORDER BY city, bedroom_type;
