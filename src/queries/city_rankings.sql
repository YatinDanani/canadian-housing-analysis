-- city_rankings.sql
-- Ranks all 18 CMAs by rent growth, vacancy tightness, and universe growth
-- for the Total bedroom type between Oct-24 and Oct-25.

WITH cma_totals AS (
    SELECT
        c.name                                                      AS city,
        c.province,
        r.survey_date,
        r.vacancy_rate,
        r.rental_universe,
        a.avg_rent
    FROM vacancy_rates r
    JOIN cities        c ON c.id = r.city_id
    JOIN avg_rents     a ON a.city_id      = r.city_id
                        AND a.survey_date  = r.survey_date
                        AND a.bedroom_type = r.bedroom_type
                        AND a.zone        = r.zone
    WHERE r.is_cma_total  = TRUE
      AND r.bedroom_type  = 'Total'
),
pivoted AS (
    SELECT
        city,
        province,
        MAX(avg_rent)      FILTER (WHERE survey_date = '2024-10-01') AS rent_oct24,
        MAX(avg_rent)      FILTER (WHERE survey_date = '2025-10-01') AS rent_oct25,
        MAX(vacancy_rate)  FILTER (WHERE survey_date = '2025-10-01') AS vacancy_oct25,
        MAX(rental_universe) FILTER (WHERE survey_date = '2024-10-01') AS universe_oct24,
        MAX(rental_universe) FILTER (WHERE survey_date = '2025-10-01') AS universe_oct25
    FROM cma_totals
    GROUP BY city, province
),
ranked AS (
    SELECT
        city,
        province,
        rent_oct24,
        rent_oct25,
        ROUND(((rent_oct25 - rent_oct24)::numeric / rent_oct24 * 100), 2) AS rent_yoy_pct,
        vacancy_oct25,
        universe_oct25 - universe_oct24                                    AS universe_added,
        RANK() OVER (ORDER BY (rent_oct25 - rent_oct24)::float / rent_oct24 DESC) AS rent_growth_rank,
        RANK() OVER (ORDER BY vacancy_oct25 ASC)                                  AS vacancy_rank
    FROM pivoted
)
SELECT *
FROM ranked
ORDER BY rent_growth_rank;
