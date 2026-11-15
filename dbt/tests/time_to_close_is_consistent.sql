-- Survival shares are probabilities and the counts add up.
SELECT *
FROM {{ ref('gold_time_to_close') }}
WHERE jobs <> closed + still_open
   OR share_open_after_7d NOT BETWEEN 0 AND 1
   OR share_open_after_30d NOT BETWEEN 0 AND 1
   OR share_open_after_30d > share_open_after_7d
   OR median_days_open_km < 0
