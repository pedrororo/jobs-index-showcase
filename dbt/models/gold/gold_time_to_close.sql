-- How long new postings stay open, by source, by job category and by role family.
--
-- Most jobs are still open, so the median of closed jobs alone is biased
-- towards short postings. Kaplan-Meier also counts open jobs (right-censored
-- at their last sighting); both are reported (docs/LAKEHOUSE.md).
--   * NEW_POSTING jobs only: for the others, first_seen_at is not publication.
--   * days_open = last_seen_at - first_seen_at in whole days (a lower bound).
--   * role_family only for jobs first seen once attributes were captured;
--     job_category (from the title, in the state snapshot) covers every job.
WITH opens AS (
    SELECT job_tracking_id, open_reason
    FROM {{ ref('silver_job_events') }}
    WHERE event_type = 'JOB_OPENED'
),
attributes_start AS (
    SELECT min(first_exported_at) AS since FROM {{ ref('silver_job_attributes') }}
),
jobs AS (
    SELECT
        s.job_tracking_id,
        s.collector,
        s.job_category,
        a.role_family,
        s.first_seen_at,
        s.status = 'CLOSED' AS is_closed,
        CAST(floor(({{ epoch_seconds('s.last_seen_at') }} - {{ epoch_seconds('s.first_seen_at') }}) / 86400) AS INTEGER)
            AS days_open
    FROM {{ ref('stg_job_state') }} s
    JOIN opens o ON o.job_tracking_id = s.job_tracking_id
    LEFT JOIN {{ ref('silver_job_attributes') }} a ON a.job_tracking_id = s.job_tracking_id
    WHERE coalesce(s.open_reason, o.open_reason) = 'NEW_POSTING'
),
grouped AS (
    SELECT 'all' AS dimension, 'all' AS value, * FROM jobs
    UNION ALL
    SELECT 'collector', collector, * FROM jobs
    UNION ALL
    SELECT 'job_category', job_category, * FROM jobs WHERE job_category IS NOT NULL
    UNION ALL
    SELECT 'role_family', role_family, *
    FROM jobs
    WHERE role_family IS NOT NULL
      AND first_seen_at >= (SELECT since FROM attributes_start)
),
per_day AS (
    SELECT dimension, value, days_open, count(*) AS jobs, count(*) FILTER (WHERE is_closed) AS closed
    FROM grouped
    GROUP BY dimension, value, days_open
),
survival AS (
    -- Jobs still observed at the start of each day: open at least this long.
    SELECT *, sum(jobs) OVER (PARTITION BY dimension, value ORDER BY days_open DESC) AS at_risk
    FROM per_day
),
curve AS (
    SELECT
        *,
        CASE
            WHEN bool_or(closed = at_risk) OVER cumulative THEN 0.0
            -- A product computed as exp(sum(ln(...))) so a window function can do it.
            ELSE exp(sum(CASE WHEN closed < at_risk THEN ln(1 - closed / at_risk) ELSE 0 END) OVER cumulative)
        END AS survival
    FROM survival
    WINDOW cumulative AS (PARTITION BY dimension, value ORDER BY days_open
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
closed_only AS (
    SELECT dimension, value, median(days_open) AS median_days_closed_only
    FROM grouped
    WHERE is_closed
    GROUP BY dimension, value
)
SELECT
    c.dimension,
    c.value,
    sum(c.jobs) AS jobs,
    sum(c.closed) AS closed,
    sum(c.jobs) - sum(c.closed) AS still_open,
    min(c.days_open) FILTER (WHERE c.survival <= 0.5) AS median_days_open_km,
    any_value(co.median_days_closed_only) AS median_days_closed_only,
    max(c.days_open) AS max_days_observed,
    -- NULL until some job in the group has been followed that long.
    CASE WHEN max(c.days_open) >= 7
        THEN round(min(c.survival) FILTER (WHERE c.days_open <= 7), 3) END AS share_open_after_7d,
    CASE WHEN max(c.days_open) >= 30
        THEN round(min(c.survival) FILTER (WHERE c.days_open <= 30), 3) END AS share_open_after_30d
FROM curve c
LEFT JOIN closed_only co
  ON co.dimension = c.dimension AND co.value IS NOT DISTINCT FROM c.value
GROUP BY c.dimension, c.value
