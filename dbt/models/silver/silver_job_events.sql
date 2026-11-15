-- One row per lifecycle event, with the opened/closed meaning made explicit.
--   open_reason        BASELINE / NEW_COHORT / NEW_POSTING / BACKFILL (only NEW_POSTING is a published job)
--                      The state's value wins: rows can be relabelled after their event was archived.
--   is_reliable_close  missing at least min_close_hours, not just several reruns in a day
-- Bronze is immutable, so both are derived here for runs written before the
-- pipeline recorded them (docs/LAKEHOUSE.md).
WITH events AS (
    SELECT
        *,
        event_at AS event_ts,
        coalesce(
            company_key,
            lower(trim({{ collapse_spaces("coalesce(company, 'unknown')") }}))
        ) AS cohort_company
    FROM {{ ref('stg_events') }}
),
first_run AS (
    SELECT min(run_id) AS run_id FROM events
),
cohort_first_run AS (
    SELECT collector, cohort_company, min(run_id) AS first_run_id
    FROM events
    WHERE event_type = 'JOB_OPENED'
    GROUP BY collector, cohort_company
),
last_miss AS (
    -- The miss that started the absence each closure ends.
    SELECT c.event_id, max(m.event_ts) AS first_missed_at
    FROM events c
    JOIN events m
      ON m.job_tracking_id = c.job_tracking_id
     AND m.event_type = 'JOB_MISSING'
     AND m.event_ts <= c.event_ts
    WHERE c.event_type = 'JOB_CLOSED'
    GROUP BY c.event_id
),
hours AS (
    SELECT
        e.*,
        c.first_run_id,
        s.open_reason AS state_open_reason,
        CASE WHEN e.event_type = 'JOB_CLOSED' THEN coalesce(
            e.hours_missing,
            ({{ epoch_seconds('e.event_ts') }} - {{ epoch_seconds('lm.first_missed_at') }}) / 3600
        ) END AS closed_after_hours
    FROM events e
    LEFT JOIN cohort_first_run c
      ON c.collector = e.collector AND c.cohort_company = e.cohort_company
    LEFT JOIN last_miss lm
      ON lm.event_id = e.event_id
    LEFT JOIN {{ ref('stg_job_state') }} s
      ON s.job_tracking_id = e.job_tracking_id
     AND e.event_type = 'JOB_OPENED'
)
SELECT
    event_id,
    run_id,
    event_ts AS event_at,
    collection_date,
    event_type,
    job_tracking_id,
    collector,
    cohort_company AS company_key,
    company,
    title,
    link,
    changed_fields,
    field_changed,
    CASE
        WHEN event_type <> 'JOB_OPENED' THEN NULL
        WHEN state_open_reason IS NOT NULL THEN state_open_reason
        WHEN open_reason IS NOT NULL THEN open_reason
        WHEN run_id = (SELECT run_id FROM first_run) THEN 'BASELINE'
        WHEN run_id = first_run_id THEN 'NEW_COHORT'
        ELSE 'NEW_POSTING'
    END AS open_reason,
    round(closed_after_hours, 1) AS hours_missing,
    CASE WHEN event_type = 'JOB_CLOSED'
        THEN closed_after_hours >= {{ var('min_close_hours') }} END AS is_reliable_close
FROM hours
