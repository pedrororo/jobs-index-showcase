-- What happened in the job market each day, separated from what happened to
-- our coverage (new sources, baseline loads, rerun artifacts).
SELECT
    collection_date,
    count(DISTINCT run_id) AS runs_with_events,  -- runs with no changes write no events
    count(*) FILTER (WHERE open_reason = 'NEW_POSTING') AS opened_new_postings,
    count(*) FILTER (WHERE open_reason = 'NEW_COHORT') AS opened_from_new_sources,
    count(*) FILTER (WHERE open_reason = 'BASELINE') AS opened_baseline,
    count(*) FILTER (WHERE open_reason = 'BACKFILL') AS opened_backfill,  -- old jobs we could not reach before
    count(*) FILTER (WHERE is_reliable_close) AS closed,
    count(*) FILTER (WHERE event_type = 'JOB_CLOSED' AND NOT coalesce(is_reliable_close, false)) AS closed_unreliable,
    count(*) FILTER (WHERE event_type = 'JOB_REOPENED') AS reopened,
    count(*) FILTER (WHERE event_type = 'JOB_UPDATED') AS updated
FROM {{ ref('silver_job_events') }}
GROUP BY collection_date
