SELECT
    run_id,
    run_status,
    CAST(started_at AS TIMESTAMP) AS started_at,
    CAST(run_duration_s AS DOUBLE) AS run_duration_s,
    CAST(raw_jobs AS BIGINT) AS raw_jobs,
    source,
    source_status,
    source_error,
    CAST(jobs_fetched AS BIGINT) AS jobs_fetched
FROM {{ source('bronze', 'bronze_run_health') }}
