-- Data quality checks for every run (docs/DATA_QUALITY.md).
--
-- Each run is compared with the runs before it, so the latest run gives
-- today's alerts and older runs show what the checks would have caught
-- (a backtest that also tunes the thresholds).
--
--   ERROR  something is broken: a source failed, returned nothing, or lost
--          more than half of its usual volume
--   WARN   worth a look: a source that never returns jobs, a long gap
--          between runs, an unusually slow or unfinished run
--   INFO   expected but notable: a source more than doubled (new coverage)
--
-- Only run_volume_drop blocks publishing: a site built from a run with less
-- than half the usual jobs would hide most jobs from visitors.
WITH health AS (
    SELECT * FROM {{ ref('stg_run_health') }}
),
runs AS (
    SELECT
        run_id,
        any_value(run_status) AS run_status,
        any_value(started_at) AS started_at,
        any_value(run_duration_s) AS run_duration_s,
        any_value(raw_jobs) AS raw_jobs
    FROM health
    GROUP BY run_id
),
run_baseline AS (
    SELECT
        *,
        lag(started_at) OVER (ORDER BY started_at, run_id) AS previous_started_at,
        {{ windowed_median('raw_jobs', 'previous_7') }} AS baseline_raw_jobs,
        {{ windowed_median('run_duration_s', 'previous_7') }} AS baseline_duration_s,
        count(*) OVER previous_7 AS baseline_runs
    FROM runs
    WINDOW previous_7 AS (ORDER BY started_at, run_id ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
),
source_baseline AS (
    SELECT
        run_id,
        started_at,
        source,
        source_status,
        source_error,
        jobs_fetched,
        {{ windowed_median('jobs_fetched', 'previous_7') }} AS baseline_jobs,
        max(jobs_fetched) OVER previous_all AS max_previous_jobs,
        count(*) OVER previous_all AS previous_runs
    FROM health
    WHERE source IS NOT NULL
    WINDOW
        previous_7 AS (PARTITION BY source ORDER BY started_at, run_id ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING),
        previous_all AS (PARTITION BY source ORDER BY started_at, run_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
),
checks AS (
    SELECT run_id, started_at, 'source_failed' AS check_name, source AS subject, 'ERROR' AS severity,
           CAST(jobs_fetched AS DOUBLE) AS observed, CAST(NULL AS DOUBLE) AS expected,
           'Fetcher failed: ' || coalesce(nullif(source_error, ''), 'no error message') AS message
    FROM source_baseline WHERE source_status <> 'ok'

    UNION ALL
    -- Reddit rate-limits (429) runs now and then, so an empty Reddit is only a warning
    SELECT run_id, started_at, 'source_empty', source, CASE WHEN source = 'redditrss' THEN 'WARN' ELSE 'ERROR' END,
           0, max_previous_jobs,
           printf('Reported ok but returned 0 jobs (up to %d before)', max_previous_jobs)
    FROM source_baseline
    WHERE source_status = 'ok' AND jobs_fetched = 0 AND max_previous_jobs > 0

    UNION ALL
    SELECT run_id, started_at, 'source_never_returned_jobs', source, 'WARN', 0, NULL,
           printf('Reported ok but has returned 0 jobs in all %d runs: disabled upstream or broken?', previous_runs + 1)
    FROM source_baseline
    WHERE source_status = 'ok' AND jobs_fetched = 0 AND coalesce(max_previous_jobs, 0) = 0 AND previous_runs >= 2

    UNION ALL
    SELECT run_id, started_at, 'source_volume_drop', source, 'ERROR', jobs_fetched, baseline_jobs,
           printf('%d jobs vs a median of %d in the previous runs (-%d%%)',
                  jobs_fetched, CAST(baseline_jobs AS BIGINT), CAST(round(100 * (1 - jobs_fetched / baseline_jobs)) AS BIGINT))
    FROM source_baseline
    WHERE jobs_fetched > 0 AND baseline_jobs >= 20 AND jobs_fetched < 0.5 * baseline_jobs

    UNION ALL
    SELECT run_id, started_at, 'source_volume_jump', source, 'INFO', jobs_fetched, baseline_jobs,
           printf('%d jobs vs a median of %d: new boards or a changed filter?', jobs_fetched, CAST(baseline_jobs AS BIGINT))
    FROM source_baseline
    WHERE baseline_jobs >= 20 AND jobs_fetched > 2 * baseline_jobs

    UNION ALL
    SELECT run_id, started_at, 'run_volume_drop', 'run', 'ERROR', raw_jobs, baseline_raw_jobs,
           printf('%d jobs collected vs a median of %d: publishing is blocked',
                  raw_jobs, CAST(baseline_raw_jobs AS BIGINT))
    FROM run_baseline
    WHERE baseline_runs >= 3 AND raw_jobs < 0.5 * baseline_raw_jobs

    UNION ALL
    SELECT run_id, started_at, 'run_gap', 'run', 'WARN',
           round(({{ epoch_seconds('started_at') }} - {{ epoch_seconds('previous_started_at') }}) / 3600, 1), 24,
           printf('%.0f hours since the previous run: lifecycle dates in between are uncertain',
                  ({{ epoch_seconds('started_at') }} - {{ epoch_seconds('previous_started_at') }}) / 3600)
    FROM run_baseline
    WHERE ({{ epoch_seconds('started_at') }} - {{ epoch_seconds('previous_started_at') }}) / 3600 > 36

    UNION ALL
    SELECT run_id, started_at, 'run_slow', 'run', 'WARN', run_duration_s, baseline_duration_s,
           printf('Took %.0f min vs a median of %.0f: lost cache or a slow source?',
                  run_duration_s / 60, baseline_duration_s / 60)
    FROM run_baseline
    WHERE baseline_runs >= 3 AND run_duration_s > 600 AND run_duration_s > 2 * baseline_duration_s

    UNION ALL
    SELECT run_id, started_at, 'run_incomplete', 'run', 'WARN', NULL, NULL,
           'Run stopped after collection (status ' || run_status || ')'
    FROM run_baseline
    WHERE run_status <> 'SUCCESS'
)
SELECT
    *,
    check_name = 'run_volume_drop' AS blocks_publish
FROM checks
