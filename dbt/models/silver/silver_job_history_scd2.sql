{{ config(
    materialized='incremental',
    unique_key=['job_tracking_id', 'version'],
    incremental_strategy=replace_strategy(),
    on_schema_change='fail',
    liquid_clustered_by=(['job_tracking_id'] if target.type == 'databricks' else none)
) }}
-- SCD Type 2 history: one version per run that changed a job.
--   valid_from  when the version was first observed
--   valid_to    when the next version started (NULL for the current one)
-- Point in time: valid_from <= T AND (valid_to IS NULL OR valid_to > T)
--
-- Incremental runs read only events newer than the table, and return
--   * the new versions (inserted), and
--   * each changed job's current version, re-emitted with valid_to set and
--     is_current = false (replaced through the unique key).
-- The strategy writes both in one statement: MERGE on Databricks,
-- delete+insert on DuckDB. A run older than the table (a backfill) is caught by
-- the scd2_covers_every_run test; rebuild with --full-refresh.
WITH status_events AS (
    SELECT
        job_tracking_id, run_id, event_at, event_type, changed_fields,
        title, company, collector, company_key, link,
        CASE event_type
            WHEN 'JOB_MISSING' THEN 'POSSIBLY_CLOSED'
            WHEN 'JOB_CLOSED' THEN 'CLOSED'
            ELSE 'OPEN'
        END AS status
    FROM {{ ref('silver_job_events') }}
    WHERE event_type IN ('JOB_OPENED', 'JOB_UPDATED', 'JOB_REOPENED', 'JOB_RETURNED', 'JOB_MISSING', 'JOB_CLOSED')
    {% if is_incremental() %}
      AND event_at > (SELECT max(valid_from) FROM {{ this }})
    {% endif %}
),
per_run AS (
    -- A run can emit several events for one job (JOB_REOPENED + JOB_UPDATED).
    SELECT
        job_tracking_id,
        run_id,
        min(event_at) AS valid_from,
        any_value(status) AS status,
        {{ sorted_join('array_agg(DISTINCT event_type)') }} AS change_types,
        array_distinct(flatten(array_agg(coalesce(changed_fields, {{ empty_string_array() }})))) AS changed_fields,
        any_value(title) AS title,
        any_value(company) AS company,
        any_value(collector) AS collector,
        any_value(company_key) AS company_key,
        any_value(link) AS link
    FROM status_events
    GROUP BY job_tracking_id, run_id
),
{% if is_incremental() %}
existing AS (
    SELECT job_tracking_id, max(version) AS last_version
    FROM {{ this }}
    WHERE job_tracking_id IN (SELECT job_tracking_id FROM per_run)
    GROUP BY job_tracking_id
),
{% endif %}
new_versions AS (
    SELECT
        p.*,
        {% if is_incremental() %}coalesce(e.last_version, 0) + {% endif %}row_number() OVER w AS version,
        lead(p.valid_from) OVER w AS valid_to,
        lead(p.valid_from) OVER w IS NULL AS is_current,
        row_number() OVER w = 1 AS is_first_in_batch
    FROM per_run p
    {% if is_incremental() %}LEFT JOIN existing e ON e.job_tracking_id = p.job_tracking_id{% endif %}
    WINDOW w AS (PARTITION BY p.job_tracking_id ORDER BY p.valid_from, p.run_id)
)
SELECT
    job_tracking_id, version, valid_from, valid_to, is_current, status, change_types,
    changed_fields, title, company, collector, company_key, link, run_id
FROM new_versions
{% if is_incremental() %}
UNION ALL
-- Close the version that was current before this batch.
SELECT
    t.job_tracking_id, t.version, t.valid_from, n.valid_from AS valid_to, false AS is_current, t.status,
    t.change_types, t.changed_fields, t.title, t.company, t.collector, t.company_key, t.link, t.run_id
FROM {{ this }} t
JOIN new_versions n
  ON n.job_tracking_id = t.job_tracking_id AND n.is_first_in_batch
WHERE t.is_current
{% endif %}
