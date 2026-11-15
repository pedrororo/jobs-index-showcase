-- Versions are 1..n, each ends exactly where the next starts, and only the last is open.
WITH ordered AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY job_tracking_id ORDER BY version) AS expected_version,
        lead(valid_from) OVER (PARTITION BY job_tracking_id ORDER BY version) AS next_valid_from
    FROM {{ ref('silver_job_history_scd2') }}
)
SELECT job_tracking_id, version
FROM ordered
WHERE version <> expected_version
   OR valid_to IS DISTINCT FROM next_valid_from
   OR (valid_to IS NOT NULL AND valid_to <= valid_from)
