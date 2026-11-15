-- Every job has exactly one current version.
SELECT job_tracking_id, count(*) FILTER (WHERE is_current) AS current_versions
FROM {{ ref('silver_job_history_scd2') }}
GROUP BY job_tracking_id
HAVING count(*) FILTER (WHERE is_current) <> 1
