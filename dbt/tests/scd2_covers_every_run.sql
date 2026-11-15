-- Every run that changed a job's status is in the history. A failure after an
-- incremental run means an older run arrived late (a backfill): rebuild with
-- dbt build --full-refresh --select silver_job_history_scd2.
SELECT DISTINCT e.run_id
FROM {{ ref('silver_job_events') }} e
WHERE e.event_type IN ('JOB_OPENED', 'JOB_UPDATED', 'JOB_REOPENED', 'JOB_RETURNED', 'JOB_MISSING', 'JOB_CLOSED')
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref('silver_job_history_scd2') }} h WHERE h.run_id = e.run_id
  )
