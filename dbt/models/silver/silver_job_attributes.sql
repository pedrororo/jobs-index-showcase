-- Latest classification of each job (SCD Type 1: overwrite, no history).
-- Exports are pruned after a few runs, so attributes only survive in bronze.
SELECT
    job_tracking_id,
    max_by(role_family, export_at) AS role_family,
    max_by(role_specialization, export_at) AS role_specialization,
    max_by(seniority, export_at) AS seniority,
    max_by(workplace_type, export_at) AS workplace_type,
    max_by(remote_scope, export_at) AS remote_scope,
    max_by(posted_at, export_at) AS posted_at,
    max_by(TRY_CAST(classification_confidence AS DOUBLE), export_at) AS classification_confidence,
    min(export_at) AS first_exported_at,
    max(export_at) AS last_exported_at
FROM {{ ref('stg_job_attributes') }}
WHERE job_tracking_id IS NOT NULL
GROUP BY job_tracking_id
