SELECT
    job_tracking_id,
    role_family,
    role_specialization,
    seniority,
    workplace_type,
    remote_scope,
    posted_at,
    classification_confidence,
    CAST(export_at AS TIMESTAMP) AS export_at
FROM {{ source('bronze', 'bronze_job_attributes') }}
