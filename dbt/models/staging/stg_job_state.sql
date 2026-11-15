SELECT
    job_tracking_id,
    status,
    open_reason,
    CAST(first_seen_at AS TIMESTAMP) AS first_seen_at,
    CAST(last_seen_at AS TIMESTAMP) AS last_seen_at,
    CAST(first_missed_at AS TIMESTAMP) AS first_missed_at,
    CAST(closed_at AS TIMESTAMP) AS closed_at,
    collector,
    company_key,
    company,
    title,
    job_category
FROM {{ source('bronze', 'bronze_job_state') }}
