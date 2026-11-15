-- Bronze lifecycle events with explicit columns and types: the contract the
-- silver models rely on, whatever extra columns bronze carries.
SELECT
    event_id,
    run_id,
    CAST(event_at AS TIMESTAMP) AS event_at,
    CAST(collection_date AS DATE) AS collection_date,
    event_type,
    job_tracking_id,
    open_reason,
    collector,
    company_key,
    company,
    title,
    link,
    changed_fields,
    field_changed,
    CAST(missing_count AS BIGINT) AS missing_count,
    CAST(hours_missing AS DOUBLE) AS hours_missing
FROM {{ source('bronze', 'bronze_events') }}
