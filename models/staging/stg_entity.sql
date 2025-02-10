WITH entity_raw AS (
    SELECT 
        entity_id, 
        subaccount_id, 
        account_creation_date,
        CASE 
            WHEN entity_id IS NULL OR subaccount_id IS NULL OR account_creation_date IS NULL THEN 0 
            ELSE 1 
        END AS is_valid
    FROM {{ ref('entity') }}
)
SELECT * FROM entity_raw
