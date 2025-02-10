WITH cleaned_entity AS (
    SELECT 
        entity_id, 
        subaccount_id, 
        account_creation_date
    FROM {{ ref('stg_entity') }}
    WHERE is_valid = 1
)
SELECT * FROM cleaned_entity
