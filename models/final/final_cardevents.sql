with staging as (
    select * from {{ ref('stg_cardevents') }}
)
select
    slash_account_id,
    card_event_id,
    card_id,
    case 
        when event_type like 'pending%' then
            replace(event_type, 'pending_', '')
        else
            event_type
    end as event_type,
    case
        when card_status like 'pending%' then
            REPLACE(card_status, 'pending_', '')
        ELSE
            card_status
    end as card_status,
    timestamp
from staging
where is_valid = 1
