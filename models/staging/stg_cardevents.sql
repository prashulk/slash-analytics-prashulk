with source as (
    select * from {{ ref('card_events') }}
)
select 
    slash_account_id,
    card_event_id,
    card_id,
    event_type,
    card_status,
    timestamp,
    case 
        when card_event_id is null then 0
        else 1
    end as is_valid
from source
