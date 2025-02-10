with source as (
    select *,
        row_number() over (partition by TRANSACTION_ID order by 
            case when MERCHANT_CATEGORY_CODE is not null then 1 else 2 end, TRANSACTION_ID) as row_num
    from {{ ref('card_transactions') }}
)
select 
    TRANSACTION_ID,
    SUBACCOUNT_ID,
    CARD_ID,
    ACCOUNT_TYPE,
    TRANSACTION_AMOUNT,
    ORIGINAL_CURRENCY_CODE,
    MERCHANT_DESCRIPTION,
    MERCHANT_CATEGORY_CODE,
    MERCHANT_COUNTRY,
    TIMESTAMP,
    case 
        when TRANSACTION_ID is null then 0
        when row_num = 1 then 1         
        else 0
    end as is_valid
from source
