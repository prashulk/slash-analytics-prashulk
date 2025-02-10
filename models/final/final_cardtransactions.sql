with cleaned_data as (
    select *
    from {{ ref('stg_cardtransactions') }}
    where is_valid = 1
),
final_data as (
select 
    TRANSACTION_ID,
    SUBACCOUNT_ID,
    CARD_ID,
    ACCOUNT_TYPE,
    TRANSACTION_AMOUNT,
    case 
        when ORIGINAL_CURRENCY_CODE = 'ILR' then 'ILS'
        when ORIGINAL_CURRENCY_CODE = 'ARY' then 'ARS'
        when ORIGINAL_CURRENCY_CODE = 'MXP' then 'MXN'
        when ORIGINAL_CURRENCY_CODE = 'CSJ' then 'CZK'
        when ORIGINAL_CURRENCY_CODE = 'VNC' then 'VND'
        when ORIGINAL_CURRENCY_CODE = 'PES' then 'PEN'
        else ORIGINAL_CURRENCY_CODE
    end as ORIGINAL_CURRENCY_CODE,
    MERCHANT_DESCRIPTION,
    MERCHANT_CATEGORY_CODE,
    MERCHANT_COUNTRY,
    TIMESTAMP,
    CAST(TIMESTAMP AS DATE) as exchange_rt_dt,
from cleaned_data
where TRANSACTION_AMOUNT is not null)
select fd.*,fd.TRANSACTION_AMOUNT/ fc.exchange_rate as final_amount
from final_data fd left join {{ ref('final_currencyconv') }} fc
on fd.ORIGINAL_CURRENCY_CODE = fc.destination_currency and fd.exchange_rt_dt = fc.exchange_rt_dt
