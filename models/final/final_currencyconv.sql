select 
 cast(conversion_dt as date) exchange_rt_dt,source_currency,destination_currency,exchange_rate
 from {{ ref('currency_conv') }}
