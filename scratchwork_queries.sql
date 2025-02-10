

select * from card_events c, entity e where c."SLASH_ACCOUNT_ID" = e."SUBACCOUNT_ID"

select * from card_events limit 100
-- group by 1 having count(*) > 1

select "TRANSACTION_ID", count(*) from card_transactions
group by 1 having count(*) > 1

select * from card_transactions limit 100
-- where "TRANSACTION_ID" = 'te_card_2kx8vgfp1z014'

--entity
select * from entity where "SUBACCOUNT_ID" is NULL

select SUBACCOUNT_ID, count(*) from entity
group by 1 having count(*) > 1

select * from entity where "ACCOUNT_CREATION_DATE" > CURRENT_DATE


WITH entity_raw AS (
    SELECT 
        entity_id, 
        subaccount_id, 
        account_creation_date,
        CASE 
            WHEN entity_id IS NULL OR subaccount_id IS NULL OR account_creation_date IS NULL THEN 0 ELSE 1 END AS is_valid
    FROM entity 
)
SELECT * FROM entity_raw;


--card_events
select * from card_events limit 100
-- where "card_status" is NULL

select distinct("EVENT_TYPE") from card_events -- event_type transf reqd

select distinct("CARD_STATUS") from card_events limit 100 -- card_status transf reqd

select * from card_events
where "CARD_STATUS" is null limit 100

select "SLASH_ACCOUNT_ID", count(*) from card_events
group by 1 order by 2 desc

select "CARD_ID", count(*) from card_events
group by 1 order by 2 desc

select "CARD_EVENT_ID", count(*) from card_events
group by 1 having count(*) > 1

select count(*) from entity

select distinct(event_type) from final_cardevents

select distinct(card_status) from final_cardevents

--cardtransactions
select "TRANSACTION_ID", count(*) from card_transactions
group by 1 order by 2 desc

select distinct("ORIGINAL_CURRENCY_CODE") from card_transactions

select distinct(extract(year from timestamp))  from card_transactions LIMIT 100
-- where "ORIGINAL_CURRENCY_CODE" IS NULL

select *  from card_transactions limit 5
--  where "TRANSACTION_AMOUNT" is null

select count(*) from (select distinct * from card_transactions)

select count(distinct "TRANSACTION_ID") from card_transactions

select (transaction_id, timestamp), count(*) from card_transactions
group by 1 order by 2 desc

with base as (select distinct * from card_transactions)
select * from base where transaction_id in (
select "TRANSACTION_ID" from base
group by 1 having count(*) > 1) and "MERCHANT_CATEGORY_CODE" is not null --helped me decide that there were some issues in merchant cat code as well and we can now clearly say that t_id has to be unique

with source as (
    select *,
        row_number() over (partition by TRANSACTION_ID order by 
            case when MERCHANT_CATEGORY_CODE is not null then 1 else 2 end, TRANSACTION_ID) as row_num
    from card_transactions
), base2 as (
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
    case when row_num = 1 then 1 else 0 end as is_valid
from source)
select count(*) from base2 where is_valid = 1


select *, cast(conversion_dt as date) from currency_conv where destination_currency = 'MXN'
--  where "ORIGINAL_CURRENCY_CODE" = 'PEN'

select * from final_currencyconv where destination_currency = 'USD' and source_currency = 'USD'

WITH base AS (
    SELECT 
        fc.*, fc."TRANSACTION_AMOUNT"/ c.exchange_rate as final_v
    FROM final_cardtransactions fc
    LEFT JOIN final_currencyconv c 
        ON fc."ORIGINAL_CURRENCY_CODE" = c.destination_currency 
,        AND fc.exchange_rt_dt = c.exchange_rt_dt
)
SELECT * FROM base WHERE "ORIGINAL_CURRENCY_CODE" = 'PKR'
--  = 'USD';


select * from final_cardevents limit 100

select * from entity

select "SUBACCOUNT_ID", count(DISTINCT "CARD_ID") from final_cardtransactions
group by 1


select card_id, count(*) from final_cardtransactions
group by 1 having count(*) > 1

select fc."TIMESTAMP",fe."ACCOUNT_CREATION_DATE" from final_cardtransactions fc, final_entity fe 
where fc."SUBACCOUNT_ID" = fe."SUBACCOUNT_ID"

select min("TIMESTAMP"), max("TIMESTAMP") from final_cardtransactions

select * from final_cardtransactions

select "ENTITY_ID", count(DISTINCT "SUBACCOUNT_ID") from final_entity
group by 1

SELECT e.entity_id, COUNT(distinct t.subaccount_id) 
FROM final_cardtransactions t
JOIN final_entity e ON t.subaccount_id = e.subaccount_id
GROUP BY e.entity_id
order by 2 desc;


SELECT "ENTITY_ID", COUNT(DISTINCT "SUBACCOUNT_ID") 
FROM final_entity 
GROUP BY "ENTITY_ID"
HAVING COUNT(DISTINCT "SUBACCOUNT_ID")  > 1;

select
    e.entity_id,
    CONCAT(LEFT(MONTHNAME(TIMESTAMP), 3), '-', YEAR(TIMESTAMP)) AS transaction_mmyy
    YEAR("TIMESTAMP") as transaction_year,
    MONTH("TIMESTAMP") as transaction_month,
    SUM("final_amount") *-1.0 AS sum_amount
from final_cardtransactions t join final_entity e on t."SUBACCOUNT_ID" = e."SUBACCOUNT_ID"
group by entity_id, transaction_mmyy, transaction_year, transaction_month
order by 1, 3, 4


SELECT e.entity_id,CONCAT(LEFT(MONTHNAME(TIMESTAMP), 3), '-', YEAR(TIMESTAMP)) AS transaction_mmyy,
 SUM("final_amount") *-1.0 AS sum_amount
FROM final_cardtransactions t
JOIN final_entity e ON t.subaccount_id = e.subaccount_id
GROUP BY 1,2
order by 3

SELECT 
    e.entity_id,
    CONCAT(LEFT(MONTHNAME(t.TIMESTAMP), 3), '-', YEAR(t.TIMESTAMP)) AS transaction_mmyy,
    YEAR(t.TIMESTAMP) AS transaction_year,
    MONTH(t.TIMESTAMP) AS transaction_month,
    SUM(t.final_amount) * -1.0 AS sum_amount
FROM 
    final_cardtransactions t
JOIN 
    final_entity e ON t.subaccount_id = e.subaccount_id
GROUP BY 
    e.entity_id, transaction_mmyy, transaction_year, transaction_month
ORDER BY 
    5;

SELECT subaccount_id, COUNT(*) 
FROM final_cardtransactions
GROUP BY subaccount_id;


WITH merchant_customer_transactions AS (
    SELECT 
        t.MERCHANT_DESCRIPTION, 
        t.SUBACCOUNT_ID,
        COUNT(*) AS transaction_count
    FROM final_cardtransactions t
    GROUP BY t.MERCHANT_DESCRIPTION, t.SUBACCOUNT_ID
)
SELECT 
    MERCHANT_DESCRIPTION,
    sum(CASE WHEN transaction_count > 1 THEN 1 else  0 END) AS retained_customers,
    COUNT(DISTINCT SUBACCOUNT_ID) AS total_customers,
    -- ROUND((COUNT(CASE WHEN transaction_count > 1 THEN 1 END) / 
    --        COUNT(DISTINCT SUBACCOUNT_ID)) * 100, 2) AS retention_percentage
FROM merchant_customer_transactions
GROUP BY MERCHANT_DESCRIPTION;
-- ORDER BY retention_percentage desc;
select * from final_cardtransactions where "MERCHANT_DESCRIPTION" = 'FOX THEATRE, INC.'

SELECT 
t.MERCHANT_DESCRIPTION, 
t.SUBACCOUNT_ID,
COUNT(*) AS transaction_count
FROM final_cardtransactions t
GROUP BY t.MERCHANT_DESCRIPTION, t.SUBACCOUNT_ID;

select distinct "MERCHANT_CATEGORY_CODE" from final_cardtransactions

select distinct "MERCHANT_DESCRIPTION" from final_cardtransactions
where "MERCHANT_CATEGORY_CODE" = '7311'

select * from final_cardevents
