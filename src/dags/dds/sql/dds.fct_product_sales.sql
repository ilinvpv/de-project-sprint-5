INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
WITH last_row AS (
    SELECT coalesce(MAX(order_id), 0) AS order_id FROM dds.fct_product_sales
)
SELECT dp.id AS product_id,
       dor.id AS order_id,
       count,
       price,
       (price*count)::numeric(14,2) AS total_sum,
       bonus_payment,
       bonus_grant
FROM (SELECT ebr.order_id,
             jsonb_array_elements(ebr.product_payments) ->> 'product_id' as product_id,
             (jsonb_array_elements(ebr.product_payments) ->> 'price')::numeric(14,2)         as price,
             (jsonb_array_elements(ebr.product_payments) ->> 'quantity')::numeric(14,2)      as count,
             (jsonb_array_elements(ebr.product_payments) ->> 'bonus_payment')::numeric(14,2) as bonus_payment,
             (jsonb_array_elements(ebr.product_payments) ->> 'bonus_grant')::numeric(14,2)   as bonus_grant
      FROM stg.bonussystem_events be
        JOIN LATERAL jsonb_to_record(event_value::jsonb) AS ebr(
               user_id integer,
               order_id varchar,
               order_date timestamp,
               product_payments jsonb
        ) ON true
      WHERE event_type = 'bonus_transaction'
) bs
JOIN dds.dm_products dp ON dp.product_id = bs.product_id
JOIN dds.dm_orders dor ON dor.order_key = bs.order_id
CROSS JOIN last_row
WHERE dor.id > last_row.order_id
ORDER BY order_id;
