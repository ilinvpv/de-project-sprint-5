INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
WITH last_row AS (
    SELECT MAX(product_id) AS product_id FROM dds.dm_products
)
SELECT o.product_id, product_name, product_price, active_from, active_to, restaurant_id
FROM (SELECT jsonb_array_elements(obr.menu) ->> '_id'   AS product_id,
             jsonb_array_elements(obr.menu) ->> 'name'  AS product_name,
             (jsonb_array_elements(obr.menu) ->> 'price')::numeric(10, 2) AS product_price,
             obr.update_ts                              AS active_from,
             '2099-12-31'::date                         AS active_to,
             id                                         as restaurant_id
      FROM stg.ordersystem_restaurants
               JOIN LATERAL jsonb_to_record(object_value::jsonb) AS obr(update_ts timestamp, menu jsonb) ON true) o, last_row
WHERE o.product_id > last_row.product_id
ORDER BY o.product_id;