INSERT INTO dds.dm_orders(id, order_key, order_status, timestamp_id, user_id, restaurant_id, courier_id)
WITH last_row AS (
    SELECT coalesce(MAX(id), 0) AS id FROM dds.dm_orders
)
SELECT oo.id AS id,
       object_id AS order_key,
       jbr.final_status AS order_status,
       dt.id AS timestamp_id,
       du.id AS user_id,
       dr.id AS restaurant_id,
       cr.courier_id
FROM stg.ordersystem_orders oo
         JOIN LATERAL jsonb_to_record(object_value::jsonb) AS jbr(
    date text,
    final_status text,
    "user" jsonb,
    restaurant jsonb
) ON true
    JOIN dds.dm_timestamps dt ON jbr.date::timestamp = dt.ts
    JOIN dds.dm_users du ON jbr.user->>'id' = du.user_id
    JOIN dds.dm_restaurants dr ON jbr.restaurant->>'id' = dr.restaurant_id
    JOIN dds.dm_couriers_rewards cr ON oo.object_id = cr.order_key
    CROSS JOIN last_row
WHERE oo.id > last_row.id;
