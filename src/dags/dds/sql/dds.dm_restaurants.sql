INSERT INTO dds.dm_restaurants(id, restaurant_id, restaurant_name, active_from, active_to)
WITH last_row AS (
    SELECT MAX(id) AS id FROM dds.dm_restaurants
)
SELECT r.id,
       object_value::jsonb ->> '_id' AS restaurant_id,
       object_value::jsonb ->> 'name' AS restaurant_name,
       (object_value::jsonb ->> 'update_ts')::timestamp AS active_from,
       '2099-12-31'::date AS active_to
FROM stg.ordersystem_restaurants r, last_row
WHERE r.id > last_row.id
ORDER BY r.id;
