INSERT INTO dds.dm_couriers_rewards(id, courier_id, order_key, sum, rate, tip_sum)
WITH last_row AS (
    SELECT coalesce(MAX(id), 0) AS id FROM dds.dm_couriers_rewards
)
SELECT d.id,
       c.id courier_id,
       json_value->>'order_id' order_key,
       (json_value->>'sum')::numeric(10, 2) sum,
       (json_value->>'rate')::numeric(10, 2) rate,
       (json_value->>'tip_sum')::numeric(10, 2) tip_sum
FROM stg.api_deliveries as d
JOIN dds.dm_couriers c ON c.courier_id = d.json_value->>'courier_id'
CROSS JOIN last_row
WHERE d.id > last_row.id
ORDER BY d.id