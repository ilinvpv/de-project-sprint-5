WITH last_row AS (
    SELECT coalesce(MAX(id), 0) AS id FROM dds.dm_couriers
)
INSERT INTO dds.dm_couriers(id, courier_id, courier_name)
SELECT c.id, json_value->> '_id' courier_id, json_value->> 'name' courier_name
FROM stg.api_couriers c, last_row
WHERE c.id > last_row.id
ORDER BY id
