INSERT INTO dds.dm_timestamps(id, ts, year, month, day, time, date)
WITH last_row AS (
    SELECT MAX(id) AS id FROM dds.dm_timestamps
)
SELECT o.id,
       ts,
       date_part('year', ts) as year,
       date_part('month', ts) as month,
       date_part('day', ts) as day,
       to_char(ts, 'HH24:MI:SS')::time as time,
       ts::date as date
FROM (SELECT id, (object_value::jsonb ->> 'date')::timestamp AS ts FROM stg.ordersystem_orders) o, last_row
WHERE o.id > last_row.id
ORDER BY o.id;
