INSERT INTO dds.dm_users(id, user_id, user_name, user_login)
WITH last_row AS (
    SELECT MAX(id) AS id FROM dds.dm_users
)
SELECT ou.id,
       object_value::jsonb ->> '_id' AS user_id,
       object_value::jsonb ->> 'name' AS user_name,
       object_value::jsonb ->> 'login' AS user_login
FROM stg.ordersystem_users ou, last_row
WHERE ou.id > last_row.id
ORDER BY ou.id;
