BEGIN;
TRUNCATE cdm.dm_settlement_report;
INSERT INTO cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
SELECT
    o.restaurant_id,
    dr.restaurant_name,
    dt.date::date AS settlement_date,
    count(DISTINCT o.id) AS orders_count,
    sum(fps.total_sum) AS orders_total_sum,
    sum(fps.bonus_payment) AS orders_bonus_payment_sum,
    sum(fps.bonus_grant) AS orders_bonus_granted_sum,
    sum(fps.total_sum * 0.25) AS order_processing_fee,
    sum(fps.total_sum - fps.bonus_payment - fps.total_sum * 0.25) AS restaurant_reward_sum
FROM dds.dm_orders o
JOIN dds.dm_restaurants dr ON o.restaurant_id = dr.id
JOIN dds.fct_product_sales fps ON o.id = fps.order_id
JOIN dds.dm_timestamps dt ON dt.id = o.timestamp_id
WHERE o.order_status = 'CLOSED'
GROUP BY o.restaurant_id, restaurant_name, settlement_date
ORDER BY settlement_date;
END;