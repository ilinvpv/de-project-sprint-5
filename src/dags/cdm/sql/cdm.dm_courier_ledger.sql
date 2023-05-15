BEGIN;
TRUNCATE cdm.dm_courier_ledger;
INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, courier_tips_sum, order_processing_fee, courier_order_sum, courier_reward_sum)
WITH courier_summary AS (
SELECT dc.courier_id,
       dc.courier_name        as courier_name,
       dt.year                   settlement_year,
       dt.month                  settlement_month,
       COUNT(DISTINCT dmo.id) AS orders_count,
       SUM(fps.total_sum)     AS orders_total_sum,
       AVG(dcr.rate)          AS rate_avg,
       SUM(dcr.tip_sum)       AS courier_tips_sum
FROM dds.dm_orders dmo
       JOIN dds.dm_timestamps dt ON dt.id = dmo.timestamp_id
       JOIN dds.dm_couriers dc ON dc.id = dmo.courier_id
       JOIN dds.fct_product_sales fps ON dmo.id = fps.order_id
       JOIN dds.dm_couriers_rewards dcr ON dmo.courier_id = dcr.courier_id
WHERE dmo.order_status = 'CLOSED'
GROUP BY dc.courier_id, courier_name, settlement_year, settlement_month
)
SELECT courier_id,
       courier_name,
       settlement_year,
       settlement_month,
       orders_count,
       orders_total_sum,
       rate_avg,
       orders_total_sum * 0.25 AS order_processing_fee,
       CASE
           WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
           WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
           WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175)
           WHEN rate_avg >= 4.9 THEN GREATEST(orders_total_sum * 0.1, 200)
       END AS courier_order_sum,
       courier_tips_sum,
       courier_tips_sum * 0.95 + CASE
            WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100)
            WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150)
            WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175)
            WHEN rate_avg >= 4.9 THEN GREATEST(orders_total_sum * 0.1, 200)
       END AS courier_reward_sum
FROM courier_summary;
end;