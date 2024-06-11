-- SPXI
WITH order_info AS
(SELECT
        raw.id,
        DATE(FROM_UNIXTIME(raw.create_time - 3600)) AS created,
        raw.code,
        raw.cod_value/cast(100 as double) as cod_value,
        raw.item_value/cast(100 as double) as item_value,
        CASE 
        WHEN r.order_assign_type = 'Group' THEN 1
        WHEN r.order_assign_type != 'Group' AND r.group_id > 0 THEN 2
        ELSE 0 END AS assign_type,
        r.order_status,
        r.group_id,
        sum(cast(json_extract(z.item_name,'$.quantity') as bigint)) as quantity,
        (sum(cast(json_extract(z.item_name,'$.weight') as bigint))*sum(cast(json_extract(z.item_name,'$.quantity') as bigint))
        )/cast(1000 as decimal(20,3)) as weight_kg


FROM shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live raw

LEFT JOIN driver_ops_raw_order_tab r ON r.order_code = raw.code AND r.order_type = 6

CROSS JOIN UNNEST (CAST(JSON_EXTRACT(raw.extra_data,'$.items') AS ARRAY<JSON>)) AS z(item_name)

WHERE DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2024-04-01' AND DATE'2024-05-21'
AND r.order_status = 'Delivered'
GROUP BY 1,2,3,4,5,6,7,8
)
,s AS 
(SELECT
        created,
        group_id,
        MAX_BY(assign_type,id) AS assign_type,
        SUM(cod_value) AS cod_value,
        SUM(item_value) AS item_value,
        COUNT(DISTINCT id) AS cnt_order



FROM order_info
WHERE group_id > 0 
GROUP BY created,group_id
UNION ALL 
SELECT
        created,
        id AS group_id,
        assign_type,
        cod_value,
        item_value,
        1 AS cnt_order



FROM order_info
WHERE group_id = 0 
)
SELECT 
        DATE_TRUNC('month',created) AS month_,
        assign_type AS "0: SINGLE - 1: GROUP - 2: STACK",
        SUM(cnt_order) AS "total_order",
        SUM(cnt_order)/CAST(COUNT(DISTINCT created) AS DECIMAL(10,2)) AS "avg_order",
        APPROX_PERCENTILE(raw.cod_value,0.8) AS "cod_value 80th percentile",
        APPROX_PERCENTILE(raw.cod_value,0.9) AS "cod_value 90th percentile",
        APPROX_PERCENTILE(raw.cod_value,0.95) AS "cod_value 95th percentile",
        APPROX_PERCENTILE(raw.cod_value,0.98) AS "cod_value 98th percentile",
        APPROX_PERCENTILE(raw.cod_value,0.99) AS "cod_value 99th percentile",

        APPROX_PERCENTILE(raw.item_value,0.8) AS "item_value 80th percentile",
        APPROX_PERCENTILE(raw.item_value,0.9) AS "item_value 90th percentile",
        APPROX_PERCENTILE(raw.item_value,0.95) AS "item_value 95th percentile",
        APPROX_PERCENTILE(raw.item_value,0.98) AS "item_value 98th percentile",
        APPROX_PERCENTILE(raw.item_value,0.99) AS "item_value 99th percentile"


FROM s raw 
GROUP BY 1,2

/*
- 80/90/95/98/99 percentile COD stack & group
- 80/90/95/98/99 percentile COD Single
- 80/90/95/98/99 percentile total value stack & group
- 80/90/95/98/99 percentile total value single
*/ 
;
-- Delivery
WITH raw AS 
(SELECT 
        oct.order_code AS code,
        merchant_paid_status,
        CASE 
        WHEN oct.payment_method = 1 THEN 'Cash'
        WHEN oct.payment_method = 6 THEN 'ShopeePay'
        ELSE 'Others' END AS payment_method,
        CASE 
        WHEN oct.merchant_paid_method = 1 THEN 'Cash'
        WHEN oct.merchant_paid_method = 6 THEN 'ShopeePay'
        ELSE 'Others' END AS merchant_paid_method,
        CASE 
        WHEN r.order_assign_type = 'Group' THEN 1
        WHEN r.order_assign_type != 'Group' AND r.group_id > 0 THEN 2
        ELSE 0 END AS assign_type,
        paid_status,
        CAST(JSON_EXTRACT(oct.extra_data,'$.total_item') AS BIGINT) AS quantity,
        merchant_paid_amount/CAST(100 AS DECIMAL(10,2)) AS "paid_at_mex_value",
        total_amount/CAST(100 AS DECIMAL(10,2)) AS "collect_at_user_value",
        DATE(FROM_UNIXTIME(oct.submit_time - 3600)) AS created,
        r.group_id,
        oct.id

FROM shopeefood.shopeefood_mart_dwd_vn_order_completed_da oct

LEFT JOIN driver_ops_raw_order_tab r ON r.order_code = oct.order_code AND r.order_type = 0

WHERE date(dt) = current_date - interval '1' day
AND DATE(FROM_UNIXTIME(oct.submit_time - 3600)) BETWEEN DATE'2024-04-01' AND DATE'2024-05-21'
AND r.order_status = 'Delivered'
)
,s AS 
(SELECT
        created,
        group_id,
        MAX_BY(assign_type,id) AS assign_type,
        COALESCE(SUM(CASE WHEN payment_method = 'Cash' THEN raw.collect_at_user_value END),0) AS collect_at_user_value,
        COALESCE(SUM(CASE WHEN merchant_paid_method = 'Cash' THEN raw.paid_at_mex_value END),0) AS paid_at_mex_value,
        COUNT(DISTINCT id) AS cnt_order



FROM raw
WHERE group_id > 0 
GROUP BY created,group_id

UNION ALL 

SELECT
        created,
        id AS group_id,
        assign_type,
        COALESCE(SUM(CASE WHEN payment_method = 'Cash' THEN raw.collect_at_user_value END),0) AS collect_at_user_value,
        COALESCE(SUM(CASE WHEN merchant_paid_method = 'Cash' THEN raw.paid_at_mex_value END),0) AS paid_at_mex_value,
        COUNT(DISTINCT id) AS cnt_order



FROM raw
WHERE group_id = 0 
GROUP BY created,id,assign_type
)
SELECT 
        DATE_TRUNC('month',created) AS month_,
        assign_type AS "0: SINGLE - 1: GROUP - 2: STACK",
        SUM(cnt_order) AS "total_order",
        SUM(cnt_order)/CAST(COUNT(DISTINCT created) AS DECIMAL(10,2)) AS "avg_order",
        APPROX_PERCENTILE(raw.collect_at_user_value,0.8) AS "collect_at_user_value 80th percentile",
        APPROX_PERCENTILE(raw.collect_at_user_value,0.9) AS "collect_at_user_value 90th percentile",
        APPROX_PERCENTILE(raw.collect_at_user_value,0.95) AS "collect_at_user_value 95th percentile",
        APPROX_PERCENTILE(raw.collect_at_user_value,0.98) AS "collect_at_user_value 98th percentile",
        APPROX_PERCENTILE(raw.collect_at_user_value,0.99) AS "collect_at_user_value 99th percentile",

        APPROX_PERCENTILE(raw.paid_at_mex_value,0.8) AS "paid_at_mex_value 80th percentile",
        APPROX_PERCENTILE(raw.paid_at_mex_value,0.9) AS "paid_at_mex_value 90th percentile",
        APPROX_PERCENTILE(raw.paid_at_mex_value,0.95) AS "paid_at_mex_value 95th percentile",
        APPROX_PERCENTILE(raw.paid_at_mex_value,0.98) AS "paid_at_mex_value 98th percentile",
        APPROX_PERCENTILE(raw.paid_at_mex_value,0.99) AS "paid_at_mex_value 99th percentile"


FROM s AS raw 
GROUP BY 1,2