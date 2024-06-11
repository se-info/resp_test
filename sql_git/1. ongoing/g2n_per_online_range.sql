WITH raw AS 
(SELECT 
        CASE 
        WHEN online_hour <= 1 THEN '1 .0 - 1'
        WHEN online_hour <= 2 THEN '2 .1 - 2'
        WHEN online_hour <= 3 THEN '3 .2 - 3'
        WHEN online_hour <= 4 THEN '4 .3 - 4'
        WHEN online_hour <= 5 THEN '5 .4 - 5'
        WHEN online_hour <= 6 THEN '6 .5 - 6'
        WHEN online_hour <= 7 THEN '7 .6 - 7'
        WHEN online_hour <= 8 THEN '8 .7 - 8'
        WHEN online_hour <= 9 THEN '9 .8 - 9'
        WHEN online_hour <= 10 THEN '10 .9 - 10'
        WHEN online_hour <= 11 THEN '11 .10 - 11'
        WHEN online_hour <= 12 THEN '12 .11 - 12'
        WHEN online_hour <= 13 THEN '13 .12 - 13'
        WHEN online_hour <= 14 THEN '14 .13 - 14'
        WHEN online_hour <= 24 THEN '15 .15 - 24'
        END AS online_range,
        online_hour,
        shipper_id,
        report_date,
        total_order_food

FROM driver_ops_driver_performance_tab

WHERE 1 = 1
AND city_id NOT IN (217,218)
AND regexp_like(coalesce(city_name,'n/a'),'Dien Bien|Test|test|stress|Stress') = false
AND total_order > 0 
AND online_hour <= 24
AND report_date BETWEEN CURRENT_DATE - INTERVAL '90' DAY AND CURRENT_DATE - INTERVAL '1' DAY
)
SELECT 
        online_range,
        COUNT(DISTINCT (shipper_id,report_date))/CAST(COUNT(DISTINCT report_date) AS DECIMAL(10,2)) AS avg_a1,
        SUM(total_order_food)/CAST(COUNT(DISTINCT report_date) AS DECIMAL(10,2)) AS avg_ado

FROM raw

GROUP BY 1
;
SELECT 
--        created_date, 
       COUNT(DISTINCT raw.order_code)/CAST(COUNT(DISTINCT raw.created_date ) AS DECIMAL(10,2)) AS gross_order,
       COUNT(DISTINCT case when order_status = 'Delivered' then raw.order_code else null end)/CAST(COUNT(DISTINCT raw.created_date ) AS DECIMAL(10,2)) AS gross_order,
       COUNT(DISTINCT case when order_status = 'Delivered' then raw.order_code else null end)/CAST(COUNT(DISTINCT raw.order_code ) AS DECIMAL(10,2)) AS G2N 
FROM driver_ops_raw_order_tab raw 
left join (select id,is_foody_delivery 
                    from shopeefood.shopeefood_mart_dwd_vn_order_completed_da where date(dt) = current_date - interval '1' day) oct 
                    on raw.id = oct.id
WHERE 1=1
AND raw.created_date BETWEEN CURRENT_DATE - INTERVAL '90' DAY AND CURRENT_DATE - INTERVAL '1' DAY
AND raw.order_type = 0
AND raw.city_id NOT IN (217,218)
AND regexp_like(coalesce(raw.city_name,'n/a'),'Dien Bien|Test|test|stress|Stress') = false
AND oct.is_foody_delivery = 1
-- GROUP BY 1 
;
SELECT 
        DATE_TRUNC('month',DATE(FROM_UNIXTIME(create_time - 3600))) AS created,
        COUNT(DISTINCT code)/CAST(COUNT(DISTINCT DATE(FROM_UNIXTIME(create_time - 3600))) AS DECIMAL(10,2)) AS gross_order,
        COUNT(DISTINCT CASE WHEN item_value > 0 THEN code ELSE NULL END)/CAST(COUNT(DISTINCT DATE(FROM_UNIXTIME(create_time - 3600))) AS DECIMAL(10,2)) AS gross_cod,
        COUNT(DISTINCT CASE WHEN item_value = 0 THEN code ELSE NULL END)/CAST(COUNT(DISTINCT DATE(FROM_UNIXTIME(create_time - 3600))) AS DECIMAL(10,2)) AS gross_non_cod


FROM shopeefood.foody_express_db__booking_tab__reg_daily_s0_live
WHERE DATE(FROM_UNIXTIME(create_time - 3600)) >= DATE'2023-03-01'
GROUP BY 1;
select report_date,sum(try_cast(deduct_amount as bigint )) 

from dev_vnfdbi_opsndrivers.shopeefood_driver_ops_compensation_list_snapshot 
where grass_date = date'2024-06-01'
group by 1
;


