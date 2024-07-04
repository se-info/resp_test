with raw as
(SELECT 
         oct.restaurant_id as merchant_id 
        ,mm.merchant_name
        ,mm.city_name
        ,DATE(FROM_UNIXTIME(oct.submit_time - 3600)) as created_date
        ,HOUR(FROM_UNIXTIME(oct.submit_time - 3600)) as created_hour
        ,CASE 
             WHEN COUNT(DISTINCT oct.id) > 30 then '1. over 30'
             WHEN COUNT(DISTINCT oct.id) <= 30 then '2. 21 - 30'
             WHEN COUNT(DISTINCT oct.id) <= 20 then '3. 16 - 20'
             WHEN COUNT(DISTINCT oct.id) <= 15 then '4. 11 - 15'
             WHEN COUNT(DISTINCT oct.id) <= 10 then '5. 6 - 10'
             WHEN COUNT(DISTINCT oct.id) <= 5 then '6. <= 5'
             END AS order_range                   
        ,COUNT(DISTINCT oct.id) as total_ado



FROM shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct

LEFT JOIN shopeefood.foody_mart__profile_merchant_master mm on mm.merchant_id = oct.restaurant_id and mm.grass_date = 'current'



WHERE 1 = 1 
AND oct.status = 7
-- AND DATE(FROM_UNIXTIME(oct.submit_time - 3600)) BETWEEN ${start_date} AND ${end_date}
AND DATE(FROM_UNIXTIME(oct.submit_time - 3600)) between current_date - interval '1' day and current_date  - interval '1' day
GROUP BY 1,2,3,4,5
)
SELECT 
        merchant_id
       ,merchant_name
       ,city_name
       ,SUM(total_ado)/CAST(COUNT(DISTINCT created_date) as DOUBLE) as avg_ado_per_day
       ,SUM(total_ado)/CAST(COUNT(created_hour) as DOUBLE) as avg_ado_per_day
       ,SUM( CASE WHEN order_range = '1. over 30' then total_ado else null end) as over_30
       ,SUM( CASE WHEN order_range = '2. 21 - 30' then total_ado else null end) as r21_30
       ,SUM( CASE WHEN order_range = '3. 16 - 20' then total_ado else null end) as r16_20   
       ,SUM( CASE WHEN order_range = '4. 11 - 15' then total_ado else null end) as r11_15
       ,SUM( CASE WHEN order_range = '5. 6 - 10' then total_ado else null end) as r6_10
       ,SUM( CASE WHEN order_range = '6. <= 5' then total_ado else null end) as less_equal_5


FROM raw 


GROUP BY 1,2,3
LIMIT 100 