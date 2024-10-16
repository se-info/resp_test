with raw as 
(SELECT 
        CASE 
        WHEN delivered_timestamp IS NOT NULL THEN DATE(delivered_timestamp)
        ELSE created_date END AS report_date,
        CASE 
        WHEN city_name IN ('HCM City', 'Ha Noi City', 'Da Nang City') THEN city_name
        WHEN city_name IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') THEN 'T2'
        ELSE 'T3' END AS city_group,
        id AS order_id,
        CASE 
        WHEN distance <= 1 THEN '1. 0-1km'
        WHEN distance <= 2 THEN '2. 1-2km'
        WHEN distance <= 3 THEN '3. 2-3km'
        WHEN distance <= 4 THEN '4. 3-4km'
        WHEN distance <= 5 THEN '5. 4-5km'
        WHEN distance > 5 THEN '6. ++5km'
        END AS distance_range,
        DATE_DIFF('second',first_auto_assign_timestamp,last_incharge_timestamp)*1.00/60 AS incharge_time,
        DATE_DIFF('second',created_timestamp,delivered_timestamp)*1.00/60 AS completion_time,
        case 
        when DATE_DIFF('second',created_timestamp,delivered_timestamp)*1.00/60 <= 20 then '1. <= 20m'
        when DATE_DIFF('second',created_timestamp,delivered_timestamp)*1.00/60 <= 30 then '2. 20 - 30m'
        when DATE_DIFF('second',created_timestamp,delivered_timestamp)*1.00/60 <= 40 then '3. 30 - 40m'
        when DATE_DIFF('second',created_timestamp,delivered_timestamp)*1.00/60 > 40 then '4. ++40m'
        end as ata_range

FROM (select raw.*,if(raw.order_type != 0,1,coalesce(is_foody_delivery,0)) as filter_delivery
from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 
left join (select id,is_foody_delivery 
           from shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
           where date(dt) = current_date - interval '1' day) oct 
                on raw.id = oct.id)

WHERE 1=1 
and order_status = 'Delivered'
AND order_type = 0 
AND is_asap = 1 
AND filter_delivery = 1 
AND DATE(delivered_timestamp) between date'2024-09-23' and current_date - interval '1' day
)
select 
        report_date,
        coalesce(city_group,'VN') as cities,
        -- distance_range,
        coalesce(ata_range,'All') as ata_range,
        avg(completion_time) as avg_completion_time,
        avg(incharge_time) as avg_incharge_time,
        count(distinct order_id) as cnt_order,
        approx_percentile(completion_time,0.95) as pct_95th_e2e


from raw 

group by 1,grouping sets(city_group,ata_range,(city_group,ata_range),())


