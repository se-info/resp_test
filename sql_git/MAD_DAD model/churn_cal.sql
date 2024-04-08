WITH agg_delivered_date_tab AS 
(SELECT 
        uid as shipper_id,
        MIN(date(from_unixtime(real_drop_time - 3600))) AS first_date ,
        ARRAY_AGG(DISTINCT date(from_unixtime(real_drop_time - 3600)) ) AS agg_delivered_date

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day)
WHERE 1 = 1
AND order_status = 400
GROUP BY 1 
)
,summary AS
(SELECT
        try_cast(sm.grass_date as date) as report_date,
        YEAR(TRY_CAST(sm.grass_date as date)) * 100 +  MONTH(TRY_CAST(sm.grass_date as date)) AS year_month,
        sm.shipper_id,
        COALESCE(tot.city_name,sm.city_name) AS working_city,
        case 
        when YEAR(date(from_unixtime(new.create_time - 3600)))*100 + MONTH(date(from_unixtime(new.create_time - 3600))) = YEAR(TRY_CAST(sm.grass_date as date)) * 100 +  MONTH(TRY_CAST(sm.grass_date as date)) then 'new onboard'
        else null end as onboard_segment,
        COALESCE(tot.total_order,0) AS total_order,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date,x -> x = TRY_CAST(sm.grass_date AS DATE))),0) AS is_a1,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date,x -> x between TRY_CAST(sm.grass_date AS DATE) - INTERVAL '29' DAY AND TRY_CAST(sm.grass_date AS DATE))),0) AS is_a30,
        COALESCE(cardinality(filter(agg.agg_delivered_date, x -> date_trunc('month',x)  = date_trunc('month',try_cast(sm.grass_date as date)))),0) as current_month,
        COALESCE(cardinality(filter(agg.agg_delivered_date, x -> date_trunc('month',x)  = date_trunc('month',try_cast(sm.grass_date as date)) - interval '1' month )),0) as previous_month   



FROM shopeefood.foody_mart__profile_shipper_master sm 

LEFT JOIN 
(SELECT 
        shipper_id,
        report_date,
        total_order,
        c.name_en as city_name

FROM (
select dot.uid as shipper_id,
        date(from_unixtime(dot.real_drop_time - 3600)) AS report_date,
        count(distinct dot.order_code) as total_order, 
        min_by(pick_city_id,real_drop_time) first_city
from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da dot
where date(dt) = current_date - interval '1' day
AND order_status = 400
group by 1,2 
) dot
LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live c on c.id = dot.first_city
WHERE 1 = 1
) tot 
    on tot.shipper_id = sm.shipper_id
    and tot.report_date = try_cast(sm.grass_date as date)

LEFT JOIN agg_delivered_date_tab agg 
    on agg.shipper_id = sm.shipper_id

LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live new 
    on new.uid = sm.shipper_id 

where sm.grass_date != 'current'
)
-- select * from summary where report_date between date'2023-10-01' and date'2023-10-31'
select
        year_month,
        case 
        when working_city in ('HCM City','Ha Noi City','Da Nang City') then working_city
        else 'OTH' end as city_group,      
        COUNT(distinct case when total_order > 0 then shipper_id else null end) as unique_transacting,
        COUNT(distinct case when onboard_segment is not null then shipper_id else null end) as new_onboard,
        COUNT(distinct case when onboard_segment is not null and total_order > 0 then shipper_id else null end) as new_onboard_active,
        COUNT(distinct case when current_month = 0 and previous_month > 0 then shipper_id else null end) as num_of_churn,
        COUNT(distinct case when current_month > 0 and previous_month = 0 and onboard_segment is null then shipper_id else null end) as num_of_reactivate

from summary

WHERE regexp_like(lower(working_city),'dien bien|test|stress|an giang|lao cai') = false 
and report_date between date'2023-09-01' and date'2023-10-31'
GROUP BY 1,2
