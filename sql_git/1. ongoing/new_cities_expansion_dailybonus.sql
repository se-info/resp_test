with tier_tab(from_point,to_point,tier,ver) AS
(VALUES
(0,2599,'T1','v1'),
(2600,99999,'T2','v1'),
(0,2339,'T1','v2'),
(2340,99999,'T2','v2')
)
,bonus_tab(from_,to_,bonus,ver) AS 
(VALUES
(0,99,0,'v1'),
(100,169,15000,'v1'),
(170,9999,30000,'v1'),
(0,89,0,'v2'),
(90,139,15000,'v2'),
(140,9999,30000,'v2')
)
,raw as 
(select 
        a.id,
        a.order_code,
        a.group_id,
        a.order_type,
        case when a.city_id = dp.city_id then p.point
        else 0 end as original_point,
        a.shipper_id,
        date(a.delivered_timestamp) as report_date,
        case when a.city_id = dp.city_id then 10
        else 0 end as original_points,
        a.city_id as order_city,
        dp.city_id as driver_city

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab a 

left join driver_ops_driver_performance_tab dp 
        on dp.shipper_id = a.shipper_id
        and dp.report_date = date(a.delivered_timestamp)

left join shopeefood.foody_partner_db__order_point_log_tab__reg_daily_s0_live p 
        on p.order_id = a.id
        and p.order_type = a.order_type
where 1 = 1 
and a.shipper_id > 0 
and a.order_status in ('Delivered','Quit')

)
,f as 
(select 
        raw.report_date,
        raw.shipper_id,
        dp.city_id,
        dp.shipper_tier,
        dp.daily_point,
        dp.sla_rate,
        count(distinct raw.order_code) as ado,
        sum(raw.original_point) as actual_earned_point,
        sum(raw.original_points) as point_v1


from raw 

left join driver_ops_driver_performance_tab dp 
        on dp.shipper_id = raw.shipper_id
        and dp.report_date = raw.report_date

where raw.report_date between date_trunc('month',current_date) - interval '3' month and current_date - interval '1' day
and dp.shipper_tier != 'Hub'
and dp.city_name in ('Kien Giang','Dong Thap','Phu Yen')
group by 1,2,3,4,5,6
)
,s as 
(select 
        f.report_date,
        f.shipper_id,
        f.shipper_tier as actual_tier,
        f.actual_earned_point,
        f.city_id, 
        f.ado,
        sum(f2.actual_earned_point) as actual_l30d_point

from f 

left join f as f2 
        on f2.shipper_id = f.shipper_id 
        and f2.report_date between f.report_date - interval '30' day and f.report_date - interval '1' day

where f.report_date >= date'2024-08-01'

group by 1,2,3,4,5,6
)
,summary as 
(select 
        s.*,
        case when v1_tier = 'T1' then 0 else b1.bonus end as v1_bonus,
        case when v2_tier = 'T1' then 0 else b2.bonus end as v2_bonus

from 
(select 
        s.*,
        v1.tier as v1_tier,
        v2.tier as v2_tier

from s 

left join tier_tab v1 on v1.ver = 'v1' and s.actual_l30d_point between v1.from_point and v1.to_point

left join tier_tab v2 on v2.ver = 'v2' and s.actual_l30d_point between v2.from_point and v2.to_point
where actual_l30d_point is not null 
) s 

left join bonus_tab b1 on b1.ver = 'v1' and s.actual_earned_point between b1.from_ and b1.to_ 

left join bonus_tab b2 on b2.ver = 'v2' and s.actual_earned_point between b2.from_ and b2.to_ 
)
select 
        date_trunc('month',s.report_date) as "month",
        b.name_en as city_name,
        v1_bonus as bonus_threshold,
        'ver1' as "version",
        count(distinct (shipper_id,report_date))*1.00/count(distinct report_date) as a1,
        sum(v1_bonus)*1.00/count(distinct report_date) as bonus,
        sum(ado)*1.00/count(distinct report_date) as ado


from summary s 

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live b 
    on b.id = s.city_id
    and b.country_id = 86

group by 1,2,3,4
UNION ALL

select 
        date_trunc('month',s.report_date) as "month",
        b.name_en as city_name,
        v2_bonus as bonus_threshold,
        'ver2' as "version",
        count(distinct (shipper_id,report_date))*1.00/count(distinct report_date) as a1,
        sum(v2_bonus)*1.00/count(distinct report_date) as bonus,
        sum(ado)*1.00/count(distinct report_date) as ado


from summary s 

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live b 
    on b.id = s.city_id
    and b.country_id = 86
group by 1,2,3,4
