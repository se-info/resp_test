with raw as 
(select 
        a.id,
        a.order_code,
        a.order_type,
        a.shipper_id,
        di.name_en as district_name,
        date(a.delivered_timestamp) as report_date

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab a 

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = a.district_id

where 1 = 1 
and a.shipper_id > 0 
and a.order_status in ('Delivered')
and date(a.delivered_timestamp) between date'2024-04-27' and date'2024-05-01'
and a.order_type = 6)
,district_filter as 
(select 
        shipper_id,
        count(distinct case when district_filter > 0 then report_date else null end) as total_day_qualified_district
from 
(select 
        report_date,
        shipper_id,
        cardinality(
        filter(array_agg(distinct raw.district_name),
               x -> x in('Hoang Mai','Ha Dong','Dong Da','Thanh Xuan','Nam Tu Liem','Thanh Tri',
                         'Go Vap','Tan Binh','Tan Phu','Binh Tan','District 12')) 
                         )as district_filter  

from raw 
group by 1,2)
group by 1 
)
,f as 
(select 
        *,
        case 
        when total_day_qualified_district >= 5  and working_days >= 5 and total_qualified_online >= 5 and total_qualified_sla >= 5 then 
        (case when total_order >= 70 then 350000 else 0 end)
        else 0 end as bonus_value

from
(select 
        raw.shipper_id,
        dp.total_qualified_online,
        dp.total_qualified_sla,
        dp.city_name,
        d.total_day_qualified_district,
        count(distinct order_code) as total_order,
        count(distinct raw.report_date) as working_days,
        array_agg(distinct raw.district_name) as district_list,
        cardinality(
        filter(array_agg(distinct raw.district_name),
               x -> x in('Hoang Mai','Ha Dong','Dong Da','Thanh Xuan','Nam Tu Liem','Thanh Tri',
                         'Go Vap','Tan Binh','Tan Phu','Binh Tan','District 12')) 
                         )as district_filter  

from raw 

left join 
(select 
        shipper_id,
        max_by(city_name,report_date) as city_name,
        count(distinct case when greatest(online_hour,work_hour) >= 5 then report_date else null end) as total_qualified_online,
        count(distinct case when sla_rate >= 95 then report_date else null end) as total_qualified_sla

from driver_ops_driver_performance_tab
where report_date between date'2024-04-27' and date'2024-05-01'
and total_order_spxi > 0 
group by 1 
) dp on dp.shipper_id = raw.shipper_id 

left join district_filter d on d.shipper_id = raw.shipper_id

inner join vnfdbi_opsndrivers.phong_test_table p on cast(p.shipper_id as bigint) = raw.shipper_id

group by 1,2,3,4,5 
)
)
select sum(bonus_value)

from f 