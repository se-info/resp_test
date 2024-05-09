with raw as 
(select 
        a.id,
        a.order_code,
        a.order_type,
        a.shipper_id,
        di.name_en as district_name,
        date(a.delivered_timestamp) as report_date,
        row_number()over(partition by a.shipper_id order by a.id asc) as rank_

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab a 

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = a.district_id

where 1 = 1 
and a.shipper_id > 0 
and a.order_status in ('Delivered')
and date(a.delivered_timestamp) = date'2024-05-06'
and a.order_type = 6
)
,eligible_driver as 
(select 
        raw.report_date,
        raw.shipper_id,
        dp.completed_rate*1.00/100 as sla_rate,
        greatest(dp.total_online_seconds,dp.total_work_seconds)*1.00/3600 as online_hour,
        count(distinct order_code) as total_order,
        array_agg(distinct raw.district_name) as district_list,
        cardinality(
        filter(array_agg(distinct raw.district_name),
               x -> x in('Hoang Mai','Ha Dong','Dong Da','Thanh Xuan','Nam Tu Liem','Thanh Tri',
                         'Go Vap','Tan Binh','Tan Phu','Binh Tan','District 12')) 
                         )as district_filter  

from raw 

left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live dp 
        on date(from_unixtime(dp.report_date - 3600)) = raw.report_date 
        and dp.uid = raw.shipper_id

inner join dev_vnfdbi_opsndrivers.spxi_remove_food_scheme p on cast(p.shipper_id as bigint) = raw.shipper_id

group by 1,2,3,4
)
,f as 
(select 
        sm.shipper_name,
        sm.city_name,
        ed.*,
        case 
        when ed.online_hour >= 8 and ed.sla_rate >= 95 and district_filter > 0 then 180000 else 0 end as bonus_value


from eligible_driver ed 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = ed.shipper_id and sm.grass_date = 'current'
-- where ed.online_hour >= 8 and ed.sla_rate >= 95 and district_filter > 0 and ed.total_order >= 25
)
select
        t2.*,
        t1.id,
        t1.rank_,
        t2.bonus_value*1.00/20 as bonus_
from raw t1 
inner join f t2 on t1.shipper_id = t2.shipper_id

where 1 = 1 
and t1.rank_ <= 20
