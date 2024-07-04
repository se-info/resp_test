with raw as 
(select 
        dot.uid as shipper_id 
       ,dot.ref_order_code 
       ,from_unixtime(dot.real_drop_time - 3600) as last_delivered_timestamp
       ,date(from_unixtime(dot.real_drop_time - 3600)) as last_delivered_date       
       ,row_number()over(partition by dot.uid order by from_unixtime(dot.real_drop_time - 3600) asc) as rank   

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

WHERE 1 = 1 
AND dot.order_status = 400 
AND date(from_unixtime(dot.real_drop_time - 3600)) between date'2023-01-22' and date'2023-01-27'
)
,summary as 
(select 
        raw.last_delivered_date
       ,raw.shipper_id
       ,ROUND(sla.completed_rate/cast(100 as double),2) as sla_rate
       ,count(distinct ref_order_code) as total_order  

from raw 
left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live sla on sla.uid = raw.shipper_id and date(from_unixtime(sla.report_date - 3600)) = raw.last_delivered_date

group by 1,2,3
order by 1
)
select 
        raw.shipper_id
       ,spp.shopee_uid
       ,sm.shipper_name
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non hub' end as driver_type  
       ,sm.city_name
       ,case when sm.shipper_status_code = 1 then 'Normal' else 'Other' end as working_status 
       ,spp.main_phone
       ,raw.ref_order_code
       ,raw.last_delivered_timestamp
       ,raw.last_delivered_date
       ,row_number()over(order by last_delivered_timestamp asc) as rank_driver
       ,map_agg(cast(s.last_delivered_date as varchar),cast(s.total_order as varchar)) as order_ext 
       ,map_agg(cast(s.last_delivered_date as varchar),s.sla_rate) as sla_ext

from raw 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.shipper_id and try_cast(sm.grass_date as date) = raw.last_delivered_date

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp on spp.uid = raw.shipper_id

left join summary s on s.shipper_id = raw.shipper_id

WHERE 1 = 1 
AND raw.rank = ${order_threshold}

group by 1,2,3,4,5,6,7,8,9,10

limit ${num_of_driver}