with raw as 
(select 
        dot.uid as shipper_id 
       ,dot.ref_order_code 
       ,date(from_unixtime(dot.real_drop_time - 3600)) as report_date   
       ,json_extract(dotet.order_data,'$.shipper_policy.type')
       ,dotet.order_data
       ,dot.delivery_cost         

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

WHERE 1 = 1 
AND dot.order_status = 400 
AND dot.ref_order_code = '31013-633162498'
)
select 
        raw.report_date
       ,raw.shipper_id
       ,spp.shopee_uid
       ,sm.shipper_name
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non hub' end as driver_type  
       ,sm.city_name
       ,sla.completed_rate/cast(100 as double) as sla_rate
       ,case when sm.shipper_status_code = 1 then 'Normal' else 'Other' end as working_status 
--        ,spp.main_phone
       ,count(distinct ref_order_code) as total_order_p_days
       ,s.total_order as total_26_30_order                 

from raw 


left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.shipper_id and try_cast(sm.grass_date as date) = raw.report_date

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp on spp.uid = raw.shipper_id

left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live sla on sla.uid = raw.shipper_id and date(from_unixtime(sla.report_date - 3600)) = raw.report_date

left join (select shipper_id,count(distinct ref_order_code) as total_order from raw where report_date between date'2023-01-26' and date'2023-01-30' group by 1) s on s.shipper_id = raw.shipper_id

where raw.report_date between date'2023-01-26' and date'2023-01-30'


group by 1,2,3,4,5,6,7,8,s.total_order



