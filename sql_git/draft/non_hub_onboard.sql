with driver as 
(SELECT 
 uid 
,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as shipper_type 
,date(from_unixtime(ps.create_time - 3600)) as onboard_date

from shopeefood.foody_internal_db__shipper_info_personal_tab__reg_continuous_s0_live ps 
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = ps.uid and sm.grass_date = 'current'
where 1 = 1 
and sm.shipper_status_code = 1
and sm.city_id in (217,218)
)

,ado as 
(SELECT 
date(from_unixtime(dot.real_drop_time - 3600)) as date_ 
,year(date(from_unixtime(dot.real_drop_time - 3600)))*100+week(date(from_unixtime(dot.real_drop_time - 3600))) as created_year_week
,dot.uid 
,dot.pick_city_id 
,sla.completed_rate*1.00000/100 as sla
,count(distinct dot.order_code) as total_order 

FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot 
LEFT JOIN shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live sla on sla.uid = dot.uid 
                                                        and date(from_unixtime(dot.real_drop_time - 3600)) = date(from_unixtime(sla.report_date - 3600))
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(from_unixtime(dot.real_drop_time - 3600))
where order_status = 400
--and ref_order_category = 0
and date(from_unixtime(dot.real_drop_time - 3600)) between current_date - interval '60' day and current_date - interval '1' day 
group by 1,2,3,4,5
)

SELECT 
 b.uid 
,b.onboard_date 
,case when fresh.id is not null then 'Hub fresh' else 'Non hub' end as source
,sum(case when c.date_ between b.onboard_date and b.onboard_date + interval '9' day then c.total_order else null end) as total_order
,sum(case when c.date_ between b.onboard_date and b.onboard_date + interval '9' day then c.sla else null end)*1.00000/count(distinct case when c.date_ between b.onboard_date and b.onboard_date + interval '9' day then c.date_ else null end) as sla_ 



FROM   driver b 
LEFT JOIN ado c on c.uid = b.uid
LEFT JOIN vnfdbi_opsndrivers.foody_hub_fresh_driver_data fresh on cast(fresh.id as bigint) = b.uid 

WHERE b.onboard_date between date'2022-02-07' and date'2022-03-11'
and fresh.id is null

GROUP BY 1,2,3
