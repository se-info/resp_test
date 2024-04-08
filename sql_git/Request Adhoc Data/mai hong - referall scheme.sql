-- select 
--          year(from_unixtime(a.create_time - 3600)) as created_year
--         ,month(from_unixtime(a.create_time - 3600)) as created_month
--         ,city.name_en as city_name
--         ,count(distinct a.id) as total_driver_registered
--         ,count(distinct case when status = 11 then a.id else null end) as total_driver_registered_success

            







-- from shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live a 

-- left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on a.city_id = city.id and city.country_id = 86

-- where is_now_shipper_before = 1

-- -- and email = 'ducnhann11@gmail.com'


-- group by 1,2,3


-- select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da



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

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
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
,b.onboard_date + interval '6' day as end_scheme_date
,case when b.uid in 
(40001388
,40009230
,40018422
,40002529
,23162982
,40000980
,40000925
,40002504
,40008525
,40003508
,40008977
,40009288
,40010087
,40019711
,40019711) then 'New driver' 
when b.uid in 
(23080687
,23151926
,23120796
,22263396
,23149751
,22263396
,21838448
,22894405
,21726485
,22793810
,23061635
,21820020
,23000704
,40009288
,40009288) then 'Existing driver' end as type_
,sum(case when c.date_ between b.onboard_date and b.onboard_date + interval '6' day then c.total_order else null end) as total_order
,sum(case when c.date_ = date'2022-09-09' then c.total_order else null end) as total_order_8aug



FROM   driver b 
LEFT JOIN ado c on c.uid = b.uid

-- WHERE b.onboard_date between date'2022-02-07' and date'2022-03-11'
where 1 = 1
and b.uid in 
(23080687
,23151926
,23120796
,22263396
,23149751
,22263396
,21838448
,22894405
,21726485
,22793810
,23061635
,21820020
,23000704
,40009288
,40009288
,40001388
,40009230
,40018422
,40002529
,23162982
,40000980
,40000925
,40002504
,40008525
,40003508
,40008977
,40009288
,40010087
,40019711
,40019711)

GROUP BY 1,2,3,4
