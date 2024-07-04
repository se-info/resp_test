with base as
(select 
        dot.uid
       ,sm.shipper_name
       ,sm.city_name  as driver_city_name
       ,dot.pick_district_id 
       ,dot.pick_city_id 
       ,dis.name_en as district_name
       ,city.name_en as city_name  
       ,dot.ref_order_code
       ,case when (dis.name_en = 'Di An' or dis.name_en = 'Thuan An') then 4000 else 0 end as bonus_value
       ,date(from_unixtime(dot.real_drop_time - 3600)) as report_date





from shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot 

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live dis on dis.id = dot.pick_district_id

left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp on rp.id = dot.uid and date(from_unixtime(dot.real_drop_time - 3600)) = date(from_unixtime(rp.report_date - 3600))

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(from_unixtime(dot.real_drop_time - 3600))

where dot.pick_city_id = 230

and sm.city_id = 230

and dot.order_status = 400

and date(from_unixtime(dot.real_drop_time - 3600)) between date'2022-08-22' and current_date - interval '1' day
)
,metrics as 
(select 
        a.report_date
       ,a.uid as shipper_id
       ,a.shipper_name
       ,a.driver_city_name
       ,rp.completed_rate/cast(100 as double) as sla_rate 
       ,count(distinct case when bonus_value > 0 then a.ref_order_code else null end ) as total_order_completed 
       ,sum(bonus_value) as total_bonus_value   




from base a 

left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp on rp.uid = a.uid and date(from_unixtime(rp.report_date - 3600)) = a.report_date

group by 1,2,3,4,5)

select 
        report_date
       ,shipper_id
       ,shipper_name
       ,driver_city_name
       ,sla_rate
       ,total_order_completed as total_order_have_bonus
       ,case when report_date < date'2022-08-26' then total_bonus_value 
             when report_date >= date'2022-08-26' and sla_rate >= 90 then total_bonus_value
             else 0 end as bonus_value


from metrics

where 1 = 1 



group by 1,2,3,4,5,6,7

having (case when report_date < date'2022-08-26' then total_bonus_value 
             when report_date >= date'2022-08-26' and sla_rate >= 90 then total_bonus_value
             else 0 end) > 0
        

