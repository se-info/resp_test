with order_raw as 
(SELECT oct.id
-- time
,from_unixtime(oct.submit_time - 60*60) as created_timestamp
,cast(from_unixtime(oct.submit_time - 60*60) as date) as created_date
,HOUR(from_unixtime(oct.submit_time - 60*60)) as created_hour
-- order info
,case when oct.status = 7 then 'Delivered'
    when oct.status = 8 then 'Cancelled'
    when oct.status = 9 then 'Quit' end as order_status
-- location
,case when oct.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
,case when oct.city_id  = 217 then 'HCM'
    when oct.city_id  = 218 then 'HN'
    when oct.city_id  = 219 then 'DN'
    else 'OTH' end as city_group
-- ,district.district_name
,oct.is_asap
-- location id
,oct.city_id
,oct.district_id


from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86
)

,pm_raw as 
(select 
         date(from_unixtime(a.start_time - 3600)) as created_date
        ,from_unixtime(a.start_time - 3600) as start_ts
        ,from_unixtime((a.start_time + running_time) - 3600) as end_ts
        ,running_time/cast(60 as double) as running_ts
        ,assigning_order
        ,available_driver 
        ,online_driver
        ,city_id
        ,b.name as peak_mode_name






from shopeefood.foody_delivery_admin_db__peak_mode_export_activity_tab__reg_daily_s0_live a 

left join shopeefood.foody_delivery_admin_db__peak_mode_tab__reg_daily_s0_live b on b.id = a.mode_id

order by start_time desc 
)

select 
        od.*
       ,case when pm.peak_mode_name in ('Peak 1 Mode','Peak 2 Mode','Peak 3 Mode') then pm.peak_mode_name
             else 'Normal Mode' end as peak_mode  


from order_raw od 

left join pm_raw pm on pm.city_id = od.city_id and od.created_timestamp >= pm.start_ts and od.created_timestamp <= pm.end_ts
