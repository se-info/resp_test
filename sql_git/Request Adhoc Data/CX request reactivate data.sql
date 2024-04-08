with ado as 
(SELECT 
         date(from_unixtime(dot.real_drop_time - 3600)) as report_date
        ,dot.uid
        ,dl.total_online_seconds/cast(3600 as double) as total_online_time
        ,sum(dot.delivery_distance/cast(1000 as double)) as total_distance
        ,count(dot.ref_order_code) as total_order

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

-- LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

LEFT JOIN shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live dl on dl.uid = dot.uid and date(from_unixtime(dl.report_date - 3600)) = date(from_unixtime(dot.real_drop_time - 3600))

where dot.order_status = 400

group by 1,2,3
)
,profile as 
(select 
        try_cast(sm.grass_date as date) as report_date
       ,sm.shipper_id
       ,pf.shopee_uid
       ,sm.shipper_name
       ,sm.city_name
       ,CASE WHEN sm.shipper_type_id = 11 then 'Non Hub'
             WHEN pf.working_time_id = 1 then '5 hour shift'
             WHEN pf.working_time_id = 2 then '8 hour shift'
             WHEN pf.working_time_id = 3 then '10 hour shift'
             else 'Others' end as driver_type


from shopeefood.foody_mart__profile_shipper_master sm 

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live pf on pf.uid = sm.shipper_id



where (try_cast(sm.grass_date as date) = date'2022-07-07'
        or 
        try_cast(sm.grass_date as date) = date'2022-08-08'
        or 
        try_cast(sm.grass_date as date) = date'2022-09-09'
        or 
        try_cast(sm.grass_date as date) = date'2022-10-10')

and sm.city_name not like '%Test%'
)


select 
         b.report_date
        ,b.uid as shipper_id  
        ,a.shopee_uid
        ,a.shipper_name
        ,a.city_name
        ,a.driver_type
        ,b.total_order

from  ado b  

left join profile a on b.uid = a.shipper_id and a.report_date = b.report_date

where (b.report_date = date'2022-07-07'
        or 
        b.report_date = date'2022-08-08'
        or 
        b.report_date = date'2022-09-09'
        or 
        b.report_date = date'2022-10-10')
