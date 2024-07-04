 
SELECT active.report_date
,case when active.report_date between date('2022-01-01') and date('2022-01-02') then 202152
      else active.report_week end as created_year_week,active.city_name
,active.city_group
,active.current_driver_tier
,active.service_
--,active.created_hour_range
,count(distinct active.shipper_id) as total_driver_active
,sum(active.total_order) as total_order

from
(SELECT base1.report_date
,case when base1.report_date between DATE('2018-12-31') and DATE('2018-12-31') then 201901
        when base1.report_date between DATE('2019-12-30') and DATE('2019-12-31') then 202001
        when base1.report_date between DATE('2021-01-01') and DATE('2021-01-03') then 202053
         else YEAR(base1.report_date)*100 + WEEK(base1.report_date) end as report_week
,base1.city_name
,base1.city_group
,base1.service_
,base1.shipper_id
--,base1.created_hour_range
,case when driver_hub.shipper_type_id = 12 then 'Hub' 
      when driver_hub.shipper_type_id = 1 then 'full time' 
      ELSE coalesce(bonus.current_driver_tier,'others') end as current_driver_tier  
--,coalesce(bonus.new_driver_tier,'full time') as new_driver_tier
,case when base1.total_order < 10 or base1.total_order is null then 'a. G: <10'
      when base1.total_order < 15 then 'b. G: 10-15'
      when base1.total_order < 25 then 'c. G: 15-25'
      when base1.total_order >= 25 then 'd. G: 25+'
      else null end daily_order_range
,base1.total_order
--,coalesce(bonus.service_level_range,'full time') as service_level_range

from
(SELECT 
case when (base.is_asap = 0 and base.order_status = 'Delivered') then date(base.last_delivered_timestamp) else  base.created_date end as report_date
,base.city_name
,base.service_
,base.city_group
,base.shipper_id
--,base.created_hour_range
,count(distinct base.id) as total_order

from
(-- order delivery: Food/Market
SELECT oct.id
,concat('order_delivery_',cast(oct.id as VARCHAR)) as uid
,oct.shipper_uid as shipper_id
,'1. Food/Fresh' as service_
,from_unixtime(oct.submit_time - 60*60) as created_timestamp
,date(from_unixtime(oct.submit_time - 60*60)) as created_date
,case when cast(from_unixtime(oct.submit_time - 60*60) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
    when cast(from_unixtime(oct.submit_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
    when cast(from_unixtime(oct.submit_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
    else YEAR(cast(from_unixtime(oct.submit_time - 60*60) as date))*100 + WEEK(cast(from_unixtime(oct.submit_time - 60*60) as date)) end as created_year_week
,case when oct.status = 7 then 'Delivered'
    when oct.status = 8 then 'Cancelled'
    when oct.status = 9 then 'Quit' end as order_status
,case when oct.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
,case when oct.city_id = 217 then 'HCM'
    when oct.city_id = 218 then 'HN'
    when oct.city_id = 219 then 'DN'
    ELSE 'OTH' end as city_group
    -- ,count(DISTINCT o.shipper_id) as shipper_active_a7
,case when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 5 then '5. 22:00-6:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 10 then '1. 6:00-11:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 13 then '2. 11:00-14:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 17 then '3. 14:00-18:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 21 then '4. 18:00-22:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 23 then '5. 22:00-6:00'
    else null end as created_hour_range
,oct.is_asap
,osl.last_delivered_timestamp
from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
-- left join foody.foody_mart__profile_shipper_master shp on shp.shipper_id = oct.shipper_uid
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86

    left join
            (SELECT order_id
                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
                
            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
            where 1=1
            group by order_id
            )osl on osl.order_id = oct.id
UNION

--************** Now Ship/NSS
SELECT ns.id
,ns.uid
,ns.shipper_id
,'2. Ship' as service_
-- time
,from_unixtime(ns.create_time - 60*60) as created_timestamp
,cast(from_unixtime(ns.create_time - 60*60) as date) as created_date
,case when cast(from_unixtime(ns.create_time - 60*60) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
when cast(from_unixtime(ns.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
when cast(from_unixtime(ns.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
else YEAR(cast(from_unixtime(ns.create_time - 60*60) as date))*100 + WEEK(cast(from_unixtime(ns.create_time - 60*60) as date)) end as created_year_week

-- order info
,case when ns.status = 11 then 'Delivered'
when ns.status in (6,9,12) then 'Cancelled'
else 'Others' end as order_status

-- location
,case when ns.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
,case when ns.city_id = 217 then 'HCM'
when ns.city_id = 218 then 'HN'
when ns.city_id = 219 then 'DN'
ELSE 'OTH' end as city_group
,case when Extract(HOUR from from_unixtime(ns.create_time - 60*60)) <= 5 then '5. 22:00-6:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 60*60)) <= 10 then '1. 6:00-11:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 60*60)) <= 13 then '2. 11:00-14:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 60*60)) <= 17 then '3. 14:00-18:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 60*60)) <= 21 then '4. 18:00-22:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 60*60)) <= 23 then '5. 22:00-6:00'
    else null end as created_hour_range
,case when ns.pick_type = 1 then 1 else 0 end as is_asap
,case when ns.drop_real_time = 0 then NULL else from_unixtime(ns.drop_real_time-3600) end as last_delivered_timestamp
from
(SELECT id,concat('now_ship_',cast(id as VARCHAR)) as uid, booking_type,shipper_id, distance,create_time, status, payment_method,'now_ship' as original_source,city_id,cast(json_extract(extra_data,'$.pick_address_info.district_id') as DOUBLE) as district_id
          ,pick_type, pick_real_time,drop_real_time
    from shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live

UNION

SELECT id,concat('now_ship_shopee_',cast(id as VARCHAR)) as uid, 4 as booking_type, shipper_id,distance,create_time,status,1 as payment_method,'now_ship_shopee' as original_source,city_id,cast(json_extract(extra_data,'$.sender_info.district_id') as DOUBLE) as district_id
       , 1 as pick_type, pick_real_time,drop_real_time
    from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live

)ns

-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = ns.city_id and city.country_id = 86


)base

where 1=1
-- and base.created_year_week >= 202029 -- YEAR(date(current_date) - interval '30' day)*100 + WEEK(date(current_date) - interval '30' day)
and base.created_date >= date(current_date) - interval '90' day
and base.created_date < date(current_date)
and base.order_status = 'Delivered'
--and base.city_group in ('HCM','HN','DN')

GROUP BY 1,2,3,4,5

)base1

LEFT JOIN
    (
     SELECT  sm.shipper_id
            ,sm.city_name
            ,case when sm.city_name = 'HCM City' then 'HCM'
                when sm.city_name = 'Ha Noi City' then 'HN'
                when sm.city_name = 'Da Nang City' then 'DN'
                else 'OTH' end as city_group
                
            ,case when sm.grass_date = 'current' then date(current_date)
                else cast(sm.grass_date as date) end as report_date
            ,sm.shipper_name
            ,date_diff('second',date_trunc('day',from_unixtime(si.create_time - 60*60)), date_trunc('day',cast(date(current_date) as TIMESTAMP)))*1.0000/(3600*24) as seniority

            from shopeefood.foody_mart__profile_shipper_master sm
            left join shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live si on si.uid = sm.shipper_id
            where 1=1
         --   and grass_date  = '2020-10-19'
            and shipper_type_id <> 3
            and shipper_status_code = 1
      and grass_region = 'VN'
            GROUP BY 1,2,3,4,5,6
    )driver on driver.shipper_id = base1.shipper_id and driver.report_date = date(current_date) - interval '1' day
    
LEFT JOIN (SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
                            ,bonus.uid as shipper_id
                            ,case when bonus.total_point <= 1800 then 'T1'
                                when bonus.total_point <= 3600 then 'T2'
                                when bonus.total_point <= 5400 then 'T3'
                                when bonus.total_point <= 8400 then 'T4'
                                when bonus.total_point > 8400 then 'T5'
                                else null end as new_driver_tier   
                            
                            ,case when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
                                when bonus.tier in (2,7,12) then 'T2'
                                when bonus.tier in (3,8,13) then 'T3'
                                when bonus.tier in (4,9,14) then 'T4'
                                when bonus.tier in (5,10,15) then 'T5'
                                else null end as current_driver_tier
                            ,bonus.total_point
                            ,case when bonus.completed_rate*1.00/(100*1.00) <= 50 then 'a. 0-50%'
                                when bonus.completed_rate*1.00/(100*1.00) <= 70 then 'b. 50-70%'
                                when bonus.completed_rate*1.00/(100*1.00) <= 75 then 'c. 70-75%'
                                when bonus.completed_rate*1.00/(100*1.00) <= 80 then 'd. 75-80%'
                                when bonus.completed_rate*1.00/(100*1.00) <= 85 then 'e. 80-85%'
                                when bonus.completed_rate*1.00/(100*1.00) <= 90 then 'f. 85-90%'
                                when bonus.completed_rate*1.00/(100*1.00) <= 95 then 'g. 90-95%'
                                when bonus.completed_rate*1.00/(100*1.00) <= 100 then 'h. 95-100%'
                                else null end as service_level_range
                        
                        FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
                        
                        )bonus on base1.report_date = bonus.report_date and base1.shipper_id = bonus.shipper_id   

LEFT JOIN
    (
     SELECT  sm.shipper_id
            ,sm.shipper_type_id
                
            ,case when sm.grass_date = 'current' then date(current_date)
                else cast(sm.grass_date as date) end as report_date

            from shopeefood.foody_mart__profile_shipper_master sm
            left join shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live si on si.uid = sm.shipper_id
            where 1=1
      and grass_region = 'VN'

            GROUP BY 1,2,3
    )driver_hub on driver_hub.shipper_id = base1.shipper_id and driver_hub.report_date = base1.report_date                        

)active
        
WHERE 1=1
AND active.report_date >= date((current_date) - interval '90' day)
and active.report_date < date(current_date)
GROUP BY 1,2,3,4,5,6