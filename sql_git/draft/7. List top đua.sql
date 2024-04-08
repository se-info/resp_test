SELECT oct.shipper_id
      ,driver.shipper_name
      ,driver.city_group
      ,driver.city_name
      ,driver.shipper_type_id
      ,case when driver.shipper_type_id <> 12 then null
      when start_shift = 0 and end_shift = 23 then 'e. All-Day'
      when end_shift - start_shift = 10 then 'd. HUB-10'
      when end_shift - start_shift = 8 then 'c. HUB-08'
      when end_shift - start_shift = 5 and start_shift < 11 then 'a. HUB-05S'
      when end_shift - start_shift = 5 and start_shift > 11 then 'b. HUB-05C'
      else null end as hub_type
      ,IF(driver.shipper_type_id = 12, 'Hub', 'Part-time') AS shipper_type
      ,COALESCE(bonus.current_driver_tier,'') AS current_driver_tier   
      ,oct.oct_cnt_total_order_completed AS cnt_total_order_completed
      ,oct.oct_cnt_outshift_order_completed
      ,oct.oct_cnt_total_order_completed_hcm_hn_only
      
      ,oct.cnt_total_peak_noon_order
      ,oct.oct_cnt_ns_order AS cnt_total_nowship_order
      
      ,oct.oct_sum_total_distance AS total_distance
      ,oct.oct_sum_total_item total_item
      ,oct.oct_sum_total_paid_to_merchant AS total_amt_paid_to_merchant

      ,COALESCE(srd.completed_rate*1.00/100,0) service_level

      
FROM
(
SELECT base.shipper_id
    --  ,base.city_group
    --  ,base.city_name

      ,count(distinct case when base.report_date = date('2021-12-12') and base.order_status in ('Delivered') then base.uid else null end) oct_cnt_total_order_completed
      ,count(distinct case when base.report_date = date('2021-12-12') and base.order_status in ('Delivered') and base.is_order_in_hub_shift = 0 then base.uid else null end) oct_cnt_outshift_order_completed
      ,count(distinct case when base.report_date = date('2021-12-12') and base.order_status in ('Delivered') and base.city_group in ('HCM','HN') then base.uid else null end) oct_cnt_total_order_completed_hcm_hn_only
      ,count(distinct case when base.report_date = date('2021-12-12') and base.order_status in ('Delivered') and ((Extract(HOUR from base.last_delivered_timestamp) BETWEEN 11 AND 12) or base.last_delivered_timestamp = TIMESTAMP '2021-12-12 13:00:00.000') then base.uid else null end ) cnt_total_peak_noon_order
      ,count(distinct case when base.report_date = date('2021-12-12') and base.source in ('Now Ship','Now Ship Shopee') and base.order_status in ('Delivered') then base.uid else null end ) oct_cnt_ns_order
      ,sum(case when base.report_date = date('2021-12-12') and base.order_status in ('Delivered') then base.distance else 0 end ) oct_sum_total_distance
      ,sum(case when base.report_date = date('2021-12-12') and base.order_status in ('Delivered') then base.item_count else 0 end ) oct_sum_total_item
      ,sum(case when base.report_date = date('2021-12-12') and base.order_status in ('Delivered') then base.paid_to_merchant else 0 end) oct_sum_total_paid_to_merchant
FROM
(SELECT base.shipper_id
      ,base.created_date
      ,case when (base.is_asap = 0 and base.order_status = 'Delivered') then date(base.last_delivered_timestamp) else  date(base.last_delivered_timestamp) end as report_date
      ,base.city_name
      ,base.city_group
      ,base.uid
      ,base.order_status
      ,base.is_breached_customer_promise 
      ,base.last_delivered_timestamp
      ,base.source
      ,base.distance
      ,base.is_asap
      ,base.item_count
      ,base.paid_to_merchant*1.00/100 as paid_to_merchant
      ,base.is_order_in_hub_shift
from
(-- ********** order_delivery
SELECT oct.id
,concat('order_delivery_',cast(oct.id as VARCHAR)) as uid
,'order_delivery' as source
,oct.shipper_uid as shipper_id
-- order distance
,oct.distance
,case when oct.distance <= 3 then '1. 0-3km'
    when oct.distance <= 5 then '2. 3-5km'
    when oct.distance <= 7 then '3. 5-7km'
    when oct.distance <= 10 then '4. 7-10km'
    when oct.distance > 10 then '5. 10km+'
    else null end as distance_range

-- time
,from_unixtime(oct.submit_time - 3600) as created_timestamp
,cast(from_unixtime(oct.submit_time - 3600) as date) as created_date
,format_datetime(cast(from_unixtime(oct.submit_time - 3600) as date),'EE') as created_day_of_week
,case when cast(from_unixtime(oct.submit_time - 3600) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
    when cast(from_unixtime(oct.submit_time - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
    else YEAR(cast(from_unixtime(oct.submit_time - 3600) as date))*100 + WEEK(cast(from_unixtime(oct.submit_time - 3600) as date)) end as created_year_week
,concat(cast(YEAR(from_unixtime(oct.submit_time - 3600)) as VARCHAR),'-',date_format(from_unixtime(oct.submit_time - 3600),'%b')) as created_year_month
,Extract(HOUR from from_unixtime(oct.submit_time - 3600)) as created_hour
,case when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 5 then '5. 22:00-5:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 10 then '1. 6:00-10:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 13 then '2. 11:00-13:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 17 then '3. 14:00-17:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 21 then '4. 18:00-21:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 23 then '5. 22:00-5:00'
    else null end as created_hour_range

-- incharge time
,osl.first_auto_assign_timestamp
,osl.last_incharge_timestamp
,date_diff('second',osl.first_auto_assign_timestamp,osl.last_incharge_timestamp) as lt_incharge -- from 1st auto assign to last incharge

-- completion time
,osl.last_delivered_timestamp
,date_diff('second',from_unixtime(oct.submit_time - 3600),osl.last_delivered_timestamp) as lt_completion
,from_unixtime(oct.estimated_delivered_time - 3600) as estimated_delivered_time
,case when osl.last_delivered_timestamp > from_unixtime(oct.estimated_delivered_time - 3600) then 1 else 0 end as is_breached_customer_promise

-- order info
,case when oct.status = 7 then 'Delivered'
    when oct.status = 8 then 'Cancelled'
    when oct.status = 9 then 'Quit' end as order_status

,case when oct.foody_service_id = 1 then 'Food'
    -- when oct.foody_service_id = 3 then 'Laundy'
    -- when oct.foody_service_id = 4 then 'Products'
    -- when oct.foody_service_id = 5 then 'Fresh'
    -- when oct.foody_service_id = 6 then 'Flowers'
    -- when oct.foody_service_id = 7 then 'Medicine'
    -- when oct.foody_service_id = 12 then 'Pets'
    -- when oct.foody_service_id = 13 then 'Liquor'
    -- when oct.foody_service_id = 15 then 'Salon'
    else 'Market' end as foody_service

-- location
,case when oct.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
,case when oct.city_id = 217 then 'HCM'
    when oct.city_id = 218 then 'HN'
    when oct.city_id = 219 then 'DN'
    ELSE 'OTH' end as city_group
-- ,district.district_name

-- flag
,oct.is_foody_delivery
,oct.is_asap

-- payment
,case when oct.payment_method = 1 then 'Cash'
    when oct.payment_method = 6 then 'Airpay'
    else 'Others' end as payment_method
    
,district.name_en as district_name 
,go.item_count
,case when oct.merchant_paid_method = 1 and oct.status = 7 then merchant_paid_amount else 0 end as paid_to_merchant
,case when cast(from_unixtime(oct.submit_time - 3600) as date) between date('2021-07-09') and date('2021-10-05') and sm.shipper_type_id = 12 and oct.city_id = 217 then 1
    when cast(from_unixtime(oct.submit_time - 3600) as date) between date('2021-07-24') and date('2021-10-04') and sm.shipper_type_id = 12 and oct.city_id = 218 then 1
    when dotet.driver_payment_policy = 2 then 1 else 0 end as is_order_in_hub_shift
from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
left join (SELECT id, ref_order_id, ref_order_category FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) WHERE date(from_unixtime(submitted_time- 3600)) BETWEEN DATE'2021-12-12' - interval '7' day AND DATE'2021-12-12') dot ON oct.id = dot.ref_order_id AND dot.ref_order_category = 0
left join (SELECT order_id, cast(json_extract(order_data,'$.shipper_policy.type') as bigint) AS driver_payment_policy FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) WHERE cast(json_extract(order_data,'$.shipper_policy.type') as bigint) = 2) dotet ON dot.id = dotet.order_id
left join shopeefood.foody_mart__profile_shipper_master sm ON oct.shipper_uid = sm.shipper_id AND TRY_CAST(sm.grass_date AS DATE) = DATE'2021-12-12'
left join shopeefood.foody_mart__fact_gross_order_join_detail go ON go.id = oct.id
    -- location
    left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id AND city.country_id = 86
                
    Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = oct.district_id

    
    -- assign time: request archive log
    left join
            (SELECT order_id
                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
                
            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
            where 1=1
            group by order_id
            )osl on osl.order_id = oct.id
WHERE 1=1
AND cast(from_unixtime(oct.submit_time - 3600) as date) >= date('2021-12-12') - interval '7' day
AND cast(from_unixtime(oct.submit_time - 3600) as date) <= date('2021-12-12')
AND oct.status = 7 -- Delivered
AND oct.shipper_uid > 0

UNION ALL

--************** Now Ship/NSS
SELECT ns.id
,ns.uid
,case when ns.booking_type = 1 then 'Now Ship Moto'
    when ns.booking_type = 2 then 'Now Ship'
    when ns.booking_type = 3 then 'Now Ship'
    when ns.booking_type = 4 then 'Now Ship Shopee'
    else null end as source
,ns.shipper_id
-- order distance
,ns.distance*1.00/1000 as distance
,case when ns.distance*1.00/1000 <= 3 then '1. 0-3km'
    when ns.distance*1.00/1000 <= 5 then '2. 3-5km'
    when ns.distance*1.00/1000 <= 7 then '3. 5-7km'
    when ns.distance*1.00/1000 <= 10 then '4. 7-10km'
    when ns.distance*1.00/1000 > 10 then '5. 10km+'
    else null end as distance_range

-- time
,from_unixtime(ns.create_time - 3600) as created_timestamp
,cast(from_unixtime(ns.create_time - 3600) as date) as created_date
,format_datetime(cast(from_unixtime(ns.create_time - 3600) as date),'EE') as created_day_of_week
,case when cast(from_unixtime(ns.create_time - 3600) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
    when cast(from_unixtime(ns.create_time - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
    else YEAR(cast(from_unixtime(ns.create_time - 3600) as date))*100 + WEEK(cast(from_unixtime(ns.create_time - 3600) as date)) end as created_year_week
,concat(cast(YEAR(from_unixtime(ns.create_time - 3600)) as VARCHAR),'-',date_format(from_unixtime(ns.create_time - 3600),'%b')) as created_year_month
,Extract(HOUR from from_unixtime(ns.create_time - 3600)) as created_hour
,case when Extract(HOUR from from_unixtime(ns.create_time - 3600)) <= 5 then '5. 22:00-5:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 3600)) <= 10 then '1. 6:00-10:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 3600)) <= 13 then '2. 11:00-13:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 3600)) <= 17 then '3. 14:00-17:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 3600)) <= 21 then '4. 18:00-21:00'
    when Extract(HOUR from from_unixtime(ns.create_time - 3600)) <= 23 then '5. 22:00-5:00'
    else null end as created_hour_range

-- incharge time
,osl.first_auto_assign_timestamp
,osl.last_incharge_timestamp
,date_diff('second',osl.first_auto_assign_timestamp,osl.last_incharge_timestamp) as lt_incharge -- from 1st auto assign to last incharge

-- completion time
,case when ns.drop_real_time = 0 then NULL else from_unixtime(ns.drop_real_time-3600) end as last_delivered_timestamp
,case when ns.drop_real_time = 0 then null else date_diff('second',from_unixtime(ns.create_time - 3600),from_unixtime(ns.drop_real_time-3600)) end as lt_completion
,case when ns.distance <= 5000 then date_add('minute',40,from_unixtime(ns.pick_real_time - 3600)) -- as estimated_delivered_time
      when ns.distance <= 10000 then date_add('minute',60,from_unixtime(ns.pick_real_time - 3600))
      when ns.distance <= 15000 then date_add('minute',100,from_unixtime(ns.pick_real_time - 3600))
      when ns.distance <= 20000 then date_add('minute',130,from_unixtime(ns.pick_real_time - 3600))
      when ns.distance <= 25000 then date_add('minute',150,from_unixtime(ns.pick_real_time - 3600))
      when ns.distance <= 30000 then date_add('minute',180,from_unixtime(ns.pick_real_time - 3600))
      when ns.distance <= 180000 then date_add('minute',250,from_unixtime(ns.pick_real_time - 3600))
      else null end as estimated_delivered_time
      
,

case when ns.distance <= 5000 then case when from_unixtime(ns.drop_real_time-3600) > date_add('minute',40,from_unixtime(ns.pick_real_time - 3600)) then 1 else 0 end -- as estimated_delivered_time
      when ns.distance <= 10000 then case when from_unixtime(ns.drop_real_time-3600) > date_add('minute',60,from_unixtime(ns.pick_real_time - 3600)) then 1 else 0 end 
      when ns.distance <= 15000 then case when from_unixtime(ns.drop_real_time-3600) >  date_add('minute',100,from_unixtime(ns.pick_real_time - 3600)) then 1 else 0 end 
      when ns.distance <= 20000 then case when from_unixtime(ns.drop_real_time-3600) > date_add('minute',130,from_unixtime(ns.pick_real_time - 3600)) then 1 else 0 end 
      when ns.distance <= 25000 then case when from_unixtime(ns.drop_real_time-3600) > date_add('minute',150,from_unixtime(ns.pick_real_time - 3600)) then 1 else 0 end 
      when ns.distance <= 30000 then case when from_unixtime(ns.drop_real_time-3600) > date_add('minute',180,from_unixtime(ns.pick_real_time - 3600)) then 1 else 0 end 
      when ns.distance <= 180000 then case when from_unixtime(ns.drop_real_time-3600) > date_add('minute',250,from_unixtime(ns.pick_real_time - 3600)) then 1 else 0 end 
      else null end as is_breached_customer_promise
-- order info
,case when ns.status = 11 then 'Delivered'
    when ns.status in (6,9,12) then 'Cancelled'
    else 'Others' end as order_status
    
,case when ns.booking_type = 1 then 'Now Ship Moto'
    when ns.booking_type = 2 then 'Now Ship'
    when ns.booking_type = 3 then 'Now Ship'
    when ns.booking_type = 4 then 'Now Ship Shopee'
    else null end as foody_service

-- location
,case when ns.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
,case when ns.city_id = 217 then 'HCM'
    when ns.city_id = 218 then 'HN'
    when ns.city_id = 219 then 'DN'
    ELSE 'OTH' end as city_group
--,district.district_name

-- flag
,1 as is_foody_delivery
,case when ns.pick_type = 1 then 1 else 0 end as is_asap

-- payment
,case when ns.payment_method = 1 then 'Cash'
    when ns.payment_method = 6 then 'Airpay'
    else 'Others' end as payment_method
,district.name_en as district_name 
,1 as item_count
,case when ns.merchant_payment_method <> 13 then item_value else 0 end as paid_to_merchant
,0 as is_order_in_hub_shift
from
    (SELECT id,concat('now_ship_',cast(id as VARCHAR)) as uid, booking_type,shipper_id, distance,create_time, status, payment_method,'now_ship' as original_source,city_id,cast(json_extract(extra_data,'$.pick_address_info.district_id') as DOUBLE) as district_id , pick_real_time,drop_real_time
              ,pick_type, payment_method as merchant_payment_method, item_value
        from shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live
    
    UNION
    
    SELECT id,concat('now_ship_shopee_',cast(id as VARCHAR)) as uid, 4 as booking_type, shipper_id,distance,create_time,status,1 as payment_method,'now_ship_shopee' as original_source,city_id,cast(json_extract(extra_data,'$.sender_info.district_id') as DOUBLE) as district_id, pick_real_time,drop_real_time
            , 1 as pick_type, 13 as merchant_payment_method, 0 as item_value
        from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live
    
    )ns

    -- assign time: request archive log
    left join
        (SELECT order_id
            ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
            ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
            ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
            
        from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
        where 1=1
        group by order_id
        )osl on osl.order_id = ns.id
    
    -- location
    left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = ns.city_id AND city.country_id = 86
    Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = ns.district_id
WHERE 1=1
AND cast(from_unixtime(ns.create_time - 3600) as date) >= date('2021-12-12') - interval '7' day
AND cast(from_unixtime(ns.create_time - 3600) as date) <= date('2021-12-12')
AND ns.status = 11 -- Delivered
AND ns.shipper_id > 0
)base

WHERE 1=1

)base

GROUP BY 1
HAVING count(distinct case when base.report_date = date('2021-12-12') and base.order_status in ('Delivered') then base.uid else null end) > 0
)oct


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
            ,sm.shipper_type
            ,date_diff('second',date_trunc('day',from_unixtime(si.create_time - 3600)), date_trunc('day',cast(date(current_date) as TIMESTAMP)))*1.0000/(3600*24) as seniority
            ,sm.shipper_type_id
            ,case when (ss.end_time - ss.start_time)*1.00/3600 > 5.00 and (ss.end_time - ss.start_time)*1.00/3600 < 10.00 then (ss.end_time - 28800)/3600 else ss.start_time/3600 end as start_shift
            ,ss.end_time/3600 as end_shift
            from shopeefood.foody_mart__profile_shipper_master sm
            left join shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live si on si.uid = sm.shipper_id
            left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss on ss.id = sm.shipper_shift_id
            where 1=1
            and sm.shipper_type_id <> 3
            and sm.shipper_status_code = 1
            and try_cast(sm.grass_date as date) = date('2021-12-12')
            GROUP BY 1,2,3,4,5,6,7,8,9,10
    ) driver on driver.shipper_id = oct.shipper_id --and driver.report_date = date('2021-12-12')

LEFT JOIN

(SELECT cast(from_unixtime(bonus.report_date - 3600) as date) as report_date
,case when cast(from_unixtime(bonus.report_date - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
    else YEAR(cast(from_unixtime(bonus.report_date - 3600) as date))*100 + WEEK(cast(from_unixtime(bonus.report_date - 3600) as date)) end as report_year_week
,bonus.uid as shipper_id
,bonus.total_point
,bonus.tier
,bonus.daily_point
,bonus.completed_rate*1.00/(100*1.00) as completed_rate_service_level
,case when bonus.completed_rate*1.00/(100*1.00) <= 50 then 'a. 0-50%'
    when bonus.completed_rate*1.00/(100*1.00) <= 70 then 'b. 50-70%'
    when bonus.completed_rate*1.00/(100*1.00) <= 75 then 'c. 70-75%'
    when bonus.completed_rate*1.00/(100*1.00) <= 80 then 'd. 75-80%'
    when bonus.completed_rate*1.00/(100*1.00) <= 85 then 'e. 80-85%'
    when bonus.completed_rate*1.00/(100*1.00) <= 90 then 'f. 85-90%'
    when bonus.completed_rate*1.00/(100*1.00) <= 95 then 'g. 90-95%'
    when bonus.completed_rate*1.00/(100*1.00) <= 100 then 'h. 95-100%'
    else null end as service_level_range
      
,case when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
      when bonus.tier in (2,7,12) then 'T2'
      when bonus.tier in (3,8,13) then 'T3'
      when bonus.tier in (4,9,14) then 'T4'
      when bonus.tier in (5,10,15) then 'T5'
      else null end as current_driver_tier
,coalesce(driver.city_group,driver_extra.city_group) as driver_city_group

from shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
left join (SELECT shipper_id
                ,city_name
                ,case when city_name = 'HCM City' then 'HCM'
                    when city_name = 'Ha Noi City' then 'HN'  
                    when city_name = 'Da Nang City' then 'DN'
                    else 'OTH' end as city_group
                ,case when grass_date = 'current' then date(current_date)
                    else cast(grass_date as date) end as report_date
                
                from shopeefood.foody_mart__profile_shipper_master
                
                where 1=1 
                and try_cast(grass_date as date) = date('2021-12-12')
                GROUP BY 1,2,3,4
            )driver on driver.shipper_id = bonus.uid --and driver.report_date = cast(from_unixtime(bonus.report_date - 3600) as date)

left join (SELECT shipper_id
                ,city_name
                ,case when city_name = 'HCM City' then 'HCM'
                    when city_name = 'Ha Noi City' then 'HN'  
                    when city_name = 'Da Nang City' then 'DN'
                    else 'OTH' end as city_group
                
                from shopeefood.foody_mart__profile_shipper_master
                
                where 1=1 
                and grass_date = 'current'
                GROUP BY 1,2,3
            )driver_extra on driver_extra.shipper_id = bonus.uid          
            

where 1=1
and cast(from_unixtime(bonus.report_date - 3600) as date) = date('2021-12-12')
and bonus.daily_point > 0
-- and bonus.completed_rate*1.00/(100*1.00) < 70
-- limit 100

)bonus on bonus.shipper_id = oct.shipper_id --and bonus.report_date = date('2021-12-12')

LEFT JOIN shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live srd on srd.uid = oct.shipper_id and date(from_unixtime(srd.report_date - 3600)) = date('2021-12-12')
    
WHERE 1=1

and driver.shipper_type not in ('full_time')
