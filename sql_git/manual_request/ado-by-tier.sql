with raw as 
(SELECT active.report_date
,case when active.report_date between date('2022-01-01') and date('2022-01-02') then 202152
      else active.report_week end as created_year_week
,active.city_name
,active.city_group
,active.current_driver_tier
--,active.created_hour_range
,count(distinct active.shipper_id) as total_driver_active
,sum(active.total_order_out_shift) as total_order_out_shift
,sum(active.total_order_in_shift) as total_order_in_shift

from
(SELECT base1.report_date
,case when base1.report_date between DATE('2018-12-31') and DATE('2018-12-31') then 201901
        when base1.report_date between DATE('2019-12-30') and DATE('2019-12-31') then 202001
        when base1.report_date between DATE('2021-01-01') and DATE('2021-01-03') then 202053
         else YEAR(base1.report_date)*100 + WEEK(base1.report_date) end as report_week
,driver.city_name
,driver.city_group 
--,base1.city_group
,base1.shipper_id
--,base1.created_hour_range
,case when driver_hub.shipper_type_id = 12 and total_order_in_shift > 0 then coalesce(concat(cast(slot.shift_hour as varchar),'-','hour shift'),concat(cast(driver_hub.shift_hour as varchar),'-','hour shift'))
      when driver_hub.shipper_type_id = 12 and total_order_in_shift = 0 and total_order_out_shift > 0 then 'T1'
      when driver_hub.shipper_type_id = 1 then 'full time' 
      ELSE coalesce(bonus.current_driver_tier,'others') end as current_driver_tier  
--,coalesce(bonus.new_driver_tier,'full time') as new_driver_tier
,base1.total_order_out_shift
,base1.total_order_in_shift
--,coalesce(bonus.service_level_range,'full time') as service_level_range

from
(SELECT 
 report_date
--,base.city_name
--,base.city_group
,base.shipper_id
--,base.created_hour_range
,count(distinct case when policy <> 2 then base.id else null end) as total_order_out_shift
,count(distinct case when policy = 2 then base.id else null end) as total_order_in_shift

from
(
SELECT 
      from_unixtime(dot.real_drop_time - 3600) as last_delivered_timestamp
        ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
        when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
        else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date                  
      ,case when order_status = 400 then 'Delivered' else 'Other' end as order_status
      ,case when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
    when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
    when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
    when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
    when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2022-01-01') and DATE('2022-01-02') then 202152
    else YEAR(cast(from_unixtime(dot.submitted_time - 60*60) as date))*100 + WEEK(cast(from_unixtime(dot.submitted_time - 60*60) as date)) end as created_year_week
      ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            else 'OTH' end as city_group 
       ,city.name_en as city_name
       ,ref_order_id as id 
       ,dot.uid as shipper_id
       ,dot.is_asap
       ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as policy 

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

)base

where 1=1
-- and base.created_year_week >= 202029 -- YEAR(date(current_date) - interval '30' day)*100 + WEEK(date(current_date) - interval '30' day)
and base.report_date >= date(current_date) - interval '120' day
and base.report_date < date(current_date)
and base.order_status = 'Delivered'
--and base.city_group in ('HCM','HN','DN')

GROUP BY 1,2

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
            and grass_region = 'VN'
            GROUP BY 1,2,3,4,5,6
    )driver on driver.shipper_id = base1.shipper_id and driver.report_date = date(current_date) - interval '1' day

LEFT JOIN (SELECT *,date(from_unixtime(date_ts - 3600)) as report_date
        ,(end_time - start_time)/3600 as shift_hour
        from
        shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
        )slot on slot.uid = base1.shipper_id and slot.report_date = base1.report_date


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
            ,sm.shipper_shift_start_timestamp
            ,sm.shipper_shift_end_timestamp
            ,(sm.shipper_shift_end_timestamp - sm.shipper_shift_start_timestamp)/3600 as shift_hour
                
            ,case when sm.grass_date = 'current' then date(current_date)
                else cast(sm.grass_date as date) end as report_date

            from shopeefood.foody_mart__profile_shipper_master sm
            left join shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live si on si.uid = sm.shipper_id
            where 1=1
            and grass_region = 'VN'

            GROUP BY 1,2,3,4,5,6
    )driver_hub on driver_hub.shipper_id = base1.shipper_id and driver_hub.report_date = base1.report_date                        
-- limit 1000

)active
        
WHERE 1=1 
GROUP BY 1,2,3,4,5)

,params(period, start_date, end_date, days) AS (
    VALUES
    (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day), '%b'), DATE_TRUNC('month', current_date - interval '1' day), current_date - interval '1' day, CAST(DAY(current_date - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day), 'W%v'), DATE_TRUNC('week', current_date - interval '1' day), current_date - interval '1' day, CAST(DATE_DIFF('day', DATE_TRUNC('week', current_date - interval '1' day), current_date) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '1' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '8' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '15' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '22' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '35' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '35' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '29' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '42' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '42' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '36' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '49' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '49' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '43' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '56' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '56' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '50' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '63' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '63' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '57' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '70' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '70' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '64' day, CAST(7 AS DOUBLE))

    )


    SELECT p.period 
          ,p.days 
          ,a.city_group
          ,a.city_name
          ,a.current_driver_tier
          ,sum(a.total_order_in_shift)*1.00000/count(distinct a.report_date) as inshift_orders
          ,sum(a.total_order_out_shift)*1.00000/count(distinct a.report_date) as outshift_orders
    
    
 from raw a 

 inner join params p on a.report_date between p.start_date and p.end_date   
    
    
    
group by 1,2,3,4,5