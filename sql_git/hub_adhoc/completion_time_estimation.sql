with base as 
(SELECT 
         dot.ref_order_id as order_id
        ,dot.uid
        ,psm.shipper_name
        ,case when dot.ref_order_category = 0 then 'Food/Fresh' else 'Ship' end as service
        ,case
        WHEN dot.pick_city_id = 217 then 'HCM'
        WHEN dot.pick_city_id = 218 then 'HN'
        ELSE 'Others' end as city_group
        ,psm.city_name as shipper_city
        ,date(from_unixtime(dot.real_drop_time -3600)) as report_date
        ,from_unixtime(dot.submitted_time - 3600) as submitted_timestamp
        ,from_unixtime(dot.real_drop_time - 3600) as final_delivered_timestamp
        ,from_unixtime(dot.estimated_drop_time - 3600) as estimated_delivered_timestamp
        ,fa.last_incharge_timestamp
        ,fa.last_picked_timestamp
        -- ,date_diff('second',from_unixtime(dot.submitted_time - 3600),from_unixtime(dot.real_drop_time - 3600))/cast(60 as double) as actual_completion_time
        -- ,YEAR(date(from_unixtime(dot.real_drop_time -3600)))*100 + WEEK(date(from_unixtime(dot.real_drop_time -3600))) as created_year_week
        ,dot.delivery_distance/cast(1000 as double) as distance
        ,case
        WHEN st.shift_hour = 5 then '5 hour shift'
        WHEN st.shift_hour = 8 then '8 hour shift'
        WHEN st.shift_hour = 10 then '10 hour shift'
 		WHEN st.shift_hour = 3 then '3 hour shift'
        WHEN st.shift_hour > 10 then 'All day shift'
        ELSE add.hub_type END AS hub_type
        ,case when cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then 'Inshift' else 'Normal' end as order_type
        -- ,dot.delivery_cost*1.00/100 as delivery_cost
        ,case when from_unixtime(dot.real_drop_time - 3600) > from_unixtime(dot.estimated_drop_time - 3600) then 'Late'
              else 'Non Late' end as is_late_eta
        ,coalesce(go.bad_weather_fee,0) as bad_weather_fee
        ,date_diff('second',fa.last_incharge_timestamp,fa.last_picked_timestamp)/cast(60 as double) as pickup_time    
        ,date_diff('second',fa.last_incharge_timestamp,from_unixtime(dot.real_drop_time - 3600))/cast(60 as double) as actual_completion_time    

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = dot.ref_order_id and dot.ref_order_category = 0

LEFT JOIN shopeefood.foody_mart__profile_shipper_master psm on psm.shipper_id = dot.uid AND try_cast(psm.grass_date as date) = date(from_unixtime(dot.real_drop_time -3600))

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on dot.id = doet.order_id

LEFT JOIN
                    (
                    SELECT   order_id , 0 as order_type
                            ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                            ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp  
                            ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp 
                            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                            where 1=1 
                            and grass_schema = 'foody_order_db'
                            group by 1,2
                    
                    UNION
                    
                    SELECT   ns.order_id, ns.order_type ,min(from_unixtime(create_time - 60*60)) first_auto_assign_timestamp
                            ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                            ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp 
                    FROM 
                            ( SELECT order_id, order_type , create_time , update_time, status
                    
                             from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                             and grass_schema = 'foody_partner_archive_db'   
                             UNION
                        
                             SELECT order_id, order_type, create_time , update_time, status
                        
                             from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                             and grass_schema = 'foody_partner_db'
                             )ns
                    GROUP BY 1,2
                    )fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type                    

LEFT JOIN 
(
SELECT   *
        ,date(from_unixtime(date_ts - 3600)) as report_date
        ,(end_time - start_time)/3600 as shift_hour
from
shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
)st on st.uid = dot.uid and st.report_date = date(from_unixtime(dot.real_drop_time -3600))

LEFT JOIN 
( 
SELECT *
        ,case
        WHEN shift_hour = 5 then '5 hour shift'
        WHEN shift_hour = 8 then '8 hour shift'
        WHEN shift_hour = 10 then '10 hour shift'
        WHEN shift_hour = 3 then '3 hour shift'
        ELSE null END AS hub_type
FROM
(select
         id
        ,date_format(from_unixtime(start_time - 25200),'%H') as start_time
        ,date_diff('hour',date_trunc('hour',from_unixtime(start_time - 3600)),date_trunc('hour',from_unixtime(end_time-3600))) as shift_hour
FROM shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live
)
) add on add.id = psm.shipper_shift_id

WHERE 1=1
AND dot.pick_city_id in (217,218,220)
AND psm.shipper_type_id in (12)
-- and dot.ref_order_category = 0
and psm.city_id in (217,218,220)
AND dot.order_status = 400
AND date(from_unixtime(dot.real_drop_time -3600)) = date'${specific_day}'
) 
,driver_locations as 
(
select 
        date_ 
       ,shipper_id
       ,array_agg(hub_name) as hub_name  


from 
(SELECT a1.date_
      ,a1.uid as shipper_id
      ,a1.hub_id_
      ,inf.hub_name

FROM
(
select uid,date(from_unixtime(hub.report_date - 3600)) as date_ ,a.hub_id_ 

from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub

CROSS JOIN UNNEST 
(
    cast(json_extract(hub.extra_data,'$.hub_ids') as array<int>)
) a(hub_id_)

)a1 

left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live inf on inf.id = a1.hub_id_

)

group by 1,2
)

select 
        base.order_id
       ,base.service
       ,base.order_type
       ,base.report_date  
       ,base.city_group as order_city 
       ,base.distance 
       ,base.bad_weather_fee
       ,base.submitted_timestamp
       ,base.final_delivered_timestamp
       ,base.estimated_delivered_timestamp
       ,base.last_incharge_timestamp
       ,base.last_picked_timestamp
       ,base.is_late_eta
       ,actual_completion_time as actual_delivering_time
       ,case when base.bad_weather_fee > 0 then (base.distance/cast(10 as double))*60 + base.pickup_time
             else (base.distance/cast(15 as double))*60 + base.pickup_time 
             end as estimation_delivering_time_10_15kmh
       ,base.uid as shipper_id
       ,base.shipper_city
       ,dl.hub_name 




from base 

left join driver_locations dl on dl.shipper_id = base.uid and base.report_date = dl.date_ 
