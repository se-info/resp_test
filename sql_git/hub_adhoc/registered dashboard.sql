with register as 
(SELECT 
do.date_
--,do.uid as shipper_id
,sm.city_name
,ht.hub_name
--,do.shift_hour
,case when do.shift_hour = 3 and do.start_time <= 6 then cast(do.shift_hour as varchar)||'hour shift - S'
      when do.shift_hour = 3 and do.start_time > 6 then cast(do.shift_hour as varchar)||'hour shift - C'
      when do.shift_hour = 5 and do.start_time <= 6 then cast(do.shift_hour as varchar)||'hour shift - S1'    
      when do.shift_hour = 5 and do.start_time <= 8 then cast(do.shift_hour as varchar)||'hour shift - S2'
      when do.shift_hour = 5 and do.start_time > 8 then cast(do.shift_hour as varchar)||'hour shift - C'              
      else cast(do.shift_hour as varchar)||'hour shift' end as hub_type
--,case when do.uid is not null then 1 else 0 end as is_registered
,hi.max_drivers
,count(distinct case when (registration_status = 'Registered' or  registration_status =  'Worked') then do.uid else null end) as total_register

FROM
(
SELECT 
        date(from_unixtime(date_ts - 3600)) as date_
        ,uid
        ,slot_id
        ,case when registration_status = 1 then 'Registered'
            when registration_status = 2 then 'OFF'
            when registration_status = 3 then 'Worked'
            end as registration_status
        ,(end_time - start_time)/3600 as shift_hour
        ,start_time/3600 as start_time

from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live 
) do


--Shipper HUB
LEFT JOIN (SELECT *,case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end as report_date
FROM
shopeefood.foody_mart__profile_shipper_master) sm on sm.shipper_id = do.uid and sm.report_date =(case when do.date_ > sm.report_date then date(current_date)  
                                                                                                      else do.date_ end)
--HUB Locations

LEFT JOIN shopeefood.foody_internal_db__shipper_config_slot_tab__vn_daily_s0_live hi on hi.id = do.slot_id

LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live ht on ht.id = hi.hub_id

where 1 = 1
and date_ between ${start_date} and ${end_date}
and sm.shipper_type_id = 12
and sm.shipper_status_code = 1
and sm.city_name != 'Dien Bien'
--and ht.hub_name is null
GROUP BY 1,2,3,4,5
)
,actual as 
(
SELECT 
        a.uid as shipper_id
       ,a.report_date 
       ,a.city_name
       ,case 
            when do.shift_hour = 3 and do.start_time <= 6 then cast(do.shift_hour as varchar)||'hour shift - S'
            when do.shift_hour = 3 and do.start_time > 6 then cast(do.shift_hour as varchar)||'hour shift - C'
            when do.shift_hour = 5 and do.start_time <= 6 then cast(do.shift_hour as varchar)||'hour shift - S1'    
            when do.shift_hour = 5 and do.start_time <= 8 then cast(do.shift_hour as varchar)||'hour shift - S2'
            when do.shift_hour = 5 and do.start_time > 8 then cast(do.shift_hour as varchar)||'hour shift - C'              
            else cast(do.shift_hour as varchar)||'hour shift' end as hub_type
       ,count(distinct case when policy = 2 then ref_order_code else null end) as inshift_order
       ,count(distinct ref_order_code) as total_del
       
FROM
(SELECT   dot.uid
        ,case
        WHEN dot.pick_city_id = 217 then 'HCM'
        WHEN dot.pick_city_id = 218 then 'HN'
        ELSE NULL end as city_group
        ,date(FROM_UNIXTIME(dot.real_drop_time - 60*60)) as report_date
        -- case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        -- when dot.order_status in (402,403,404) and cast(json_extract(doet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(doet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
        -- else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
        --,(slot.end_time - slot.start_time)/3600 as shift_hour
        ,sm.city_name
        ,cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) policy
        ,dot.ref_order_code

        
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

--LEFT JOIN shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot on slot.uid = dot.uid and date(from_unixtime(slot.date_ts - 3600)) = date(from_unixtime(dot.real_drop_time - 3600))

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on dot.id = doet.order_id

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
WHERE 1=1
AND dot.order_status = 400
and dot.pick_city_id not in (238,469)
)a
LEFT JOIN 
            (
            SELECT 
                    date(from_unixtime(date_ts - 3600)) as date_
                    ,uid
                    ,slot_id
                    ,case when registration_status = 1 then 'Registered'
                        when registration_status = 2 then 'OFF'
                        when registration_status = 3 then 'Worked'
                        end as registration_status
                    ,(end_time - start_time)/3600 as shift_hour
                    ,start_time/3600 as start_time

            from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live 
            ) do on do.uid = a.uid and do.date_ = a.report_date
            
where report_date between ${start_date} and ${end_date}

and policy = 2 
group by 1,2,3,4
)
,final_1 as 
(select 
        cast(date_ as varchar) as period 
       ,'1. Register' as metrics
       ,'2. Daily' as period_group
       ,city_name
       ,hub_type
       ,sum(max_drivers) as max_registered
       ,sum(total_register) as value


from register 

group by 1,2,3,4,5

UNION ALL 

select 
        'W'||cast(week(date_) as varchar) as period 
       ,'1. Register' as metrics
       ,'1. Weekly' as period_group
       ,city_name
       ,hub_type
       ,sum(max_drivers)/cast(count(distinct date_) as double) as max_registered
       ,sum(total_register)/cast(count(distinct date_) as double) as value


from register 

group by 1,2,3,4,5)

,final_2 as 
(
select 
        cast(report_date as varchar) as period
       ,'2. Active' as metrics
       ,'2. Daily' as period
       ,city_name
       ,hub_type
       ,0 as max_registered
       ,count(distinct case when inshift_order > 0 then shipper_id else null end)/cast(count(distinct report_date) as double) as value


from actual 

group by 1,2,3,4,5,6

UNION ALL 

select 
        'W'||cast(week(report_date) as varchar) as period 
       ,'2. Active' as metrics
       ,'1. Weekly' as period
       ,city_name
       ,hub_type
       ,0 as max_registered
       ,count(case when inshift_order > 0 then shipper_id else null end)/cast(count(distinct report_date) as double) as value


from actual 

group by 1,2,3,4,5,6
)

select * 

from final_1

UNION ALL 

select * 

from final_2
