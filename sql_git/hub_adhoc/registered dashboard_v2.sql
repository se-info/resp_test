with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'${start_date}',date'${end_date}') bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,CAST(report_date as varchar)
        ,CAST(report_date as varchar)
        ,CAST(1 as double)

from raw_date
group by 1,2,3,4,5

UNION ALL 

SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,CAST(date_trunc('week',report_date) as varchar)
        ,CAST((date_trunc('week',report_date) + interval '7' day - interval '1' day) as varchar)
        ,CAST(date_diff('day',date_trunc('week',report_date),(date_trunc('week',report_date) + interval '7' day - interval '1' day)) as double)

from raw_date
group by 1,2,3,4,5

-- UNION ALL 

-- SELECT 
--         '3. Monthly'
--         ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
--         ,CAST(date_trunc('month',report_date) as varchar)
--         ,CAST((date_trunc('month',report_date) + interval '1' month - interval '1' day) as varchar) 
--         ,CAST(date_diff('day',date_trunc('month',report_date),(date_trunc('month',report_date) + interval '30' day - interval '1' day)) as double)

-- from raw_date

-- group by 1,2,3,4,5
)
,register as 
(SELECT 
 do.date_
,do.slot_id
,sm.city_name
-- ,ht.hub_name
,cast(do.shift_hour as varchar)||'hour shift - '||cast(do.start_time as varchar) as hub_type
,hi.max_drivers as max_driver 
,count(distinct case when (registration_status = 'Registered' or  registration_status =  'Worked') then do.uid else null end) as total_register
,count(distinct case when is_active = 1 then do.uid else null end) as active_


FROM
(
SELECT 
         date(from_unixtime(do.date_ts - 3600)) as date_
        ,do.uid
        ,do.slot_id
        ,case when do.registration_status = 1 then 'Registered'
            when do.registration_status = 2 then 'OFF'
            when do.registration_status = 3 then 'Worked'
            end as registration_status
        ,(do.end_time - do.start_time)/3600 as shift_hour
        ,do.start_time/3600 as start_time
        ,case when act.uid is not null then 1 else 0 end as is_active

from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live do

LEFT JOIN 
(SELECT   
         dot.uid
        ,date(FROM_UNIXTIME(dot.real_drop_time - 60*60)) as report_date
        ,cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) policy
        ,dot.ref_order_code
        ,row_number() over(partition by uid,date(FROM_UNIXTIME(dot.real_drop_time - 60*60)) order by real_drop_time desc) as rank 

        
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on dot.id = doet.order_id


WHERE 1=1
AND dot.order_status = 400
AND dot.pick_city_id not in (238,469)
AND cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 
-- AND date(FROM_UNIXTIME(dot.real_drop_time - 60*60)) between current_date - interval '90' day and current_date - interval '1' day

)act on act.uid = do.uid 
     and act.report_date = date(from_unixtime(do.date_ts - 3600)) 
     and act.rank = 1 

) do


--Shipper HUB
LEFT JOIN (
SELECT 
          *
        ,case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end as report_date
FROM
shopeefood.foody_mart__profile_shipper_master
) sm on sm.shipper_id = do.uid and sm.report_date =(case when do.date_ > sm.report_date then date(current_date)  
                                                                                        else do.date_ end)
--HUB Location  
LEFT JOIN shopeefood.foody_internal_db__shipper_config_slot_tab__vn_daily_s0_live hi on hi.id = do.slot_id

-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live ht on ht.id = hi.hub_id

where 1 = 1
and sm.shipper_type_id = 12
and sm.shipper_status_code = 1
and sm.city_name != 'Dien Bien'

GROUP BY 1,2,3,4,5
)
select 
        p.period
       ,p.period_group
       ,a.city_name
       ,a.hub_type
       ,sum(a.max_driver)/p.days as max_registered
       ,sum(a.total_register)/p.days as registered
       ,sum(a.active_)/p.days as active




from register a 

inner join params_date p on a.date_ between cast(p.start_date as date) and cast(p.end_date as date)

group by 1,2,3,4,p.days

-- where cast(a.period as date) between date'2022-10-02' and date'2022-10-19' 
