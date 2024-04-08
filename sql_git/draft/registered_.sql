with daily as 
(SELECT cast(do.date_ as varchar) as period
,'1. Daily' as period_gropup
--,do.uid as shipper_id
,sm.city_name
,ht.hub_name
--,do.shift_hour
        ,case
        WHEN do.shift_hour = 5 then '5 hour shift'
        WHEN do.shift_hour = 8 then '8 hour shift'
        WHEN do.shift_hour = 10 then '10 hour shift'
        ELSE 'HUB' END AS hub_type
--,case when do.uid is not null then 1 else 0 end as is_registered
,hi.max_drivers
,count(case when registration_status = 'Registered' or  registration_status =  'Worked' then do.uid else null end) as total_register

FROM
(SELECT date(from_unixtime(date_ts - 3600)) as date_,uid,slot_id
,case when registration_status = 1 then 'Registered'
      when registration_status = 2 then 'OFF'
      when registration_status = 3 then 'Worked'
      end as registration_status
,(end_time - start_time)/3600 as shift_hour

from foody.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ) do


--Shipper HUB
LEFT JOIN (SELECT *,case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end as report_date
FROM
shopeefood.foody_mart__profile_shipper_master) sm on sm.shipper_id = do.uid and sm.report_date =(case when do.date_ > sm.report_date then date(current_date)  
                                                                                                      else do.date_ end)
--HUB Locations
LEFT JOIN shopeefood.foody_internal_db__shipper_config_slot_tab__vn_daily_s0_live hi on hi.id = do.slot_id
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live ht on ht.id = hi.hub_id

where 1 = 1
and date_ >= date(current_date) 
and date_ < date(current_date) + interval '14' day 
and sm.shipper_type_id = 12
and sm.shipper_status_code = 1
and sm.city_name != 'Dien Bien'
--and ht.hub_name is null
GROUP BY 1,2,3,4,5,6
)
-----
----
,weekly as 

(SELECT cast((year(period)*100+week(period)) as VARCHAR) as period
,'2. Weekly' as period_gropup
,city_name
,hub_name
        ,case
        WHEN shift_hour = 5 then '5 hour shift'
        WHEN shift_hour = 8 then '8 hour shift'
        WHEN shift_hour = 10 then '10 hour shift'
        ELSE 'HUB' END AS hub_type
,sum(max_drivers)/count(distinct period) as max_drivers
,sum(total_register)/count(distinct period) as total_register 
FROM 
(SELECT do.date_ as period
,do.slot_id
,hi.id
,do.shift_hour
,sm.city_name
,ht.hub_name
        ,case
        WHEN do.shift_hour = 5 then '5 hour shift'
        WHEN do.shift_hour = 8 then '8 hour shift'
        WHEN do.shift_hour = 10 then '10 hour shift'
        ELSE 'HUB' END AS hub_type        
,hi.max_drivers
,count(distinct do.slot_id)
,count(case when registration_status = 'Registered' or  registration_status =  'Worked' then do.uid else null end)*1.00/count(distinct do.date_) as total_register
FROM
(SELECT date(from_unixtime(date_ts - 3600)) as date_,uid,slot_id
,case when registration_status = 1 then 'Registered'
      when registration_status = 2 then 'OFF'
      when registration_status = 3 then 'Worked'
      end as registration_status
,(end_time - start_time)/3600 as shift_hour

from foody.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ) do


--Shipper HUB
LEFT JOIN (SELECT *,case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end as report_date
FROM
shopeefood.foody_mart__profile_shipper_master) sm on sm.shipper_id = do.uid and sm.report_date =(case when do.date_ > sm.report_date then date(current_date)  
                                                                                                      else do.date_ end)
--HUB Locations
LEFT JOIN (SELECT date(from_unixtime(date_ts - 3600)) as date_ts
,id
,shift_category
,hub_id
,max_drivers
from shopeefood.foody_internal_db__shipper_config_slot_tab__vn_daily_s0_live
where 1 = 1
and config_status = 1
) hi on hi.id = do.slot_id
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live ht on ht.id = hi.hub_id 



where 1 = 1
and date_ >= date(current_date) 
and date_ < date(current_date) + interval '21' day 
and sm.shipper_type_id = 12
and sm.shipper_status_code = 1
and sm.city_name != 'Dien Bien'

GROUP BY 1,2,3,4,5,6,7,8)
GROUP BY 1,2,3,4,5)


SELECT *

FROM 
(SELECT a.*
from daily a
UNION ALL
SELECT b.*
from weekly b)

UNION ALL 

SELECT *

FROM 
(SELECT  a.period
        ,a.period_gropup
        ,'1.All' as city_name
        ,'1.All' as hub_name
        ,'1.All' as hub_type
        ,sum(a.max_drivers) as maxdrivers
        ,sum(a.total_register) as total_register

from daily a
GROUP BY 1,2,3,4,5

UNION ALL 
SELECT
      b.period
     ,b.period_gropup
     ,'1.All' as city_name
     ,'1.All' as hub_name
     ,'1.All' as hub_type
    ,sum(b.max_drivers) as maxdrivers
    ,sum(b.total_register) as total_register
from weekly b
GROUP BY 1,2,3,4,5
)



