with future as 
(SELECT 
cast((year(do.date_)*100+week(do.date_)) as VARCHAR) as period
,do.uid
,'1. Future' as period_group
,sm.city_name
,ht.hub_name
--,do.shift_hour
        ,case
        WHEN do.shift_hour = 5 then '5 hour shift'
        WHEN do.shift_hour = 8 then '8 hour shift'
        WHEN do.shift_hour = 10 then '10 hour shift'
        ELSE 'HUB' END AS hub_type
--,wt.is_published          
,count(case when registration_status != 'OFF' then do.uid else null end) as total_register

FROM
(SELECT date(from_unixtime(date_ts - 3600)) as date_,uid
,case when registration_status = 1 then 'Registered'
      when registration_status = 2 then 'OFF'
      when registration_status = 3 then 'Worked'
      end as registration_status
,(end_time - start_time)/3600 as shift_hour
,slot_id,id
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
and date_ >= date(current_date) 
and date_ < date(current_date) + interval '30' day 
and sm.shipper_type_id = 12
and sm.shipper_status_code = 1
and sm.city_name != 'Dien Bien'
GROUP BY 1,2,3,4,5,6)
---
---
,historical as 

(SELECT 
cast((year(do.date_)*100+week(do.date_)) as VARCHAR) as period
,do.uid
,'2. Historical' as period_group
,sm.city_name
,ht.hub_name
--,do.shift_hour
        ,case
        WHEN do.shift_hour = 5 then '5 hour shift'
        WHEN do.shift_hour = 8 then '8 hour shift'
        WHEN do.shift_hour = 10 then '10 hour shift'
        ELSE 'HUB' END AS hub_type
        
,count(case when registration_status != 'OFF' then do.uid else null end) as total_register

FROM
(SELECT date(from_unixtime(date_ts - 3600)) as date_,uid
,case when registration_status = 1 then 'Registered'
      when registration_status = 2 then 'OFF'
      when registration_status = 3 then 'Worked'
      end as registration_status
,(end_time - start_time)/3600 as shift_hour
,slot_id
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
and date_ >= date(current_date) - interval '21' day 
and date_ < date(current_date) 
and date_ >= date('2021-10-21')
and sm.shipper_type_id = 12
and sm.shipper_status_code = 1
and sm.city_name != 'Dien Bien'
GROUP BY 1,2,3,4,5,6)
---
---
,actual as 

(SELECT c.period,c.city_name,c.hub_name,c.hub_type,c.period_group,c.group_
,'2. Driver Active' as group_metrics
,count(c.uid) as total_driver

from
(SELECT cast((year(period)*100+week(period)) as VARCHAR) as period,period_group,city_name,hub_name,hub_type,uid
,case when count(case when is_work = 1 then uid else null end) = 1  then 'b. 1 days'
      when count(case when is_work = 1 then uid else null end) = 2 then 'c. 2 days'
      when count(case when is_work = 1 then uid else null end) = 3 then 'd. 3 days'
      when count(case when is_work = 1 then uid else null end) =4 then 'e. 4 days'
      when count(case when is_work = 1 then uid else null end) =5 then 'f. 5 days' 
      when count(case when is_work = 1 then uid else null end) =6 then 'g. 6 days'
      when count(case when is_work = 1 then uid else null end) =7 then 'h. 7 days'
      else 'a. No Registered' end as group_
FROM 
(SELECT
do.date_ as period
,do.uid
,'2. Historical' as period_group
,sm.city_name
,ht.hub_name
--,do.shift_hour
        ,case
        WHEN do.shift_hour = 5 then '5 hour shift'
        WHEN do.shift_hour = 8 then '8 hour shift'
        WHEN do.shift_hour = 10 then '10 hour shift'
        ELSE 'HUB' END AS hub_type
        ,registration_status
,ir.total_order
,case when ir.total_order >0 and registration_status = 'Worked' then 1 
      else 0 end as is_work
--,count(case when registration_status = 'Worked' then do.uid else null end) as actual_driver        
--,count(case when registration_status != 'OFF' then do.uid else null end) as total_register
FROM
(SELECT date(from_unixtime(date_ts - 3600)) as date_,uid
,case when registration_status = 1 then 'Registered'
      when registration_status = 2 then 'OFF'
      when registration_status = 3 then 'Worked'
      end as registration_status
,(end_time - start_time)/3600 as shift_hour
,slot_id
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
--HUB ORDER
LEFT JOIN (SELECT base.report_date
,base.uid
,count(case when base.is_inshift = 1 then base.ref_order_id else null end ) as total_order
FROM
(SELECT dot.uid
,dot.ref_order_id
,case
WHEN dot.pick_city_id = 217 then 'HCM'
WHEN dot.pick_city_id = 218 then 'HN'
ELSE NULL end as city_group
,psm.city_name
,date(from_unixtime(dot.real_drop_time -3600)) as report_date
,case when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-07-09') and date('2021-10-05') and psm.shipper_type_id = 12 and dot.pick_city_id = 217 then 1
when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-07-24') and date('2021-10-04') and psm.shipper_type_id = 12 and dot.pick_city_id = 218 then 1
when cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_inshift
FROM foody.foody_partner_db__driver_order_tab dot

LEFT JOIN
(
select *
,Case
when grass_date = 'current' then date(current_date)
else cast(grass_date AS DATE ) END as report_date
from shopeefood.foody_mart__profile_shipper_master
) psm on psm.shipper_id = dot.uid AND psm.report_date = date(from_unixtime(dot.real_drop_time -3600))
left join foody.foody_order_db__order_completed_tab oct on oct.id = dot.ref_order_id
LEFT JOIN foody.foody_partner_db__driver_order_extra_tab doet on dot.id = doet.order_id

LEFT JOIN ( SELECT
id,date_format(from_unixtime(start_time - 25200),'%H') as start_time
,date_diff('hour',date_trunc('hour',from_unixtime(start_time - 3600)),date_trunc('hour',from_unixtime(end_time-3600))) as shift_hour
FROM foody.foody_internal_db__shipper_shift_tab) sst on sst.id = psm.shipper_shift_id

WHERE 1=1
AND dot.pick_city_id in (217,218)
AND psm.shipper_type_id in (12)
and dot.ref_order_category = 0
and psm.city_id in (217,218)
AND dot.ref_order_status in (7,9,11)
GROUP by 1,2,3,4,5,6)base
where base.is_inshift = 1
GROUP BY 1,2) ir on ir.uid = do.uid and ir.report_date = do.date_


 

where 1 = 1
and date_ >= date(current_date) - interval '21' day 
and date_ < date(current_date) 
and date_ >= date('2021-10-21')
and sm.shipper_type_id = 12
and sm.shipper_status_code = 1
and sm.city_name != 'Dien Bien'
GROUP BY 1,2,3,4,5,6,7,8

)
GROUP BY 1,2,3,4,5,6)c
group by 1,2,3,4,5,6,7)



SELECT *

FROM 

(SELECT
a.period,a.city_name,a.hub_name,a.hub_type,a.period_group
,case when a.total_register =1 then 'b. 1 days'
      when a.total_register =2 then 'c. 2 days'
      when a.total_register =3 then 'd. 3 days'
      when a.total_register =4 then 'e. 4 days'
      when a.total_register =5 then 'f. 5 days' 
      when a.total_register =6 then 'g. 6 days'
      when a.total_register =7 then 'h. 7 days'
      else 'a. No Registered' end as group_
      
,'1. Driver Register' as group_metrics      
,count(a.uid) as total_driver

from future a 
where 1 = 1 
group by 1,2,3,4,5,6

UNION

select 
b.period,b.city_name,b.hub_name,b.hub_type,b.period_group
,case when b.total_register =1 then 'b. 1 days'
      when b.total_register =2 then 'c. 2 days'
      when b.total_register =3 then 'd. 3 days'
      when b.total_register =4 then 'e. 4 days'
      when b.total_register =5 then 'f. 5 days' 
      when b.total_register =6 then 'g. 6 days'
      when b.total_register =7 then 'h. 7 days'
      else 'a. No Registered' end as group_
,'1. Driver Register' as group_metrics
,count(b.uid) as total_driver
from historical b
where 1 = 1 
group by 1,2,3,4,5,6)

UNION

SELECT *
from actual