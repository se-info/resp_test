with data_ as 
(SELECT 
--cast((year(do.date_)*100+week(do.date_)) as VARCHAR) as period
do.uid
,'1. Weekly' as period_group
,sm.city_name
--,do.shift_hour
        ,case
        WHEN filter.shift_hour = 5 then '5 hour shift'
        WHEN filter.shift_hour = 8 then '8 hour shift'
        WHEN filter.shift_hour = 10 then '10 hour shift'
        ELSE 'HUB' END AS hub_type
--,wt.is_published          
,sum(do.is_work_1212) as is_work_1212
,count(case when do.registration_status != 'OFF' then do.uid else null end) as total_register

FROM
(SELECT date(from_unixtime(date_ts - 3600)) as date_,uid
,year(date(from_unixtime(date_ts - 3600)))*100+week(date(from_unixtime(date_ts - 3600))) as week_
,case when registration_status = 1 then 'Registered'
      when registration_status = 2 then 'OFF'
      when registration_status = 3 then 'Worked'
      end as registration_status
,(end_time - start_time)/3600 as shift_hour
,case when date(from_unixtime(date_ts - 3600)) = date('2021-12-12') and registration_status != 2 then 1 
      else 0 end as is_work_1212
,slot_id,id
from foody.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
) do
---filter do 
left join
(SELECT date(from_unixtime(date_ts - 3600)) as date_,uid
,year(date(from_unixtime(date_ts - 3600)))*100+week(date(from_unixtime(date_ts - 3600))) as week_
,case when registration_status = 1 then 'Registered'
      when registration_status = 2 then 'OFF'
      when registration_status = 3 then 'Worked'
      end as registration_status
,(end_time - start_time)/3600 as shift_hour
,slot_id,id,row_number() over(partition by uid,year(date(from_unixtime(date_ts - 3600)))*100+week(date(from_unixtime(date_ts - 3600))) order by date(from_unixtime(date_ts - 3600)) desc) as rank_ 
from foody.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
) filter on filter.uid = do.uid and filter.week_ = do.week_ and filter.rank_ = 1


--Shipper HUB
LEFT JOIN (SELECT *,case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end as report_date
FROM
shopeefood.foody_mart__profile_shipper_master) sm on sm.shipper_id = do.uid and sm.report_date =(case when do.date_ > sm.report_date then date(current_date)  
                                                                                                      else do.date_ end)
--HUB Locations
--LEFT JOIN shopeefood.foody_internal_db__shipper_config_slot_tab__vn_daily_s0_live hi on hi.id = do.slot_id
--LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live ht on ht.id = hi.hub_id


where 1 = 1
--and do.date_ >= date(current_date) - interval '7' day
--and do.date_ < date(current_date) + interval '30' day
and do.date_ between date('2022-01-31') and date('2022-02-06')
and do.date_ >= date('2021-10-21')
and sm.shipper_type_id = 12
and sm.shipper_status_code = 1
and sm.city_name != 'Dien Bien'
GROUP BY 1,2,3,4
)





SELECT *

FROM 

(SELECT
a.period,a.city_name,a.hub_type,a.period_group
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

from data_ a 
where 1 = 1 
group by 1,2,3,4,5)