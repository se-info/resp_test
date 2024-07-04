SELECT a.*,sm.shipper_name,sm.city_name 

FROM 
(SELECT bonus.report_date
      ,bonus.shipper_id 
      ,bonus.current_driver_tier
      ,past.current_driver_tier as past_tier 
      ,past.report_date as last_date_past_tier
      ,row_number()over(partition by bonus.shipper_id order by past.report_date desc) as rank


FROM 
(SELECT 
 cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
,bonus.uid as shipper_id
,case when hub.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else null end as current_driver_tier
,bonus.total_point
,bonus.daily_point

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

---Check Hub
LEFT JOIN
(SELECT shipper_id
,shipper_type_id
,case when grass_date = 'current' then date(current_date)
else cast(grass_date as date) end as report_date

from shopeefood.foody_mart__profile_shipper_master

where 1=1
and (grass_date = 'current' OR cast(grass_date as date) >= date('2019-01-01'))
GROUP BY 1,2,3
)hub on hub.shipper_id = bonus.uid and hub.report_date = cast(from_unixtime(bonus.report_date - 60*60) as date)

)bonus
---Check Past Tier 

LEFT JOIN    
( SELECT 
         report_date 
        ,shipper_id 
        ,current_driver_tier
        ,row_number()over(partition by shipper_id,current_driver_tier order by report_date desc) as rank


FROM     
(SELECT 
 cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
,bonus.uid as shipper_id
,case when hub.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else null end as current_driver_tier
,bonus.total_point
,bonus.daily_point

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
LEFT JOIN
(SELECT shipper_id
,shipper_type_id
,case when grass_date = 'current' then date(current_date)
else cast(grass_date as date) end as report_date

from shopeefood.foody_mart__profile_shipper_master

where 1=1
and (grass_date = 'current' OR cast(grass_date as date) >= date('2019-01-01'))
GROUP BY 1,2,3
)hub on hub.shipper_id = bonus.uid and hub.report_date = cast(from_unixtime(bonus.report_date - 60*60) as date)
)
--where shipper_id = 15924981

group by 1,2,3
order by report_date desc
)past on past.shipper_id = bonus.shipper_id and past.current_driver_tier <> bonus.current_driver_tier


where 1 = 1 
and bonus.report_date = current_date - interval '1' day
and past.rank = 1
--and bonus.shipper_id = 17301502

group by 1,2,3,4,5
)a 
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id 

where rank = 1 
--and current_driver_tier = 'T4'
and sm.grass_date = 'current'
and sm.shipper_status_code = 1

