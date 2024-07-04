with raw as 
(SELECT  
        shipper_id
       ,shipper_rank - shipper_tier_rank as group_change 
       ,MIN(report_date) as first_tier 
       ,MAX(report_date) as last_tier 

FROM       
(SELECT bonus.report_date
      ,bonus.shipper_id 
      ,bonus.current_driver_tier
      ,row_number()over(partition by bonus.shipper_id order by bonus.report_date) as shipper_rank
      ,row_number()over(partition by bonus.shipper_id,bonus.current_driver_tier order by bonus.report_date) as shipper_tier_rank



FROM 
(SELECT 
 cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
,bonus.uid as shipper_id
,case when sm.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else null end as current_driver_tier
,bonus.total_point
,bonus.daily_point

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

---Check Hub
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = bonus.uid 
        and try_cast(sm.grass_date as date) = cast(from_unixtime(bonus.report_date - 60*60) as date)
)bonus
)
GROUP BY 1,2
)
,tier as 
(SELECT 
 cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
,bonus.uid as shipper_id
,case when sm.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else null end as current_driver_tier
,bonus.total_point
,bonus.daily_point

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

---Check Hub
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = bonus.uid 
        and try_cast(sm.grass_date as date) = cast(from_unixtime(bonus.report_date - 60*60) as date)
)

SELECT * 

FROM
(select
       r.shipper_id 
      ,r.first_tier - interval '1' day as past_tier_last_date
      ,COALESCE(t1.current_driver_tier,'T1') as past_tier  
      ,r.last_tier
      ,t2.current_driver_tier as last_tier_date
      ,row_number()over(partition by r.shipper_id order by first_tier desc) as rank_ 

from raw r 

left join tier t1 on t1.shipper_id = r.shipper_id and t1.report_date = r.first_tier - interval '1' day

left join tier t2 on t2.shipper_id = r.shipper_id and t2.report_date = r.last_tier

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = r.shipper_id and sm.grass_date = 'current'

WHERE 1 = 1 
AND sm.shipper_status_code = 1 
)
WHERE 1 = 1 
AND rank_ = 1 