with base as 
(SELECT t1 as point_start
      , t1 + 100 as point_end
      ,case when t1 between 0 and 1800 then 'T1' 
            when t1 between 1801 and 3600 then 'T2'
            when t1 between 3601 and 5400 then 'T3'
            when t1 between 5401 and 8400 then 'T4'
            when t1 >= 8401 then 'T5' else null end as tier


from  (
    (select sequence(0, 30000, 100) t )

cross join 
      unnest (t) as t(t1) 
      )

)

,bonus as 

(SELECT 
        cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
        ,bonus.uid as shipper_id
        ,case when hub.shipper_type_id = 12 then 'Hub'
        when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
        when bonus.tier in (3,8,13) then 'T3'
        when bonus.tier in (4,9,14) then 'T4'
        when bonus.tier in (5,10,15) then 'T5'
        else null end as current_driver_tier
        ,bonus.total_point as l30_points
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

)

,final as 
(select 
         a.shipper_id
        ,sm.city_name 
        ,a.report_date 
        ,b.point_start as range_
        ,a.current_driver_tier as current_tier 
        ,case when a.current_driver_tier = 'T1' then 'T2'
              when a.current_driver_tier = 'T2' then 'T3'
              when a.current_driver_tier = 'T3' then 'T4'
              when a.current_driver_tier = 'T4' then 'T5'
              when a.current_driver_tier = 'T5' then 'Max tier' end as tier_reach 
        ,a.l30_points 




from bonus a 


left join base b on a.l30_points >= point_start and a.l30_points < point_end

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and sm.grass_date = 'current'

where a.current_driver_tier != 'Hub'

-- and a.shipper_id = 1146567 
and sm.city_name in ('HCM City','Ha Noi City')
and sm.shipper_status_code = 1 

and a.report_date between current_date - interval '1' day and current_date - interval '1' day
)


select  report_date
       ,city_name 
       ,range_ as range_point
       ,current_tier
       ,tier_reach as continue_tier 
       ,count(distinct shipper_id) as total_driver


from final 


group by 1,2,3,4,5