with base as 
(select shipper_id

        ,count(distinct report_date) as total_working_day
        
        ,count(distinct case when date_format(report_date,'%a') = 'Mon' then report_date else null end) as total_working_day_mon
        ,count(distinct case when date_format(report_date,'%a') = 'Tue' then report_date else null end) as total_working_day_tue
        ,count(distinct case when date_format(report_date,'%a') = 'Wed' then report_date else null end) as total_working_day_wed
        ,count(distinct case when date_format(report_date,'%a') = 'Thu' then report_date else null end) as total_working_day_thu
        ,count(distinct case when date_format(report_date,'%a') = 'Fri' then report_date else null end) as total_working_day_fri
        ,count(distinct case when date_format(report_date,'%a') = 'Sat' then report_date else null end) as total_working_day_sat
        ,count(distinct case when date_format(report_date,'%a') = 'Sun' then report_date else null end) as total_working_day_sun

        ,sum(total_online_time)/cast(count(distinct report_date) as double) as avg_online_time


from vnfdbi_opsndrivers.snp_foody_shipper_daily_report 

where report_date between current_date - interval '30' day and current_date - interval '1' day

group by 1

)
,driver as 

(
        select 
                sm.shipper_id
               ,sm.city_name
               ,current_driver_tier 


from shopeefood.foody_mart__profile_shipper_master sm 

---tier 
LEFT JOIN
(SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
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

where cast(from_unixtime(bonus.report_date - 60*60) as date) = date(current_date) - interval '1' day
)current on sm.shipper_id = current.shipper_id

where sm.grass_date = 'current'

)

,final as 
(select 
         a.shipper_id
        ,a.city_name
        ,a.current_driver_tier
        ,case when b.avg_online_time < 3 then '1. 0 - 3 hours'
              when b.avg_online_time < 5 then '2. 3 - 5 hours'
              when b.avg_online_time < 8 then '3. 5 - 8 hours'
              when b.avg_online_time < 10 then '4. 8 - 10 hours'
              when b.avg_online_time >= 10 then '5. >= 10 hours' end as online_range

        ,b.avg_online_time
        ,b.total_working_day
        ,b.total_working_day_mon
        ,b.total_working_day_tue
        ,b.total_working_day_wed 
        ,b.total_working_day_thu 
        ,b.total_working_day_fri 
        ,b.total_working_day_sat
        ,b.total_working_day_sun





from driver a 


left join base b on b.shipper_id = a.shipper_id) 


select * from final where shipper_id = 2996387