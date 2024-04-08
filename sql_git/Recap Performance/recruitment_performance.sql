with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2021-12-01',current_date - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,report_date
        ,report_date

from raw_date

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,date_trunc('week',report_date) 
        ,max(report_date)

from raw_date

group by 1,2,3

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)

from raw_date

group by 1,2,3
)
,driver_raw as 
(select 
       sm.shipper_id 
      ,case when sm.shipper_status_code = 1 then 'Normal' else 'Off' end as working_status
      ,date(from_unixtime(spp.create_time - 3600)) as onboard_date
      ,case when sm.shipper_status_code <> 1 then date(from_unixtime(quit.update_time - 3600)) else null end as quit_work_date

from shopeefood.foody_mart__profile_shipper_master sm 

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp on spp.uid = sm.shipper_id

left join shopeefood.foody_internal_db__shipper_quit_request_tab__reg_daily_s0_live quit on quit.uid = sm.shipper_id 

where sm.grass_date = 'current'
and sm.city_id in 
(217
,218
,219
,220
,221
,222
,223
,228
,230
,248
,254
,257
,263
,265
,271
,273)
)
,driver_final as 
(
SELECT
        onboard_date as date_
       ,'driver_onboard' as metrics 
       ,count(distinct shipper_id) as value  

FROM driver_raw 
GROUP BY 1,2

UNION ALL 

SELECT 
        quit_work_date as date_ 
       ,'driver_offboard' as metrics 
       ,count(distinct shipper_id) as value

FROM driver_raw
GROUP BY 1,2

)

select 
        p.period_group
       ,p.period  
       ,sum(case when metrics = 'driver_onboard' then value else null end ) as total_onboard_driver 
       ,sum(case when metrics = 'driver_offboard' then value else null end ) as total_offboard_driver 

from driver_final a 

inner join params_date p on a.date_ between p.start_date and p.end_date

where p.period_group = '3. Monthly'

group by 1,2