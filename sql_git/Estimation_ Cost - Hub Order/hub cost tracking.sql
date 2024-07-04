with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2022-10-01',current_date - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(
-- SELECT 
--         '1. Daily'
--         ,CAST(report_date as varchar)
--         ,CAST(report_date as varchar)
--         ,CAST(report_date as varchar)
--         ,CAST(1 as double)

-- from raw_date
-- group by 1,2,3,4,5

-- UNION ALL 
-- SELECT 
--         '2. Weekly'
--         ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
--         ,CAST(date_trunc('week',report_date) as varchar)
--         -- ,CAST((date_trunc('week',report_date) + interval '7' day - interval '1' day) as varchar)
--         ,max(report_date)
--         ,CAST(date_diff('day',date_trunc('week',report_date),((date_trunc('week',report_date) + interval '7' day))) as double)

-- from raw_date

-- UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3

)
,hub_info as 
(select 
    date(from_unixtime(hub.report_date - 3600)) as report_date
    ,uid as shipper_id
    ,cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) as shift_category_name
    ,cast(json_extract(hub.extra_data,'$.total_order') as bigint) as total_order_inshift
    ,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
    else 0 end as extra_ship
    ,cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) as is_apply_fixed_amount -- check driver has order <= threshold and pass all kpi >> dieu kien de duoc bu min
    ,cast(json_extract(hub.extra_data,'$.total_income') as bigint) as total_income
    ,cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint) as calculated_shipping_shared
    -- ,hub.extra_data
    ,cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) as city_id
    ,case 
    	when cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) not in (217,218,220) then 999 
    	else cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) end as dummy_city_id
    ,cast(json_extract(hub.extra_data,'$.hub_ids') as array<int>) as hub_id
    ,cast(json_extract(hub.extra_data,'$.total_bonus') as bigint) as total_bonus
        , CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) AS hub_shift
    , CAST(json_extract(hub.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hub.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count
    , CAST(json_extract(hub.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 AS online_in_shift
    , CAST(json_extract(hub.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour
    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) start_shift
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) end_shift
    ,date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.00 as time_in_shift
    ,case 
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '10 hour shift' then 2
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '8 hour shift' then 2
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift' 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) > 6 then 1
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift' then 0             
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) > 6 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) < 20 then 1
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' then 0
        else null end as kpi_peak_hour


from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
)


select 
         p.period_group
        ,p.period 
        ,p.start_date
        ,p.end_date
        ,case when dummy_city_id = 217 then 'HCM' when dummy_city_id = 218 then 'HN' when dummy_city_id = 220 then 'HP' end as city_group   
        -- ,array_join(array_agg(distinct shift_category_name),',') as hub_name_agg
        ,shift_category_name
        ,count(shipper_id)/cast(p.days as double) as total_drivers
        ,count(case when extra_ship > 0 then shipper_id else null end)/cast(p.days as double) as total_driver_compensated
        ,sum(extra_ship)/cast(p.days as double) as total_extra_ship
        ,sum(total_bonus)/cast(p.days as double) as total_bonus        
        ,sum(total_order_inshift)/cast(p.days as double) as total_order_inshift
        ,p.days 





from hub_info hi 

inner join params_date p on hi.report_date between cast(p.start_date as date) and cast(p.end_date as date)

where hi.dummy_city_id != 999
-- and shift_category_name in ('8 hour shift','10 hour shift')


group by 1,2,3,4,5,6,p.days
