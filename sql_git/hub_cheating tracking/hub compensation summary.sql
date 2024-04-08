with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2022-12-20',current_date - interval '1' day) bar)

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

-- UNION ALL 
-- SELECT 
--         '2. Weekly'
--         ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
--         ,date_trunc('week',report_date) 
--         ,max(report_date)

-- from raw_date

-- group by 1,2,3

-- UNION 

-- SELECT 
--         '3. Monthly'
--         ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
--         ,date_trunc('month',report_date)
--         ,max(report_date)

-- from raw_date

-- group by 1,2,3
) 

,hub_income as
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
from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
-- where date(from_unixtime(hub.report_date - 3600)) between current_date - interval '120' day and current_date - interval '1' day
)

select 
         p.period_group
        ,p.period 
        ,p.start_date
        ,p.end_date
        ,case when dummy_city_id = 217 then 'HCM' when dummy_city_id = 218 then 'HN' when dummy_city_id = 220 then 'HP' end as city_group   
        -- ,array_join(array_agg(distinct shift_category_name),',') as hub_name_agg
        -- ,shift_category_name
        ,sum(extra_ship) as total_extra_ship
        ,sum(total_order_inshift) as total_order_inshift
        -- ,p.days 





from hub_income hi 

inner join params_date p on hi.report_date between cast(p.start_date as date) and cast(p.end_date as date)

where hi.dummy_city_id != 999
-- and shift_category_name in ('8 hour shift','10 hour shift')


group by 1,2,3,4,5
