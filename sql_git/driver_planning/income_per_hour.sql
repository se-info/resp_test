-- WITH report_date AS
-- (SELECT
--     DATE(report_date) AS report_date
-- FROM
--     ((SELECT sequence(current_date - interval '35' day, current_date - interval '1' day) bar)
-- CROSS JOIN
--     unnest (bar) as t(report_date)
-- ))
-- , period AS
-- (SELECT
--     report_date
--     , '1. Weekly' AS period_group
--     , 'W' || CAST(WEEK(report_date) AS VARCHAR) AS period
--     , DATE_FORMAT(DATE_TRUNC('week', report_date), '%d-%b') AS explain_date
--     , DENSE_RANK() OVER (ORDER BY DATE_TRUNC('week', report_date) DESC) AS no
--     , CAST(7 AS DOUBLE) AS days
-- FROM
--     report_date
-- WHERE
--     report_date BETWEEN DATE_TRUNC('week', current_date) - interval '28' day AND  DATE_TRUNC('week', current_date) - interval '1' day

-- UNION ALL

-- SELECT
--     report_date
--     , '2. Daily' AS period_group
--     , DATE_FORMAT(report_date, '%Y-%m-%d') AS period
--     , DATE_FORMAT(report_date, '%a') AS explain_date
--     , DENSE_RANK() OVER (ORDER BY report_date DESC) AS no
--     , CAST(1 AS DOUBLE) AS days
-- FROM
--     report_date
-- WHERE
--     report_date BETWEEN current_date - interval '30' day AND current_date - interval '1' day
--     )
with hub_base as
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
    ,hub.extra_data
    ,cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) as city_id
    ,case 
        when cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) not in (217,218,220) then 999 
        else cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) end as dummy_city_id
    ,cast(json_extract(hub.extra_data,'$.hub_ids') as array<int>) as hub_id
    ,slot_id
from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
)
,hub_driver as
(select 
    report_date
    ,shipper_id
    ,array_agg(shift_category_name) as shift_arr
    ,cardinality(array_agg(shift_category_name)) as num_shift
from hub_base
-- where report_date = date '2023-03-26'
group by 1,2
)

,all_drivers AS 
(SELECT 
    d.report_date
    , d.shipper_id 
    , d.city_name 
    , CASE 
        WHEN d.city_name IN ('HCM City', 'Ha Noi City', 'Da Nang City') THEN 'T1'
        WHEN d.city_name IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') THEN 'T2'
    ELSE 'T3' END AS city_tier
    , case 
        when d.current_driver_tier = 'Hub' and hub.num_shift > 1 then '1. Hub-Multi-shift' 
        when d.current_driver_tier = 'Hub' and hub.num_shift = 1 then 
            case 
                when shift_arr[1] = '3 hour shift' then '3. HUB03'
                when shift_arr[1] = '5 hour shift' then '4. HUB05'
                when shift_arr[1] = '8 hour shift' then '5. HUB08'
                when shift_arr[1] = '10 hour shift' then '6. HUB10'
                end
        when d.current_driver_tier = 'Hub' and hub.num_shift is null then '2. Hub_outshift'
        else d.current_driver_tier end
        AS shipper_tier 
    , d.total_online_time AS online_time 
    , IF(d.cnt_total_order_delivered > 0, 1, 0) AS working_days
    , DATE(FROM_UNIXTIME(si.create_time - 3600)) AS onboard_date
    , IF(d.current_driver_tier = 'Hub', COALESCE(h.in_shift_work_time, 0), NULL) AS inshift_online_time
    , IF(d.current_driver_tier = 'Hub', COALESCE(d.total_online_time, 0) - COALESCE(h.in_shift_work_time, 0), NULL) AS outshift_online_time
    , COALESCE(i.total_income, 0) AS total_income
    , COALESCE(i.nonhub_income, 0) AS nonhub_income
    , COALESCE(i.hub_income, 0) AS hub_income
FROM vnfdbi_opsndrivers.snp_foody_shipper_daily_report d 
LEFT JOIN 
    (SELECT 
        partner_id AS shipper_id
        , date_ AS report_date
        , SUM(total_earning_before_tax) AS total_income
        , SUM(total_earning_non_hub) AS nonhub_income
        , SUM(total_earning_hub) AS hub_income
    FROM vnfdbi_opsndrivers.snp_foody_shipper_income_tab
    WHERE date_ BETWEEN date '2023-03-13' and date '2023-03-19'
    GROUP BY 1,2) i ON d.shipper_id = i.shipper_id AND d.report_date = i.report_date
LEFT JOIN vnfdbi_opsndrivers.snp_foody_hub_driver_report_tab h ON d.shipper_id = h.shipper_id AND d.report_date = h.report_date AND d.current_driver_tier = 'Hub'
LEFT JOIN shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live si ON d.shipper_id = si.uid 
LEFT JOIN hub_driver hub on d.shipper_id = hub.shipper_id and d.report_date= hub.report_date
WHERE 1=1
and d.report_date BETWEEN date '2023-03-13' and date '2023-03-19'
AND d.current_driver_tier != 'full-time'
)
, segment AS 
(SELECT 
    report_date
    , shipper_id 
    , city_name 
    , city_tier
    , shipper_tier
    -- , IF(DATE_DIFF('day', onboard_date, report_date) + 1 > 14, 'Existing', 'New') AS shipper_type
    , online_time 
    , working_days
    , total_income
FROM all_drivers
)

SELECT 
    s.city_tier
    , s.city_name
    , s.shipper_tier
    ,report_date
    -- , s.shipper_type
    -- , p.period_group
    -- , p.period || ' : ' || p.explain_date AS period
    , SUM(working_days) AS working_days
    , SUM(online_time) AS online_time
    , SUM(total_income) AS total_income
-- FROM period p 
-- LEFT JOIN segment s 
from segment s
-- ON p.report_date = s.report_date 
GROUP BY 1,2,3,4
HAVING SUM(online_time) > 0

UNION ALL 

SELECT 
    s.city_tier
    , 'All*' AS city_name
    , s.shipper_tier
    , s.report_date
    -- , s.shipper_type
    -- , p.period_group
    -- , p.period || ' : ' || p.explain_date AS period
    , SUM(working_days) AS working_days
    , SUM(online_time) AS online_time
    , SUM(total_income) AS total_income
-- FROM period p 
from  segment s 
-- ON p.report_date = s.report_date 
GROUP BY 1,2,3,4
HAVING SUM(online_time) > 0

-- select 
--     *
-- FROM vnfdbi_opsndrivers.snp_foody_shipper_income_tab
-- where current_driver_tier = 'Hub'
-- and grass_Date = date '2023-03-27'
-- limit 100

-- select 
--     *
-- from vnfdbi_opsndrivers.snp_foody_shipper_daily_report d 
-- limit 10

-- with 
-- limit 1000