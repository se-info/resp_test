WITH hub_onboard AS
(SELECT
    shipper_id
    , shipper_ranking - type_ranking AS groupx_
    , MIN(report_date) AS first_join_hub
    , MAX(report_date) AS last_drop_hub
FROM
    (SELECT
        shipper_id
        , shipper_type_id
        , DATE(grass_date) AS report_date
        , RANK() OVER (PARTITION BY shipper_id ORDER BY DATE(grass_date)) AS shipper_ranking
        , RANK() OVER (PARTITION BY shipper_id, shipper_type_id ORDER BY DATE(grass_date)) AS type_ranking
    FROM shopeefood.foody_mart__profile_shipper_master
    WHERE shipper_type_id IN (12, 11)
    AND grass_date != 'current'
    )
WHERE shipper_type_id = 12
GROUP BY 1,2
)

-- select 
--     *
-- from hub_onboard
-- limit 10


, hub_new_onboard AS
(SELECT
    shipper_id
    , MIN(first_join_hub) AS first_join_hub
    , MAX(last_drop_hub) AS last_drop_hub
FROM
    (SELECT
        shipper_id
        , first_join_hub 
        , last_drop_hub
    FROM hub_onboard
    -- WHERE first_join_hub BETWEEN DATE'2022-06-13' AND DATE'2022-06-26'
    where first_join_hub < date '2022-06-13'
    )
GROUP BY 1
)

-- select 
--     *
-- from hub_new_onboard
-- limit 10

, grass_date AS
(SELECT
    grass_date AS report_date
FROM
    ((SELECT sequence(DATE'2022-06-13', DATE'2022-07-20') bar)
CROSS JOIN
    unnest (bar) as t(grass_date)
))
,orders as
(
    SELECT
        dot.uid as shipper_id
        ,dot.ref_order_id as order_id
        ,dot.ref_order_code as order_code
        ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
        ,dot.ref_order_category
        ,case when dot.ref_order_category = 0 then 'order_delivery'
            when dot.ref_order_category = 3 then 'now_moto'
            when dot.ref_order_category = 4 then 'now_ship'
            when dot.ref_order_category = 5 then 'now_ship'
            when dot.ref_order_category = 6 then 'now_ship_shopee'
            when dot.ref_order_category = 7 then 'now_ship_sameday'
            else null end source
        ,dot.ref_order_status
        ,dot.order_status
        ,case when dot.order_status = 1 then 'Pending'
            when dot.order_status in (100,101,102) then 'Assigning'
            when dot.order_status in (200,201,202,203,204) then 'Processing'
            when dot.order_status in (300,301) then 'Error'
            when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
            else null end as order_status_group

        ,dot.is_asap

        ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 3600))
            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
            else date(FROM_UNIXTIME(dot.submitted_time- 3600)) end as report_date
        ,date(FROM_UNIXTIME(dot.submitted_time- 3600)) created_date

        ,case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 3600) end as last_delivered_timestamp
        --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
        ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            ELSE 'OTH' end as city_group
        ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
    WHERE dot.order_status = 400
    AND case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 3600))
            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
            else date(FROM_UNIXTIME(dot.submitted_time- 3600)) end BETWEEN DATE'2022-06-13' AND DATE'2022-07-20'
)
, inshift_orders AS
(SELECT
    shipper_id
    , report_date
    , COUNT(DISTINCT order_uid) AS delivered_orders
    , COUNT(DISTINCT IF(driver_payment_policy = 2, order_uid, NULL)) AS inshift_delivered_orders
FROM orders
GROUP BY 1,2
)

,base_order as
(select 
    shipper_id
    ,array_agg(report_date) report_date_agg
    ,array_agg(inshift_delivered_orders) as inshift_orders_agg
from inshift_orders
where inshift_delivered_orders >0 
group by 1
)
,filter_list as
(select 
    shipper_id
    ,report_date_agg
    ,cardinality(filter(report_date_agg, x -> x between date '2022-06-13' and date '2022-06-26')) = 0 is_rule1
    ,cardinality(filter(report_date_agg, x -> x between date '2022-06-27' and date '2022-07-20')) > 0 is_rule2
    ,array_min(filter(report_date_agg, x -> x between date '2022-06-27' and date '2022-07-07')) as first_day_have_order
from base_order
)
, kpi_qualified AS
(SELECT
    hc.uid AS shipper_id
    , DATE(FROM_UNIXTIME(hc.report_date - 3600)) AS report_date
    , CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) AS hub_shift
    , CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count
    , CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600.00 AS online_in_shift
    , CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour
    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) start_shift
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) end_shift
    ,date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
            , from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.0000 as time_in_shift
    , CASE
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '10 hour shift'
            AND CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) = 0
            -- AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 10 >= 0.9
            AND (CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600.00) / 
                (date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600), from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.0000)
                >= 0.9
            AND CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 >= 2
            AND regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') THEN 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '8 hour shift'
            AND CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) = 0
            -- AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 8 >= 0.9
            AND (CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600.00) / 
                (date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600), from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.0000)
                >= 0.9
            AND CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 >= 2
            AND regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') THEN 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift'
            AND CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) = 0
            -- AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 5 >= 0.9
            AND (CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600.00) / 
                (date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600), from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.0000)
                >= 0.9
            AND CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600.00 >= 1
            AND regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') THEN 1
        ELSE 0
    END AS is_qualified_kpi
FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
WHERE DATE(FROM_UNIXTIME(hc.report_date - 3600)) BETWEEN DATE'2022-06-27' AND DATE'2022-07-22'
)
,kpi_qualified_v2 as
(select 
    shipper_id
    ,report_date
    ,hub_shift
    ,deny_count
    ,ignore_count
    ,online_in_shift
    ,online_peak_hour
    ,is_auto_accept
    ,start_shift
    ,end_shift
    ,time_in_shift
    ,cast(trim(substr(hub_shift,1,2)) as int) as shift_type
    ,case when deny_count = 0 
        and ignore_count = 0 
        and online_in_shift/time_in_shift >= 0.9
        and is_auto_accept = true
        and online_peak_hour >= 
            case when hub_shift in ('10 hour shift','8 hour shift') then 2
                when hub_shift = '5 hour shift' and hour(start_shift) != 6 then 1
                when hub_shift = '3 hour shift' and hour(start_shift) in (10,18,17) then 1
                else 0 end
        then 1 else 0
        end as is_qualified_kpi

from kpi_qualified
)
-- select 
--     *
-- from kpi_qualified_v2

,raw as
(select 
    dr.shipper_id
    ,sm.shipper_name
    ,sm.city_name
    ,dr.first_day_have_order as start_scheme_day
    ,dr.first_day_have_order + interval '13' day as end_scheme_day
    ,kpi.is_qualified_kpi
    ,kpi.hub_shift
    ,cardinality(filter(report_date_agg, x -> x = kpi.report_date )) inshift_delivered_orders
    ,case when cardinality(filter(report_date_agg, x -> x = date '2022-07-07' )) > 0 and kpi.is_qualified_kpi = 1 then 1 else 0 end is_qualified_on_7jul
from filter_list dr
inner join kpi_qualified_v2 kpi
    on dr.shipper_id = kpi.shipper_id and kpi.report_date between dr.first_day_have_order and dr.first_day_have_order + interval '13' day
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON dr.shipper_id = sm.shipper_id and dr.first_day_have_order = try_cast(sm.grass_date as date)
-- inner join inshift_orders as o
--     on dr.shipper_id = o.shipper_id and o.report_date between dr.first_day_have_order and dr.first_day_have_order + interval '13' day
where dr.is_rule1 = true and dr.is_rule2 = true
)
select 
    shipper_id
    ,shipper_name
    ,city_name
    ,start_scheme_day
    ,end_scheme_day
    ,is_qualified_on_7jul
    ,array_agg(distinct hub_shift)
    ,sum(case when is_qualified_kpi = 1 and inshift_delivered_orders > 0 then 1 else 0 end) as total_day_eligible
from raw
group by 1,2,3,4,5,6
