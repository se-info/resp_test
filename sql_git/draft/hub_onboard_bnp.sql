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
    WHERE first_join_hub BETWEEN DATE'2022-02-24' AND DATE'2022-03-10'
    )
GROUP BY 1
)
, grass_date AS
(SELECT
    grass_date AS report_date
FROM
    ((SELECT sequence(DATE'2022-02-24', DATE'2022-03-23') bar)
CROSS JOIN
    unnest (bar) as t(grass_date)
))
, hub_date AS
(SELECT
    h.shipper_id
    , h.first_join_hub
    , h.last_drop_hub
    , g.report_date
    , IF(sm.shipper_type_id = 12, 1, 0) AS is_hub
FROM hub_new_onboard h
LEFT JOIN grass_date g ON g.report_date BETWEEN h.first_join_hub AND h.last_drop_hub
LEFT JOIN (SELECT shipper_id, shipper_type_id, grass_date FROM shopeefood.foody_mart__profile_shipper_master WHERE grass_date != 'current') sm ON h.shipper_id = sm.shipper_id AND TRY_CAST(grass_date AS DATE) = g.report_date
WHERE g.report_date BETWEEN h.first_join_hub AND h.first_join_hub + interval '13' day -- limit within 14 days from onboard date
)
, kpi_qualified AS
(SELECT
    hc.uid AS shipper_id
    , DATE(FROM_UNIXTIME(hc.report_date - 3600)) AS report_date
    , CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) AS hub_shift
    , CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count
    , CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 AS online_in_shift
    , CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour
    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept
    , CASE
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '10 hour shift'
            AND CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 10 >= 0.9
            AND CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 >= 2
            AND regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') THEN 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '8 hour shift'
            AND CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 8 >= 0.9
            AND CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 >= 2
            AND regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') THEN 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift'
            AND CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 5 >= 0.9
            AND CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 >= 1
            AND regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') THEN 1
        ELSE 0
    END AS is_qualified_kpi
FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
WHERE DATE(FROM_UNIXTIME(hc.report_date - 3600)) BETWEEN DATE'2022-02-24' AND DATE'2022-03-23'
)
,data_meet_kpi_all_working_day as 
(select distinct
    h.shipper_id
    ,h.first_join_hub
    ,kpi.report_date
    ,kpi.is_qualified_kpi
    -- ,case when count(distinct case when is_qualified_kpi is not null then kpi.report_date else null end ) = sum(is_qualified_kpi) then 1 else 0 end as is_meet_kpi_all_wkday
    -- ,is_qualified_kpi
from hub_date h
left join kpi_qualified kpi
    on h.shipper_id = kpi.shipper_id and kpi.report_date between h.first_join_hub and h.first_join_hub + interval '13' day
where first_join_hub between h.first_join_hub and h.first_join_hub + interval '13' day
-- group by 1,2
)
,meet_kpi_all_wkd as
(select 
    shipper_id
    ,case when count(distinct report_date) = sum(is_qualified_kpi) then 1 else 0 end as is_meet_kpi_all_wkday
from data_meet_kpi_all_working_day
group by 1
)
, inshift_orders AS
(SELECT
    shipper_id
    , report_date
    , COUNT(DISTINCT order_uid) AS delivered_orders
    , COUNT(DISTINCT IF(driver_payment_policy = 2, order_uid, NULL)) AS inshift_delivered_orders
FROM
    (SELECT
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
            else date(FROM_UNIXTIME(dot.submitted_time- 3600)) end BETWEEN DATE'2022-02-24' AND DATE'2022-03-23'
    )
GROUP BY 1,2
)
, all_ AS
(SELECT
    h.shipper_id
    , sm.shipper_name
    , sm.city_name
    , IF(ho.shipper_id IS NOT NULL, 1, 0) AS is_onboard_before_feb24
    , h.first_join_hub
    , h.last_drop_hub
    , h.report_date
    , h.is_hub
    , kpi.hub_shift
    , kpi.deny_count
    , kpi.ignore_count
    , kpi.online_in_shift
    , kpi.online_peak_hour
    , kpi.is_auto_accept
    , kpi.is_qualified_kpi
    , COALESCE(o.delivered_orders, 0) AS delivered_orders
    , COALESCE(o.inshift_delivered_orders, 0) AS inshift_delivered_orders
    , IF(COALESCE(o.inshift_delivered_orders, 0) > 0 AND kpi.is_qualified_kpi = 1, 1, 0) AS eligible_days
    , coalesce(mkpi.is_meet_kpi_all_wkday,0) as is_meet_kpi_all_wkday
    -- , is_qualified_kpi
FROM hub_date h
LEFT JOIN inshift_orders o ON h.shipper_id = o.shipper_id AND h.report_date = o.report_date
LEFT JOIN kpi_qualified kpi ON h.shipper_id = kpi.shipper_id AND h.report_date = kpi.report_date
LEFT JOIN (SELECT shipper_id, shipper_name, city_name FROM shopeefood.foody_mart__profile_shipper_master WHERE grass_date = 'current') sm ON h.shipper_id = sm.shipper_id
LEFT JOIN (SELECT shipper_id FROM hub_onboard WHERE first_join_hub < DATE'2022-02-24' GROUP BY 1) ho ON h.shipper_id = ho.shipper_id
LEFT JOIN meet_kpi_all_wkd AS mkpi on h.shipper_id = mkpi.shipper_id
WHERE sm.city_name IN ('HCM City', 'Ha Noi City')
)

SELECT
    all_.shipper_id
    , shipper_name
    , city_name
    , is_onboard_before_feb24
    , all_.first_join_hub
    , all_.first_join_hub + interval '13' day AS end_bonus_date
    , last_drop_hub
    , report_date
    , is_hub
    , hub_shift
    , h.hub_shift_final
    , deny_count
    , ignore_count
    , online_in_shift
    , online_peak_hour
    , is_auto_accept
    , is_qualified_kpi
    , delivered_orders
    , inshift_delivered_orders
    , eligible_days
    , SUM(eligible_days) OVER (PARTITION BY  all_.shipper_id ORDER BY report_date ASC) AS total_eligible_days
    -- , case when count(distinct case when is_qualified_kpi is not null then report_date else null end ) = sum(is_qualified_kpi) then 1 else 0 end as is_meet_kpi_all_wkday
FROM all_
LEFT JOIN -- latest hub shift
    (SELECT
        shipper_id
        , MAX_BY(hub_shift, report_date) AS hub_shift_final
    FROM all_
    WHERE hub_shift IS NOT NULL
    GROUP BY 1) h ON all_.shipper_id = h.shipper_id

where all_.is_meet_kpi_all_wkday = 1

/*
2022.03.10 -- adjust logic meet_kpi_all_wkd
2022.03.14 -- fix logic meet_kpi_all_wkd > chi apply trong period first_join_hub > first_join_hub + 13 day
link communicate to drivers: https://gofast.vn/tin-tuc/hcm-hn-mo-ung-dung-lien-tay-nhan-uu-dai-500k/
*/