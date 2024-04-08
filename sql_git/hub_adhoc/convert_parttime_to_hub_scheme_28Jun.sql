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
(
SELECT
    shipper_id
    , MIN(first_join_hub) AS first_join_hub
    , MAX(last_drop_hub) AS last_drop_hub
FROM
    (SELECT
        shipper_id
        , first_join_hub
        , last_drop_hub
    FROM hub_onboard
    WHERE first_join_hub BETWEEN DATE'2022-07-30' AND DATE'2022-08-07'
    )
GROUP BY 1
)
, grass_date AS
(SELECT
    grass_date AS report_date
FROM
    ((SELECT sequence(DATE'2022-07-30', DATE'2022-08-20') bar)
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

-- check driver_tier before join hub --> get previous 1 day before join hub
,hub_date_v2 as
(select 
    hub.shipper_id
    ,hub.first_join_hub
    ,hub.last_drop_hub
    ,hub.report_date
    ,hub.is_hub
    ,case 
        when sm.shipper_type_id = 12 then 'Hub'
        when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
        when bonus.tier in (2,7,12) then 'T2'
        when bonus.tier in (3,8,13) then 'T3'
        when bonus.tier in (4,9,14) then 'T4'
        when bonus.tier in (5,10,15) then 'T5'
        else null end as driver_tier_before_join_hub
    ,sm.city_name
from hub_date hub
left join shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
    on hub.shipper_id = bonus.uid and cast(from_unixtime(bonus.report_date - 3600) as date) = hub.first_join_hub - interval '1' day
left join shopeefood.foody_mart__profile_shipper_master sm
    on hub.shipper_id = sm.shipper_id and hub.first_join_hub - interval '1' day = try_cast(sm.grass_date as date)
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
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) start_shift
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) end_shift
    ,date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
            , from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.00 as time_in_shift
    ,case 
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '10 hour shift' then 2
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '8 hour shift' then 2
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift' then 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' 
            and (hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) <= 11
                or hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) <= 18) then 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' then 0
        else null end as kpi_peak_hour
FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
WHERE DATE(FROM_UNIXTIME(hc.report_date - 3600)) BETWEEN DATE'2022-07-30' AND DATE'2022-08-20'
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
            else date(FROM_UNIXTIME(dot.submitted_time- 3600)) end BETWEEN DATE'2022-07-30' AND DATE'2022-08-20'
    )
GROUP BY 1,2
)
, all_ AS
(SELECT
    h.shipper_id
    , sm.shipper_name
    , sm.city_name
    , IF(ho.shipper_id IS NOT NULL, 1, 0) AS is_onboard_before_may25
    , h.first_join_hub
    , h.first_join_hub + interval '13' day AS end_bonus_date
    , h.last_drop_hub
    , h.report_date
    , h.is_hub
    , h.driver_tier_before_join_hub
    , kpi.hub_shift
    , kpi.deny_count
    , kpi.ignore_count
    , kpi.online_in_shift
    , kpi.online_peak_hour
    , kpi.kpi_peak_hour
    , kpi.is_auto_accept
    , kpi.start_shift
    , kpi.end_shift
    , kpi.time_in_shift
    , COALESCE(o.delivered_orders, 0) AS delivered_orders
    , COALESCE(o.inshift_delivered_orders, 0) AS inshift_delivered_orders
    , case 
        when kpi.online_in_shift/kpi.time_in_shift >=0.9
        and kpi.deny_count = 0
        and kpi.ignore_count = 0
        and kpi.is_auto_accept = true
        and kpi.online_peak_hour >= kpi.kpi_peak_hour
        then 1 else 0 end is_qualified_kpi
    , case 
        when kpi.online_in_shift/kpi.time_in_shift >=0.9
        and kpi.deny_count = 0
        and kpi.ignore_count = 0
        and kpi.is_auto_accept = true
        and kpi.online_peak_hour >= kpi.kpi_peak_hour
        and COALESCE(o.inshift_delivered_orders, 0) > 0
        then 1 else 0 end is_eligible_day
        -- and COALESCE(o.inshift_delivered_orders, 0) > 0 
    -- , IF(COALESCE(o.inshift_delivered_orders, 0) > 0 AND kpi.is_qualified_kpi = 1, 1, 0) AS eligible_days
    -- , coalesce(mkpi.is_meet_kpi_all_wkday,0) as is_meet_kpi_all_wkday
    -- , is_qualified_kpi
FROM hub_date_v2 h
LEFT JOIN inshift_orders o ON h.shipper_id = o.shipper_id AND h.report_date = o.report_date
LEFT JOIN kpi_qualified kpi ON h.shipper_id = kpi.shipper_id AND h.report_date = kpi.report_date
LEFT JOIN (SELECT shipper_id, shipper_name, city_name FROM shopeefood.foody_mart__profile_shipper_master WHERE grass_date = 'current') sm ON h.shipper_id = sm.shipper_id
LEFT JOIN (SELECT shipper_id FROM hub_onboard WHERE first_join_hub < DATE'2022-07-30' GROUP BY 1) ho ON h.shipper_id = ho.shipper_id
-- LEFT JOIN meet_kpi_all_wkd AS mkpi on h.shipper_id = mkpi.shipper_id
-- WHERE sm.city_name IN ('HCM City', 'Ha Noi City','Hai Phong City')
WHERE sm.city_name IN ('HCM City', 'Ha Noi City')
-- and kpi.hub_shift in ('3 hour shift','5 hour shift','8 hour shift')
)
,final as
(select 
    all_.shipper_id
    ,all_.shipper_name
    ,all_.city_name
    ,all_.driver_tier_before_join_hub
    ,all_.is_onboard_before_jul30
    ,all_.first_join_hub
    ,all_.end_bonus_date
    ,sum(is_eligible_day) as total_eligible_days
    ,sum(case when report_date = date '2022-08-08' and inshift_delivered_orders>0 then 1 else 0 end) is_have_order_08_aug
    ,array_agg(distinct hub_shift) as list_hub_worked
from all_
group by 1,2,3,4,5,6,7
)
select 
    f.shipper_id
    ,f.shipper_name
    ,f.city_name
    ,f.driver_tier_before_join_hub
    ,f.is_onboard_before_jul30
    ,f.first_join_hub
    ,f.end_bonus_date
    ,f.total_eligible_days
    ,f.is_have_order_08_aug
    ,f.list_hub_worked
    ,case
        when total_eligible_days >=6 and is_have_order_08_aug >= 1 and is_onboard_before_jul30 = 0 and driver_tier_before_join_hub in ('T1','T2','T3') then 100000
        when total_eligible_days >=6 and is_have_order_08_aug >= 1 and is_onboard_before_jul30 = 0 and driver_tier_before_join_hub in ('T4','T5') then 250000
        when total_eligible_days >=6 and is_have_order_08_aug >= 1 and is_onboard_before_jul30 = 0 and driver_tier_before_join_hub is null and f.city_name in ('Hai Phong City') then 100000
        else 0 end as bonus_scheme

from final f
-- where total_eligible_days >=6
-- and is_have_order_06_jun >= 1
-- and is_onboard_before_may25 = 0 -- remove join hub before scheme
/*
peak hour: 11h - 12h vs 18h - 19h

Applied time: 25-5 > 05-06 +9 ngay = 2022-06-18

link: https://gofast.vn/tin-tuc/thuong-nong-khi-gia-nhap-hub-truoc-31-5-2022/
áp dụng: TPHCM, HN, DN
doi tuong: tai xe da từng là part time và gia nhập hub từ 25-05 -> 05-06
đăng kí hoạt động và có ít nhất 1 đơn vào 06.06
đạt tối thiểu KPI 6 ngày trở lên trong 14 ngày tam gia Hub first_join: 25-05 > 05-06 > end_bonus_date = 07-06 > 18-06
Hub all type hub

*/