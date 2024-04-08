WITH onboard_date AS
(SELECT
    uid AS shipper_id
    , DATE(FROM_UNIXTIME(create_time - 3600)) AS onboard_date
FROM shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live
WHERE DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2021-01-01' AND DATE'2021-12-21'
)
, main_shipper_type AS
(SELECT
    shipper_id
    , MAX_BY(shipper_type_id, count_days) AS main_shipper_type_id
FROM
    (SELECT
        o.shipper_id
        , sm.shipper_type_id
        , COUNT(DISTINCT sm.grass_date) AS count_days
    FROM onboard_date o
    LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON o.shipper_id = sm.shipper_id
    GROUP BY 1,2
    )
GROUP BY 1
)
, base AS
(SELECT
    shipper_id
    , last_delivered_date
    , SUM(cnt_total_order_delivered) AS l30d_delivered_order
    , COUNT(DISTINCT report_date) AS l30d_working_days
FROM
    (SELECT
        shipper_id
        , report_date
        , MAX(report_date) OVER (PARTITION BY shipper_id) AS last_delivered_date
        , COUNT(DISTINCT order_uid) AS cnt_total_order_delivered
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

            ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
                when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
            ,date(FROM_UNIXTIME(dot.submitted_time- 60*60)) created_date

            ,case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end as last_delivered_timestamp
            --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
            ,case when dot.pick_city_id = 217 then 'HCM'
                when dot.pick_city_id = 218 then 'HN'
                when dot.pick_city_id = 219 then 'DN'
                ELSE 'OTH' end as city_group
        FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
        LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
        WHERE dot.order_status = 400
        )
    GROUP BY 1,2
    )
WHERE report_date BETWEEN DATE'2021-12-22' - interval '30' day AND DATE'2021-12-22' - interval '1' day
GROUP BY 1,2
)

SELECT
    o.shipper_id
    , sip.national_id_number
    , sm1.city_name
    , o.onboard_date
    , DATE(FROM_UNIXTIME(sip.termination_date - 3600)) AS termination_date
    , CASE
        WHEN sm1.shipper_type_id = 1 then 'full_time'
        WHEN sm1.shipper_type_id = 2 then 'part_time'
        WHEN sm1.shipper_type_id = 3 then 'tester'
        WHEN sm1.shipper_type_id = 6 then 'part_time_09'
        WHEN sm1.shipper_type_id = 7 then 'part_time_11'
        WHEN sm1.shipper_type_id = 8 then 'part_time_12'
        WHEN sm1.shipper_type_id = 9 then 'part_time_14'
        WHEN sm1.shipper_type_id = 10 then 'part_time_15'
        WHEN sm1.shipper_type_id = 11 then 'part_time_16'
        WHEN sm1.shipper_type_id = 12 then 'Hub'
    ELSE NULL END AS current_driver_type
    , CASE
        WHEN m.main_shipper_type_id = 1 then 'full_time'
        WHEN m.main_shipper_type_id = 2 then 'part_time'
        WHEN m.main_shipper_type_id = 3 then 'tester'
        WHEN m.main_shipper_type_id = 6 then 'part_time_09'
        WHEN m.main_shipper_type_id = 7 then 'part_time_11'
        WHEN m.main_shipper_type_id = 8 then 'part_time_12'
        WHEN m.main_shipper_type_id = 9 then 'part_time_14'
        WHEN m.main_shipper_type_id = 10 then 'part_time_15'
        WHEN m.main_shipper_type_id = 11 then 'part_time_16'
        WHEN m.main_shipper_type_id = 12 then 'Hub'
    ELSE NULL END AS main_driver_type
    , if(sip.birth_date is not null, 2021 - sip.birth_date / 10000, null) as age
    , sr.worked_company AS previous_job
    , b.last_delivered_date
    , b.l30d_delivered_order
    , b.l30d_working_days
FROM onboard_date o
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm1 ON o.shipper_id = sm1.shipper_id AND sm1.grass_date = 'current'
LEFT JOIN main_shipper_type m ON o.shipper_id = m.shipper_id
LEFT JOIN base b ON o.shipper_id = b.shipper_id
LEFT JOIN shopeefood.foody_internal_db__shipper_info_personal_tab__reg_continuous_s0_live sip ON o.shipper_id = sip.uid
LEFT JOIN
    (SELECT
        id
        , last_name || ' ' || first_name AS driver_name
        , json_extract(extra_data, '$.shipper_uid') AS shipper_uid
        , json_extract(extra_data, '$.worked_companys.types') AS work_company_total
        , json_array_length(json_extract(extra_data, '$.worked_companys.types')) AS length
        , CASE
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 6) = true then 'Student'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 7) = true then 'Officer'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 3) = true then 'Baemin'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 1) = true then 'Grab'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 2) = true then 'Gojek'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 4) = true then 'BE'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 5) = true then 'Ahamove'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 8) = true then 'Other'
        ELSE 'Other' END AS worked_company
        , status
        , identity_number
    FROM shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live
    WHERE json_extract(extra_data, '$.shipper_uid') IS NOT NULL
    ) sr ON o.shipper_id = CAST(sr.shipper_uid AS BIGINT)