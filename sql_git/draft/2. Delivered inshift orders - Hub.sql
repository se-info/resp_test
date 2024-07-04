WITH  base AS (
SELECT dot.uid as shipper_id
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

      ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
            else date(from_unixtime(dot.submitted_time- 3600)) end as report_date
      ,date(from_unixtime(dot.submitted_time- 3600)) created_date
      ,if(dot.is_asap = 0, fa.last_auto_assign_timestamp, from_unixtime(dot.submitted_time- 3600)) as inflow_timestamp

      ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 3600) end as last_delivered_timestamp

      ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'HP'
            when dot.pick_city_id = 220 then 'HP'
            ELSE 'OTH' end as city_group
      ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
LEFT JOIN
        (
        SELECT   order_id , 0 as order_type
                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                ,max(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_auto_assign_timestamp
                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                where 1=1
                and grass_schema = 'foody_order_db'
                group by 1,2

        UNION ALL

        SELECT   ns.order_id, ns.order_type
                ,min(from_unixtime(create_time - 3600)) first_auto_assign_timestamp
                ,max(from_unixtime(create_time - 3600)) last_auto_assign_timestamp
                ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
        FROM
                ( SELECT order_id, order_type , create_time , update_time, status

                 from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                 where order_type in (4,5,6,7)
                 and grass_schema = 'foody_partner_archive_db'
                 UNION

                 SELECT order_id, order_type, create_time , update_time, status

                 from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                 where order_type in (4,5,6,7)
                 and schema = 'foody_partner_db'
                 ) ns
        GROUP BY 1,2
        ) fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type
WHERE dot.order_status = 400
)
, min_fee (inflow_date, start_time, end_time, min_fee) AS (
VALUES
    (DATE'2022-01-28', 1030, 1231, 15000)
    , (DATE'2022-01-28', 1700, 2001, 15000)

    , (DATE'2022-01-29', 1030, 1231, 15000)
    , (DATE'2022-01-29', 1700, 2001, 15000)

    , (DATE'2022-01-30', 1030, 1231, 15000)
    , (DATE'2022-01-30', 1700, 2001, 15000)

    , (DATE'2022-01-31', 1030, 1231, 20000)
    , (DATE'2022-01-31', 1700, 2000, 20000)
    , (DATE'2022-01-31', 2000, 2231, 15000)

    , (DATE'2022-02-01', 0630, 1030, 17000)
    , (DATE'2022-02-01', 1030, 1231, 20000)
    , (DATE'2022-02-01', 1700, 2000, 20000)
    , (DATE'2022-02-01', 2000, 2231, 15000)

    , (DATE'2022-02-02', 0630, 1030, 17000)
    , (DATE'2022-02-02', 1030, 1231, 20000)
    , (DATE'2022-02-02', 1700, 2000, 20000)
    , (DATE'2022-02-02', 2000, 2231, 15000)

    , (DATE'2022-02-03', 0630, 1030, 17000)
    , (DATE'2022-02-03', 1030, 1231, 20000)
    , (DATE'2022-02-03', 1700, 2000, 20000)
    , (DATE'2022-02-03', 2000, 2231, 15000)

    , (DATE'2022-02-04', 1030, 1231, 15000)
    , (DATE'2022-02-04', 1700, 2001, 17000)

    , (DATE'2022-02-05', 1030, 1231, 15000)
    , (DATE'2022-02-05', 1700, 2001, 17000)

    , (DATE'2022-02-06', 1030, 1231, 16000)
    , (DATE'2022-02-06', 1700, 2001, 17000)
)
, base1 AS
(SELECT
    base.shipper_id
    , sm.shipper_name
    , sm.city_name
    , IF(sm.shipper_type_id = 11, 'Non-hub', 'Hub') AS is_hub
    , m.start_time, m.end_time, m.min_fee, DATE(base.inflow_timestamp) AS inflow_date
    , COUNT(DISTINCT base.order_uid) AS nowfood_delivered_orders
    , COUNT(DISTINCT base.order_uid) * m.min_fee AS total_min_fee
FROM base
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON base.shipper_id = sm.shipper_id AND sm.grass_date = 'current'
INNER JOIN min_fee m
ON DATE(base.inflow_timestamp) = m.inflow_date
AND HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) >= m.start_time
AND HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) < m.end_time
AND sm.shipper_type_id IN (11,12)
AND base.driver_payment_policy = 2
AND base.source = 'order_delivery'
GROUP BY 1,2,3,4,5,6,7,8
)
SELECT
    shipper_id
    , shipper_name
    , city_name
    , is_hub
    , inflow_date
    ,  SUM(IF(start_time = 0630 AND end_time = 1030, nowfood_delivered_orders, 0)) AS "NowFood inshift orders (6h30-10h30)"
    ,  SUM(IF(start_time = 1030 AND end_time = 1231, nowfood_delivered_orders, 0)) AS "NowFood inshift orders (10h30-12h30)"
    ,  SUM(IF(start_time = 1700 AND end_time IN (2000, 2001), nowfood_delivered_orders, 0)) AS "NowFood inshift orders (17h30-20h00)"
    ,  SUM(IF(start_time = 2000 AND end_time = 2231, nowfood_delivered_orders, 0)) AS "NowFood inshift orders (20h00-22h30)"

    ,  SUM(IF(start_time = 0630 AND end_time = 1030, total_min_fee, 0)) AS "NowFood total min fee (6h30-10h30)"
    ,  SUM(IF(start_time = 1030 AND end_time = 1231, total_min_fee, 0)) AS "NowFood total min fee (10h30-12h30)"
    ,  SUM(IF(start_time = 1700 AND end_time IN (2000, 2001), total_min_fee, 0)) AS "NowFood total min fee (17h30-20h00)"
    ,  SUM(IF(start_time = 2000 AND end_time = 2231, total_min_fee, 0)) AS "NowFood total min fee (20h00-22h30)"

    , SUM(IF(start_time = 0630 AND end_time = 1030, total_min_fee, 0))
    + SUM(IF(start_time = 1030 AND end_time = 1231, total_min_fee, 0))
    + SUM(IF(start_time = 1700 AND end_time IN (2000, 2001), total_min_fee, 0))
    + SUM(IF(start_time = 2000 AND end_time = 2231, total_min_fee, 0)) AS "Total min fee"
FROM base1
GROUP BY 1,2,3,4,5
ORDER BY 1,5