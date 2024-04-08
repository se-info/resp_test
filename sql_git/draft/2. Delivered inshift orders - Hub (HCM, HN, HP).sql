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
            when dot.pick_city_id = 219 then 'DN'
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
SELECT
    base.shipper_id
    , sm.shipper_name
    , base.city_group AS city_name
    , IF(sm.shipper_type_id = 11, 'Non-hub', 'Hub') AS is_hub
    , COUNT(DISTINCT IF(HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) >= 1030 AND HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) <= 1230 AND base.source = 'order_delivery', base.order_uid, NULL)) AS nowfood_orders_1030am_1230pm
    , COUNT(DISTINCT IF(HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) >= 1700 AND HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) <= 2000 AND base.source = 'order_delivery', base.order_uid, NULL)) AS nowfood_orders_17pm_20pm
    , COUNT(DISTINCT IF(HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) >= 1030 AND HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) <= 1230 AND base.source != 'order_delivery', base.order_uid, NULL)) AS nowship_orders_1030am_1230pm
    , COUNT(DISTINCT IF(HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) >= 1700 AND HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) <= 2000 AND base.source != 'order_delivery', base.order_uid, NULL)) AS nowship_orders_17pm_20pm
--     , COUNT(DISTINCT IF(HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) >= 2000 AND HOUR(base.inflow_timestamp)*100+ MINUTE(base.inflow_timestamp) <= 2230, base.order_uid, NULL)) AS delivered_orders_20pm_2230pm
FROM base
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON base.shipper_id = sm.shipper_id AND TRY_CAST(sm.grass_date AS DATE) = DATE'2022-01-15'
WHERE sm.shipper_type_id IN (11,12)
AND DATE(base.inflow_timestamp) = DATE'2022-01-15'
AND base.city_group IN ('HCM', 'HN', 'HP')
AND base.driver_payment_policy = 2
GROUP BY 1,2,3,4