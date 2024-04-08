WITH
  raw AS (
   SELECT *
   FROM
     (
      SELECT
        ns.order_id
      , ns.order_type
      , ns.status
      , (CASE WHEN (ns.order_type = 0) THEN 'Food/Market' WHEN (ns.order_type = 4) THEN 'NowShip Instant' WHEN (ns.order_type = 5) THEN 'NowShip Food Mex' WHEN (ns.order_type = 6) THEN 'NowShip Shopee' WHEN (ns.order_type = 7) THEN 'NowShip Same Day' WHEN (ns.order_type = 8) THEN 'NowShip Multi Drop' WHEN ((ns.order_type = 200) AND (ogi.ref_order_category = 0)) THEN 'Food/Market' WHEN ((ns.order_type = 200) AND (ogi.ref_order_category = 6)) THEN 'NowShip Shopee' WHEN ((ns.order_type = 200) AND (ogi.ref_order_category = 7)) THEN 'NowShip Same Day' ELSE 'Others' END) order_source
      , (CASE WHEN (ns.order_type <> 200) THEN ns.order_type ELSE ogi.ref_order_category END) order_category
      , CASE WHEN (ns.order_type = 200) THEN '1. Group Order' 
             WHEN COALESCE(dot.group_id, 0) > 0 THEN '2. Stack Order' 
             ELSE '3. Single Order' 
             END AS order_group_type
      , ns.city_id
      , city.name_en city_name
      , (CASE WHEN (ns.city_id = 217) THEN 'HCM' WHEN (ns.city_id = 218) THEN 'HN' WHEN (ns.city_id = 219) THEN 'DN' ELSE 'OTH' END) city_group
      , from_unixtime(ns.create_time - 3600) create_time
      , from_unixtime(ns.update_time - 3600) update_time
      , "date"("from_unixtime"((ns.create_time - (60 * 60)))) date_
      , (CASE WHEN (CAST("from_unixtime"((ns.create_time - (60 * 60))) AS date) BETWEEN "date"('2019-12-30') AND "date"('2019-12-31')) THEN 202001 WHEN (CAST("from_unixtime"((ns.create_time - (60 * 60))) AS date) BETWEEN "date"('2021-01-01') AND "date"('2021-01-03')) THEN 202053 ELSE (("year"(CAST("from_unixtime"((ns.create_time - (60 * 60))) AS date)) * 100) + "week"(CAST("from_unixtime"((ns.create_time - (60 * 60))) AS date))) END) year_week
      , (CASE WHEN ((ns.order_type = 200) AND (ogi.ref_order_category = 0)) THEN COALESCE(g.food_service, 'NA') WHEN (ns.order_type = 0) THEN COALESCE(s.food_service, 'NA') ELSE 'NowShip' END) food_service
    --   , (CASE WHEN (ns.order_type <> 200) THEN 1 ELSE COALESCE(order_rank.total_order_in_group_at_start, 0) END) total_order_in_group
    --   , (CASE WHEN (ns.order_type <> 200) THEN 1 ELSE COALESCE(order_rank.total_order_in_group_actual_del, 0) END) total_order_in_group_actual_del
      , COALESCE(order_rank.total_order_in_group_at_start, 0)  total_order_in_group
      , COALESCE(order_rank.total_order_in_group_actual_del, 0) total_order_in_group_actual_del
      , ns.shipper_uid shipper_id
      FROM
        (
         SELECT
           order_id
         , order_type
         , create_time
         , assign_type
         , update_time
         , status
         , city_id
         , shipper_uid
         FROM
           shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
         WHERE 1 = 1 AND status IN (3, 4, 8, 9, 2, 14, 15, 17, 18)
UNION          SELECT
           order_id
         , order_type
         , create_time
         , assign_type
         , update_time
         , status
         , city_id
         , shipper_uid
         FROM
           shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
         WHERE 1 = 1 AND status IN (3, 4, 8, 9, 2, 14, 15, 17, 18)
      )  ns
      LEFT JOIN (
         SELECT
           id
         , ref_order_category
         FROM
           shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live
         GROUP BY 1, 2
      )  ogi ON ogi.id > 0 AND ogi.id = (CASE WHEN ns.order_type = 200 THEN ns.order_id ELSE 0 END)
      LEFT JOIN (
         SELECT
           ref_order_id
         , ref_order_category
         , group_id
         FROM
           shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live
         WHERE (grass_schema = 'foody_partner_db')
         GROUP BY 1, 2, 3
      )  dot ON dot.ref_order_id = ns.order_id AND ns.order_type <> 200 AND ns.order_type = dot.ref_order_category
      LEFT JOIN (
         SELECT
           ogm.group_id
         , ogi.group_code
         , count(DISTINCT ogm.ref_order_id) total_order_in_group
         , count(DISTINCT (CASE WHEN (ogi.create_time = ogm.create_time) THEN ogm.ref_order_id ELSE null END)) total_order_in_group_at_start
         , count(DISTINCT (CASE WHEN ((ogi.create_time = ogm.create_time) AND (ogm.mapping_status = 11)) THEN ogm.ref_order_id ELSE null END)) total_order_in_group_actual_del
         FROM
           shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm
         LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi ON ogi.id = ogm.group_id
         WHERE 1 = 1 AND ogm.group_id IS NOT NULL
         GROUP BY 1, 2
      )  order_rank ON order_rank.group_id = ((CASE WHEN (ns.order_type = 200) THEN ns.order_id ELSE dot.group_id END))
      LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city ON (city.id = ns.city_id) AND (city.country_id = 86)

      LEFT JOIN (
         SELECT
           dot.ref_order_id
         , dot.ref_order_category
         , CASE WHEN (go.now_service_category_id = 1) THEN 'Food' 
                WHEN (go.now_service_category_id > 0) THEN 'Fresh/Market' 
                ELSE 'Others'
                END AS food_service
         FROM
           shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
         LEFT JOIN (
            SELECT
              id
            , now_service_category_id
            FROM
              shopeefood.foody_mart__fact_gross_order_join_detail
            WHERE (grass_region = 'VN')
            GROUP BY 1, 2
         )  go ON go.id = dot.ref_order_id AND dot.ref_order_category = 0
         WHERE 1 = 1 
         AND dot.ref_order_category = 0 
         AND go.now_service_category_id >= 0
         GROUP BY 1, 2, 3
      )  s ON s.ref_order_id = ns.order_id AND ns.order_type = 0 AND ns.order_type = dot.ref_order_category
      LEFT JOIN (
         SELECT
           ogm.group_id
         , ogm.ref_order_category
         , (CASE WHEN (go.now_service_category_id = 1) THEN 'Food' WHEN (go.now_service_category_id > 0) THEN 'Fresh/Market' ELSE 'Others' END) food_service
         FROM
           shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm
         LEFT JOIN (
            SELECT
              id
            , now_service_category_id
            FROM
              shopeefood.foody_mart__fact_gross_order_join_detail
            WHERE (grass_region = 'VN')
            GROUP BY 1, 2
         )  go ON (go.id = ogm.ref_order_id) AND (ogm.ref_order_category = 0)
         WHERE 1 = 1 AND (ogm.ref_order_category = 0) AND (COALESCE(ogm.group_id, 0) > 0) AND (go.now_service_category_id >= 0)
         GROUP BY 1, 2, 3
      )  g ON g.group_id = ns.order_id AND ns.order_type = 200 AND (CASE WHEN (ns.order_type <> 200) THEN ns.order_type ELSE ogi.ref_order_category END )= 0
      
      WHERE 1 = 1 
      AND date(from_unixtime(ns.create_time -3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day
      AND ns.order_type <> 200 
      AND ns.city_id <> 238
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
     )
    WHERE 1 = 1 
    AND order_group_type = '2. Stack Order' 
) 
, group_assign AS (
   SELECT
     a.*
   , COALESCE(c.total_driver_deny, 0) no_deny
   , COALESCE(c.total_driver_fault_deny, 0) no_deny_driver_fault
   , (CASE WHEN (b.order_id > 0) THEN 1 ELSE 0 END) is_driver_accept
   FROM
     (
      SELECT
        order_id
      , order_source
      , food_service
      , shipper_id
      , date_
      , total_order_in_group
      , total_order_in_group_actual_del
      , city_group
      , city_name
      , count(order_id) no_assign
      , count((CASE WHEN (status IN (3, 4)) THEN order_id ELSE null END)) no_incharged
      , count((CASE WHEN (status IN (8, 9, 17, 18)) THEN order_id ELSE null END)) no_ignored
      FROM
        raw
      WHERE 1 = 1 
      and date_ between current_date - interval '30' day and current_date - interval '1' day
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
   )  a
   LEFT JOIN (
      SELECT
        order_id
      , order_source
      , food_service
      FROM
        raw
      WHERE (status IN (3, 4))
      GROUP BY 1, 2, 3
   )  b ON a.order_id = b.order_id AND a.order_source = b.order_source AND a.food_service = b.food_service
   LEFT JOIN (
      SELECT
        group_id
      , uid
      , count(DISTINCT (CASE WHEN ((is_deny_driver_fault = 1) AND (order_create_time = group_create_time)) THEN order_id ELSE null END)) total_driver_fault_deny
      , count(DISTINCT (CASE WHEN (order_create_time = group_create_time) THEN order_id ELSE null END)) total_driver_deny
      FROM
        (
         SELECT
           dod.*
         , group_info.ref_order_id
         , group_info.ref_order_category
         , group_info.group_id
         , (CASE WHEN (dod.deny_type = 1) THEN 1 ELSE 0 END) is_deny_driver_fault
         , group_info.create_time order_create_time
         , group_info.group_create_time
         FROM
           shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod
         LEFT JOIN (
            SELECT
              ogm.*
            , ogi.create_time group_create_time
            FROM
              (
                (
               SELECT *
               FROM
                 shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live
               WHERE (grass_schema = 'foody_partner_db')
            )  ogm

            LEFT JOIN (
               SELECT *
               FROM
                 shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live
               WHERE (grass_schema = 'foody_partner_db')
            )  ogi ON (ogi.id = ogm.group_id)
            )
            WHERE 1 = 1 
         )  group_info ON group_info.order_id = dod.order_id
         WHERE (1 = 1)
      )  deny
      GROUP BY 1, 2
   )  c ON a.order_id = c.group_id AND a.shipper_id = c.uid
) 
-- , group_actual AS (
--    SELECT
--      base.city_group
--    , base.city_name
--    , base.date_
--    , (CASE WHEN (order_category = 0) THEN 'Food/Market' WHEN (order_category = 6) THEN 'NowShip Shopee' WHEN (order_category = 7) THEN 'NowShip Same Day' ELSE null END) order_source
--    , total_order_in_group
--    , sum(total_order_in_group) total_group_order
--    , count(DISTINCT group_uid) total_group
--    FROM
--      (
--       SELECT
--         id
--       , (CASE WHEN (city_id = 217) THEN 'HCM' WHEN (city_id = 218) THEN 'HN' WHEN (city_id = 219) THEN 'DN' ELSE 'OTH' END) city_group
--       , city_name
--       , CAST(grass_date AS date) date_
--       , order_category
--       , group_uid
--       , group_data
--       , CAST("json_extract"(group_data, '$.num_orders') AS bigint) total_order_in_group
--       FROM
--         ((
--          SELECT
--            asb.id
--          , "json_extract"(asb.processing_info, '$.ds_stack_response.unassigned_orders') group_jobs
--          , CAST("json_extract"(asb.processing_info, '$.order_category') AS bigint) order_category
--          , city.name_en city_name
--          , asb.city_id
--          , asb.grass_date
--         --  select * 
--          FROM shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab__reg_daily_s0_live asb
--          LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city ON (asb.city_id = city.id) AND (city.country_id = 86)
--          WHERE 1 = 1 
--          AND asb.city_id <> 238 
--         --  AND asb.processing_info LIKE '%order_category%'
--          AND asb.processing_info LIKE '%ds_stack_response%' --processing jobs
--         --  AND asb.processing_info LIKE '%grouped_jobs%'
--       ) 
--       CROSS JOIN UNNEST(CAST(group_jobs AS map(varchar,json))) x (group_uid, group_data))
--       WHERE (1 = 1)
--       GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
--    )  base
--    GROUP BY 1, 2, 3, 4, 5
-- ) 
, metrics AS
(
--    SELECT
--      '1. Order grouped by system' section
--    , order_source
--    , date_
--    , city_group
--    , city_name
--    , total_order_in_group
--    , "sum"(total_group) kpi
--    FROM
--      group_actual
--    GROUP BY 1, 2, 3, 4, 5, 6
SELECT
     '1. Order stacked assign driver' section
   , order_source
   , date_
   , city_group
   , city_name
--    , total_order_in_group
   , "sum"(no_assign) kpi
   FROM
     group_assign
   GROUP BY 1, 2, 3, 4, 5
UNION ALL    SELECT
     '2. Order stacked driver accept' section
   , order_source
   , date_
   , city_group
   , city_name
--    , total_order_in_group
   , "count"(DISTINCT (CASE WHEN (is_driver_accept = 1) THEN order_id ELSE null END)) kpi
   FROM
     group_assign
   GROUP BY 1, 2, 3, 4, 5
UNION ALL    SELECT
     (CASE WHEN (total_order_in_group_actual_del = 0) THEN '3.1. Order stacked - complete 0 order' 
           WHEN (total_order_in_group_actual_del < total_order_in_group) THEN '3.2. Order stacked - complete >=1 order & deny/cancel >=1 order' 
           ELSE '3.3. Order stacked - complete all orders' END) section
   , order_source
   , date_
   , city_group
   , city_name
--    , total_order_in_group
   , "count"(DISTINCT (CASE WHEN (is_driver_accept = 1) THEN order_id ELSE null END)) kpi
   FROM
     group_assign
   GROUP BY 1, 2, 3, 4, 5
) 
SELECT
  m1.section
, m1.order_source
, m1.date_
, m1.city_group
, m1.city_name
-- , m1.total_order_in_group
, m1.kpi
-- , m2.kpi system_kpi

FROM metrics m1

-- LEFT JOIN metrics m2 ON m2.section = '1. Order grouped by system' 
--                     AND m1.order_source = m2.order_source 
--                     AND m1.date_ = m2.date_ 
--                     AND m1.city_group = m2.city_group
--                     AND m1.city_name = m2.city_name
--                     AND m1.total_order_in_group = m2.total_order_in_group
where (m1.date_ between date'2022-08-26' and date'2022-08-27') 





-- select min(grass_date) from shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab__reg_daily_s0_live
