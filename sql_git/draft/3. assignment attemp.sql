WITH params(period, start_date, end_date, days) AS (
    VALUES
    (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day), '%b'), DATE_TRUNC('month', current_date - interval '1' day), current_date - interval '1' day, CAST(DAY(current_date - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day), 'W%v'), DATE_TRUNC('week', current_date - interval '1' day), current_date - interval '1' day, CAST(DATE_DIFF('day', DATE_TRUNC('week', current_date - interval '1' day), current_date) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '1' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '8' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '15' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '22' day, CAST(7 AS DOUBLE))
    , (CAST(current_date - interval '1' day AS VARCHAR), current_date - interval '1' day, current_date - interval '1' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '2' day AS VARCHAR), current_date - interval '2' day, current_date - interval '2' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '3' day AS VARCHAR), current_date - interval '3' day, current_date - interval '3' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '4' day AS VARCHAR), current_date - interval '4' day, current_date - interval '4' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '5' day AS VARCHAR), current_date - interval '5' day, current_date - interval '5' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '6' day AS VARCHAR), current_date - interval '6' day, current_date - interval '6' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '7' day AS VARCHAR), current_date - interval '7' day, current_date - interval '7' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '8' day AS VARCHAR), current_date - interval '8' day, current_date - interval '8' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '9' day AS VARCHAR), current_date - interval '9' day, current_date - interval '9' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '10' day AS VARCHAR), current_date - interval '10' day, current_date - interval '10' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '11' day AS VARCHAR), current_date - interval '11' day, current_date - interval '11' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '12' day AS VARCHAR), current_date - interval '12' day, current_date - interval '12' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '13' day AS VARCHAR), current_date - interval '13' day, current_date - interval '13' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '14' day AS VARCHAR), current_date - interval '14' day, current_date - interval '14' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '15' day AS VARCHAR), current_date - interval '15' day, current_date - interval '15' day, CAST(1 AS DOUBLE))
    )
, grass_date AS (
SELECT
    grass_date
FROM
    ((SELECT sequence(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, current_date - interval '1' day) bar)
CROSS JOIN
    unnest (bar) as t(grass_date)
))
, daily AS (
SELECT order_source
      ,food_service
      ,date_
      ,sum(no_assign) sum_total_assign
      ,sum(no_incharged) sum_total_incharged
      ,sum(no_ignored) sum_total_ignored
      ,sum(no_deny) sum_total_deny
FROM
(
SELECT   ns.order_id, ns.order_type, ns.order_category, order_group_type
        ,ns.order_source
        ,ns.city_name
        ,ns.city_group
        ,ns.food_service
        ,min(ns.date_) date_
        ,count(ns.order_id) no_assign
        ,count(case when status in (3,4) then order_id else null end) no_incharged
        ,count(case when status in (8,9,17,18) then order_id else null end) no_ignored
        ,count(case when status in (2,14,15) then order_id else null end) no_deny
FROM
(
SELECT   ns.order_id
        ,ns.order_type
        ,ns.status
        ,case when ns.order_type = 0 then '1. Food/Market'
                when ns.order_type = 4 then '2. NowShip Instant'
                when ns.order_type = 5 then '3. NowShip Food Mex'
                when ns.order_type = 6 then '4. NowShip Shopee'
                when ns.order_type = 7 then '5. NowShip Same Day'
                when ns.order_type = 8 then '6. NowShip Multi Drop'
                when ns.order_type = 200 and ogi.ref_order_category = 0 then '1. Food/Market'
                when ns.order_type = 200 and ogi.ref_order_category = 6 then '4. NowShip Shopee'
                when ns.order_type = 200 and ogi.ref_order_category = 7 then '5. NowShip Same Day'
                else 'Others' end as order_source
        ,case when ns.order_type <> 200 then ns.order_type else ogi.ref_order_category end as order_category
        ,case when ns.order_type = 200 then '1. Group Order'
              when coalesce(dot.group_id,0) > 0 then '2. Stack Order' else '3. Single Order' end as order_group_type
        ,ns.city_id
        ,city.name_en as city_name
        ,case when ns.city_id  = 217 then 'HCM'
            when ns.city_id  = 218 then 'HN'
            when ns.city_id  = 219 then 'DN' else 'OTH'
            end as city_group
        ,from_unixtime(ns.create_time - 3600) as create_time
        ,from_unixtime(ns.update_time - 3600) as update_time
        ,date(from_unixtime(ns.create_time - 3600)) as date_
        ,case when ns.order_type = 200 and ogi.ref_order_category = 0 then coalesce(g.food_service,'NA')
              when ns.order_type = 0 then coalesce(s.food_service,'NA')
              else 'NowShip' end as food_service
FROM
        ( SELECT order_id, order_type , create_time , assign_type, update_time, status, city_id

         from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
         where 1=1
         and status in (3,4,8,9,2,14,15,17,18)

         UNION ALL

         SELECT order_id, order_type, create_time , assign_type, update_time, status, city_id

         from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
         where 1=1
         and status in (3,4,8,9,2,14,15,17,18)
         )ns
LEFT JOIN (select id, ref_order_category from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) group by 1,2) ogi on ogi.id > 0 and ogi.id = case when ns.order_type = 200 then ns.order_id else 0 end

LEFT JOIN
            (SELECT ogm.group_id
                   ,ogi.group_code
                   ,count (distinct ogm.ref_order_id) as total_order_in_group
             FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm
             LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
             WHERE 1=1
             and ogm.group_id is not null
             GROUP BY 1,2
             )order_rank on order_rank.group_id = case when ns.order_type = 200 then ns.order_id else 0 end
-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = ns.city_id and city.country_id = 86

left join
            (select ref_order_id, ref_order_category, group_id
             from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day)
             where grass_schema = 'foody_partner_db' group by 1,2,3
             ) dot on dot.ref_order_id = ns.order_id and (ns.order_type <> 200 and ns.order_type = dot.ref_order_category)

left join
            (select dot.ref_order_id, dot.ref_order_category
                   ,case when go.now_service_category_id = 1 then 'Food'
                         when go.now_service_category_id > 0 then 'Fresh/Market'
                         else 'Others' end as food_service
             from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
             left join (select id, now_service_category_id from shopeefood.foody_mart__fact_gross_order_join_detail where grass_region = 'VN' GROUP BY 1,2) go on go.id = dot.ref_order_id and dot.ref_order_category = 0
             where 1=1
             and dot.ref_order_category = 0
             and go.now_service_category_id >= 0
             group by 1,2,3
             ) s on s.ref_order_id = ns.order_id and (ns.order_type = 0 and ns.order_type = dot.ref_order_category)

left join
            (select ogm.group_id, ogm.ref_order_category--,go.now_service_category_id, ogm.ref_order_id
                   ,case when go.now_service_category_id = 1 then 'Food'
                         when go.now_service_category_id > 0 then 'Fresh/Market'
                         else 'Others' end as food_service
             from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm
             left join (select id, now_service_category_id from shopeefood.foody_mart__fact_gross_order_join_detail where grass_region = 'VN' GROUP BY 1,2) go on go.id = ogm.ref_order_id and ogm.ref_order_category = 0
             where 1=1
             and ogm.ref_order_category = 0
             and coalesce(ogm.group_id,0) > 0
             and go.now_service_category_id >= 0
             group by 1,2,3
             ) g on g.group_id = ns.order_id and ns.order_type = 200 and (case when ns.order_type <> 200 then ns.order_type else ogi.ref_order_category end  = 0)


WHERE 1=1
and date(from_unixtime(ns.create_time - 3600)) between DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month and current_date - interval '1' day
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)ns
WHERE 1=1
GROUP BY 1,2,3,3,4,5,6,7,8
)base
GROUP BY 1,2,3
)

SELECT
    p.period
    , p.days AS days
    , SUM(IF(order_source = '1. Food/Market', sum_total_assign, 0)) / p.days AS nowfood_assign
    , SUM(IF(order_source = '1. Food/Market', coalesce(sum_total_incharged, 0) + coalesce(sum_total_deny, 0), 0)) / p.days AS _nowfood_acceptance
    , SUM(IF(order_source = '1. Food/Market', sum_total_incharged, 0)) / p.days AS _nowfood_completed
    , SUM(IF(order_source = '1. Food/Market', sum_total_deny, 0)) / p.days AS _nowfood_denied
    , SUM(IF(order_source = '1. Food/Market', sum_total_ignored, 0)) / p.days AS _nowfood_ignored
    , SUM(IF(order_source != '1. Food/Market', sum_total_assign, 0)) / p.days AS nowship_assign
    , SUM(IF(order_source != '1. Food/Market', coalesce(sum_total_incharged, 0) + coalesce(sum_total_deny, 0), 0)) / p.days AS _nowship_acceptance
    , SUM(IF(order_source != '1. Food/Market', sum_total_incharged, 0)) / p.days AS _nowship_completed
    , SUM(IF(order_source != '1. Food/Market', sum_total_deny, 0)) / p.days AS _nowship_denied
    , SUM(IF(order_source != '1. Food/Market', sum_total_ignored, 0)) / p.days AS _nowship_ignored
FROM daily d
INNER JOIN params p ON d.date_ BETWEEN p.start_date AND p.end_date
GROUP BY 1,2