with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2021-12-01',current_date - interval '1' day) bar)

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

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,date_trunc('week',report_date) 
        ,max(report_date)

from raw_date

group by 1,2,3

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)

from raw_date

group by 1,2,3
)
,incharge_tab as
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        where status in (3,4) -- shipper incharge

        UNION

        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
        where status in (3,4) -- shipper incharge
)
,last_incharge as
(select 
    order_uid
    ,order_id
    ,max_by(city_id,create_time) as city_id
    ,max_by(assign_type,create_time) as assign_type
    ,max_by(update_time,create_time) as update_time
    ,max_by(status,create_time) as status
    ,max_by(create_time,create_time) as create_time
    ,max_by(order_type,create_time) as order_type
    ,max_by(experiment_group,create_time) as experiment_group
from incharge_tab
-- where order_id = 246260221
group by 1,2
)
,group_order as
(select 
    a.order_id
        ,a.order_type
        ,case when a.order_type <> 200 then order_type else ogi.ref_order_category end as order_category
        ,case when a.assign_type = 1 then '1. Single Assign'
                when a.assign_type in (2,4) then '2. Multi Assign'
                when a.assign_type = 3 then '3. Well-Stack Assign'
                when a.assign_type = 5 then '4. Free Pick'
                when a.assign_type = 6 then '5. Manual'
                when a.assign_type in (7,8) then '6. New Stack Assign'
                else null end as assign_type
        ,from_unixtime(a.create_time - 60*60) as create_time
        ,from_unixtime(a.update_time - 60*60) as update_time
        ,date(from_unixtime(a.create_time - 60*60)) as date_
        ,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week
from last_incharge a
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end
where a.order_type = 200
)
, order_delivery AS (
-- order_delivery
SELECT
    created_date AS grass_date
    , service
    , case 
        when is_stack_group_order = 1 then 'Group Order'
        when is_stack_group_order = 2 then 'Stack Order'
        else 'Single Order'
    end as assignment_type
    ,COUNT(DISTINCT order_uid) AS delivered_orders
FROM
    (
    SELECT dot.uid as shipper_id
            ,dot.ref_order_id as order_id
            ,dot.ref_order_code as order_code
            ,dot.ref_order_category
            ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
            ,case when dot.ref_order_category = 0 then 'Food/Market'
                when dot.ref_order_category in (3,4,5,6,7) then 'SPX Instant'
                else 'SPX Instant' end service
            ,dot.ref_order_status
            ,dot.order_status
            ,case when dot.order_status = 1 then 'Pending'
                when dot.order_status in (100,101,102) then 'Assigning'
                when dot.order_status in (200,201,202,203,204) then 'Processing'
                when dot.order_status in (300,301) then 'Error'
                when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
                else null end as order_status_group
            ,case 
                when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time = ogi.create_time then 1  -- 1: group
                when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 2 -- 2: stack
                when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time != ogi.create_time then 2 -- 2: stack
                else 0 end as is_stack_group_order
            ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
            ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
            ,from_unixtime(dot.submitted_time- 60*60) created_timestamp
            ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
            ,case when dot.estimated_drop_time = 0 then null else from_unixtime(dot.estimated_drop_time - 60*60) end as estimated_delivered_time
            ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
            ,case when dot.pick_city_id = 217 then 'HCM'
                when dot.pick_city_id = 218 then 'HN'
                when dot.pick_city_id = 219 then 'DN'
                ELSE 'OTH' end as city_group
            ,dot.pick_city_id as city_id
            ,case when COALESCE(oct.foody_service_id,0) = 1 then 'Food'
                when COALESCE(oct.foody_service_id,0) in (5) then 'Fresh'
                when COALESCE(oct.foody_service_id,0) > 0 then 'Market'
                when dot.ref_order_category = 0 then 'Food - Others'
                else 'Nowship' end as food_service

    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

    left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
    left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = dot.ref_order_id and dot.ref_order_category = 0
    -- stack group_code
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                        and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                        and ogm_filter.create_time >  ogm.create_time
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id AND city.country_id = 86
    LEFT JOIN group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category
    WHERE 1=1
    and ogm_filter.create_time is null
    and dot.pick_city_id <> 238
    )
WHERE order_status = 400
-- AND ref_order_category = 0
AND created_date BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month AND current_date - interval '1' day
GROUP BY 1,2,3
)


-- , spx_order as 
-- -- now_ship
-- (SELECT
--     created_date AS grass_date
--     ,'SPX Instant' AS service
--     ,case
--         when is_stack_group_order = 1 then 'Group Order'
--         when is_stack_group_order = 2 then 'Stack Order'
--         else 'Single Order'
--     end as assignment_type
--     ,COUNT(DISTINCT uid) AS delivered_orders
-- FROM
--     (
--     SELECT base.*
--         ,dot.group_id
--         ,dot.order_create_time
--         ,dot.group_create_time
--         ,case 
--             when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and dot.order_create_time = dot.group_create_time then 1 -- 1: group
--             when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 2
--             when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and dot.order_create_time != dot.group_create_time then 2 -- 2: stack
--             else 0 end as  is_stack_group_order
--         ,case when base.order_status = 'Delivered' then 1 else 0 end as is_del
--         ,case when base.order_status = 'Cancelled' then 1 else 0 end as is_cancel
--         ,case when base.order_status = 'Pickup Failed' then 1 else 0 end as is_pick_failed
--         ,case when base.order_status = 'Returned' then 1 else 0 end as is_return
--     FROM
--             (

--             --************** Now Ship/NSS
--             SELECT ns.id
--             ,ns.uid
--             ,ns.shipper_id
--             ,ns.code as order_code
--             ,ns.shopee_order_code
--             ,ns.customer_id
--             -- time
--             ,from_unixtime(ns.create_time - 60*60) as created_timestamp
--             ,cast(from_unixtime(ns.create_time - 60*60) as date) as created_date
--             ,case when status in (11) and ns.drop_real_time > 0 then date(from_unixtime(ns.drop_real_time - 60*60))
--                 when status in (14) and ns.update_time > 0 then date(from_unixtime(ns.update_time - 60*60))
--                 else date(from_unixtime(ns.create_time- 60*60)) end as report_date

--             ,case when ns.booking_type = 2 and ns.booking_service_type = 1 then 'now_ship_user'
--                 when ns.booking_type = 3 and ns.booking_service_type = 1 then 'now_ship_merchant'
--                 when ns.booking_type = 4 and ns.booking_service_type = 1 then 'now_ship_shopee'
--                 when ns.booking_type = 2 and ns.booking_service_type = 2 then 'now_ship_same_day'
--                 when ns.booking_type = 2 and ns.booking_service_type = 3 then 'now_ship_multi_drop'
--                 else null end as source
--             ,case when ns.booking_type = 2 and ns.booking_service_type = 1 then 4
--                 when ns.booking_type = 3 and ns.booking_service_type = 1 then 5
--                 when ns.booking_type = 4 and ns.booking_service_type = 1 then 6
--                 when ns.booking_type = 2 and ns.booking_service_type = 2 then 7
--                 when ns.booking_type = 2 and ns.booking_service_type = 3 then 8
--                 else null end as order_type
--             -- order info
--             ,case when ns.status = 11 then 'Delivered'
--                 when ns.status in (6,9,12) then 'Cancelled'
--                 when ns.booking_type = 4 and ns.status = 17 then 'Pickup Failed'
--                 when ns.booking_type <> 4 and ns.status = 23 then 'Pickup Failed'
--                 when ns.status = 14 then 'Returned'
--                 when ns.booking_type = 4 and ns.status = 3 then 'Assigning Timeout'
--                 else 'Others' end as order_status
--             ,case when ns.status = 6 then 'USER_CANCELLED'
--                 when ns.status = 9 then 'DRIVER_CANCELLED'
--                 when ns.status = 12 then 'SYSTEM_CANCELLED'
--                 else null end as cancel_by
--             -- location
--             ,case when ns.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
--             ,case when ns.city_id = 217 then 'HCM'
--                     when ns.city_id = 218 then 'HN'
--                     when ns.city_id = 219 then 'DN'
--                     ELSE 'OTH' end as city_group
--             ,ns.city_id
--             ,case when ns.pick_type = 1 then 1 else 0 end as is_asap
--             from
--                     (SELECT id,concat('now_ship_',cast(id as VARCHAR)) as uid, code, booking_type, case when booking_type = 3 then cast(referal_id as varchar) else cast(customer_id as varchar) end as customer_id, shipper_id, distance,create_time, status, payment_method,'now_ship' as original_source,city_id,cast(json_extract(extra_data,'$.pick_address_info.district_id') as DOUBLE) as district_id
--                             ,booking_service_type, pick_real_time, drop_real_time, pick_type, update_time, '' as shopee_order_code,assigning_count
--                             ,cast(json_extract(extra_data, '$.pick_address_info.address') as varchar) as sender_address
--                             ,cast(json_extract(extra_data, '$.sender_info.name')as varchar) as sender_name
--                             ,cast(json_extract(extra_data, '$.sender_info.phone')as varchar) as sender_phone

--                             ,cast(json_extract(extra_data, '$.drop_address_info.address') as varchar) as receiver_address
--                             ,cast(json_extract(extra_data, '$.receiver_info.name')as varchar) as receiver_name
--                             ,cast(json_extract(extra_data, '$.receiver_info.phone')as varchar) as receiver_phone


--                         from shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live

--                     UNION ALL

--                     SELECT id,concat('now_ship_shopee_',cast(id as VARCHAR)) as uid, code, 4 as booking_type, sender_username_v2 as customer_id, shipper_id,distance,create_time,status,1 as payment_method,'now_ship_shopee' as original_source,city_id,cast(json_extract(extra_data,'$.sender_info.district_id') as DOUBLE) as district_id
--                             ,booking_service_type, pick_real_time, drop_real_time, 1 as pick_type, update_time, shopee_order_code, coalesce(a.assign_cnt,0) as assigning_count
--                             ,cast(json_extract(extra_data, '$.sender_info.address') as varchar) as sender_address
--                             ,cast(json_extract(extra_data, '$.sender_info.name')as varchar) as sender_name
--                             ,cast(json_extract(extra_data, '$.sender_info.phone')as varchar) as sender_phone

--                             ,cast(json_extract(extra_data, '$.recipient_info.address') as varchar) as receiver_address
--                             ,cast(json_extract(extra_data, '$.recipient_info.name')as varchar) as receiver_name
--                             ,cast(json_extract(extra_data, '$.recipient_info.phone')as varchar) as receiver_phone

--                         from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live sbt

--                         left join
--                                 (SELECT booking_id, count(booking_id) assign_cnt

--                                 FROM shopeefood.foody_express_db__shopee_booking_change_log_tab__reg_daily_s0_live

--                                 WHERE 1=1
--                                 and message like '%to ASSIGNED%'
--                                 GROUP BY 1
--                                 )a on a.booking_id = sbt.id
--                     )ns

--             -- location
--             left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = ns.city_id and city.country_id = 86

--             WHERE 1=1
--             and ns.city_id <> 238
--             )base

--     LEFT JOIN
--             (SELECT dot.*, dotet.order_data, group_info.create_time as order_create_time, group_info.group_create_time

--             FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
--             left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

--             LEFT JOIN
--                     (
--                     SELECT ogm.*, ogi.create_time as group_create_time
--                     FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm
--                     LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
--                     WHERE 1=1
--                     ) group_info on group_info.order_id = dot.id and group_info.mapping_status = 11 and group_info.group_id = dot.group_id
--             ) dot on dot.ref_order_id = base.id and dot.ref_order_code = base.order_code
--     LEFT JOIN group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category
--     WHERE 1=1
--     and base.order_status <> 'Others'
--     )
-- WHERE is_del = 1
-- AND created_date BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month AND current_date - interval '1' day
-- GROUP BY 1,2,3
-- )
-- ,combine_order_spf as (
--     select
--         *
--     from order_delivery

--     union all

--     select 
--         *
--     from spx_order
-- )
SELECT
     p.period_group
    ,p.period
    -- , p.days AS days
    , SUM(IF(service = 'Food/Market', delivered_orders, 0)) /cast((date_diff('day',p.start_date,p.end_date) +1) as double) AS nowfood_ado
    , SUM(IF(service = 'Food/Market' AND assignment_type = 'Group Order', delivered_orders, 0)) /cast((date_diff('day',p.start_date,p.end_date) +1) as double) AS _nowfood_group_ado
    , SUM(IF(service = 'Food/Market' AND assignment_type = 'Stack Order', delivered_orders, 0)) /cast((date_diff('day',p.start_date,p.end_date) +1) as double) AS _nowfood_stack_ado
    , SUM(IF(service = 'Food/Market' AND assignment_type = 'Single Order', delivered_orders, 0)) /cast((date_diff('day',p.start_date,p.end_date) +1) as double) AS _nowfood_single_ado
    , SUM(IF(service = 'SPX Instant', delivered_orders, 0)) /cast((date_diff('day',p.start_date,p.end_date) +1) as double) AS nowship_ado
    , SUM(IF(service = 'SPX Instant' AND assignment_type = 'Group Order', delivered_orders, 0)) /cast((date_diff('day',p.start_date,p.end_date) +1) as double) AS _nowship_group_ado
    , SUM(IF(service = 'SPX Instant' AND assignment_type = 'Stack Order', delivered_orders, 0)) /cast((date_diff('day',p.start_date,p.end_date) +1) as double) AS _nowship_stack_ado
    , SUM(IF(service = 'SPX Instant' AND assignment_type = 'Single Order', delivered_orders, 0)) /cast((date_diff('day',p.start_date,p.end_date) +1) as double) AS _nowship_single_ado
FROM order_delivery d
INNER JOIN params_date p ON d.grass_date BETWEEN p.start_date AND p.end_date
GROUP BY 1,2,date_diff('day',p.start_date,p.end_date)
