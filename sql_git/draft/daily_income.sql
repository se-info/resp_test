WITH violation AS
(SELECT
    base.shipper_id
    , COUNT(DISTINCT base.ticket_id) AS violations
FROM
    (SELECT
        ht.id AS ticket_id
        , CASE
            WHEN ht.status = 1 THEN '1. Open'
            WHEN ht.status = 2 THEN '2. Pending'
            WHEN ht.status = 3 THEN '3. Resolved'
            WHEN ht.status = 5 THEN '4. Completed'
            WHEN ht.status = 4 THEN '5. Closed'
        ELSE NULL END AS status
        , CASE
            WHEN ht.incharge_team = 1 THEN 'CC'
            WHEN ht.incharge_team = 2 THEN 'PROJECTOR'
            WHEN ht.incharge_team = 3 THEN 'EDITOR'
            WHEN ht.incharge_team = 4 THEN 'GOFAST'
            WHEN ht.incharge_team = 5 THEN 'PRODUCT SUPPORT'
            WHEN ht.incharge_team = 6 THEN 'AGENT'
            WHEN ht.incharge_team = 7 THEN 'AGENT MANAGER'
        ELSE NULL END AS incharge_team
        , CASE
            WHEN ht.ticket_type = 1 THEN 'VIOLATION_OF_RULES'
            WHEN ht.ticket_type = 2 THEN 'CHANGE_SHIPPER_INFO'
            WHEN ht.ticket_type = 3 THEN 'FRAUD'
            WHEN ht.ticket_type = 4 THEN 'CUSTOMER_FEEDBACK'
            WHEN ht.ticket_type = 5 THEN 'CC_FEEDBACK'
            WHEN ht.ticket_type = 6 THEN 'NOW_POLICE'
            WHEN ht.ticket_type = 7 THEN 'MERCHANT_FEEDBACK'
            WHEN ht.ticket_type = 8 THEN 'PARTNER_SIGNATURE_NOTE'
            WHEN ht.ticket_type = 9 THEN 'REQUEST_CHANGE_DRIVER_INFO'
        ELSE NULL END AS ticket_type
        , CASE
            WHEN ht.city_id = 217 THEN 'HCM'
            WHEN ht.city_id = 218 THEN 'HN'
            WHEN ht.city_id = 219 THEN 'DN'
            WHEN ht.city_id = 220 THEN 'HP'
        ELSE 'OTH' END AS city_group
        , FROM_UNIXTIME(ht.create_time - 3600) AS created_timestamp
        , DATE(FROM_UNIXTIME(ht.create_time - 3600)) AS created_date
        , COALESCE(htl.label,'NO_ACTION') AS resolution
        , IF(ht.resolve_time > 0, FROM_UNIXTIME(ht.resolve_time - 3600), FROM_UNIXTIME(ht.update_time - 3600)) AS resolve_timestamp
        , DATE_DIFF('second', FROM_UNIXTIME(ht.create_time - 3600), IF(ht.resolve_time > 0, FROM_UNIXTIME(ht.resolve_time - 3600), FROM_UNIXTIME(ht.update_time - 3600))) lt_resolve
        , htu.uid AS shipper_id
        , ht.extra_data AS ex

    FROM shopeefood.foody_internal_db__hr_tick_tab__reg_daily_s0_live ht
    LEFT JOIN shopeefood.foody_internal_db__hr_tick_label_tab__reg_daily_s0_live htl on htl.tick_id = ht.id
    LEFT JOIN shopeefood.foody_internal_db__hr_tick_user_tab__reg_daily_s0_live htu on htu.tick_id = ht.id

    WHERE 1=1
    AND ht.incharge_team = 4
    AND DATE(FROM_UNIXTIME(ht.create_time - 3600)) between DATE'2022-01-16' and DATE'2022-01-29'
    AND ht.status = 5
    AND COALESCE(htl.label,'NO_ACTION') NOT IN ('NO_ACTION','REWARD_SHIPPER')
    AND htu.uid IS NOT NULL
    ) base
GROUP BY 1
)
, shipper_orders AS
(SELECT
    snp.shipper_id
    , snp.shipper_name
    , snp.city_name
    , COUNT(DISTINCT snp.report_date) as working_day
    , SUM(case when snp.is_hub_qualified = 1 then snp.cnt_total_order_delivered else null end) as delivered_qualified_orders
    , SUM(snp.cnt_total_order_delivered) AS delivered_orders
    , SUM(COALESCE(snp.delivered_ns_shopee_distance, 0))
    + SUM(COALESCE(snp.delivered_ns_offshopee_distance, 0))
    + SUM(COALESCE(snp.delivered_market_distance, 0))
    + SUM(COALESCE(snp.delivered_fresh_distance, 0))
    + SUM(COALESCE(snp.delivered_food_distance, 0))
    AS delivered_distance
    , SUM(IF(CAST(COALESCE(sla.completed_rate, 0) AS DOUBLE) / 100 >= 90, 1, 0)) AS eligible_sla_days
    , COUNT(DISTINCT snp.report_date) AS shipper_days
    , SUM(IF(CAST(COALESCE(sla.completed_rate, 0) AS DOUBLE) / 100 >= 90, CAST(COALESCE(sla.completed_rate, 0) AS DOUBLE) / 100, 0)) / COUNT(DISTINCT snp.report_date) AS avg_sla
FROM
     (
SELECT
    base3.shipper_id
    , base3.report_date
    , CASE
        WHEN driver_type.shipper_type_id = 1 THEN 'full-time'
        WHEN driver_type.shipper_type_id = 3 THEN 'tester'
        WHEN driver_type.shipper_type_id = 11 THEN 'part-time'
        WHEN driver_type.shipper_type_id = 12 THEN 'hub'
    ELSE 'part-time' END AS current_shipper_type
    , driver.shipper_name
    , driver.city_name
    , driver.city_group
    , base3.is_hub_qualified
    , IF( driver.city_group IN ('HCM', 'HN')
        , COALESCE(bonus.current_driver_tier,'full-time')
        , 'part-time') AS current_driver_tier
    , base3.oct_cnt_total_order AS cnt_total_order
    , base3.oct_cnt_delivered_order AS cnt_total_order_delivered
    , base3.oct_cnt_completed_order AS cnt_completed_order
    , base3.oct_cnt_cancelled_order AS cnt_cancelled_order

    , base3.oct_cnt_delivered_order_hcm AS cnt_delivered_order_hcm
    , base3.oct_cnt_completed_order_hcm AS cnt_completed_order_hcm
    , base3.oct_cnt_completed_order_hn AS cnt_completed_order_hn
    , base3.oct_cnt_delivered_order_hn AS cnt_delivered_order_hn

    , base3.oct_cnt_quit_order AS cnt_quit_order
    , base3.oct_cnt_breach_order AS cnt_breach_order
    , base3.cnt_total_order_delivered_food_arrive_merchant_ontime
    , base3.cnt_total_order_delivered_food_arrive_buyer_ontime
    , base3.cnt_total_order_delivered_food_market
    , base3.cnt_total_order_delivered_food
    , base3.cnt_total_order_delivered_market
    , base3.cnt_total_order_delivered_fresh
    , base3.cnt_total_order_delivered_ns_offshopee
    , base3.cnt_total_order_delivered_ns_shopee
    , base3.cnt_total_order_del_for_lead_time_completion
    , base3.sum_total_leadtime_completion
    , base3.cnt_total_order_delivered_late_sla
    , base3.cnt_total_order_delivered_valid_submit_to_del_food

    , base3.delivered_food_distance
    , base3.delivered_market_distance
    , base3.delivered_fresh_distance
    , base3.delivered_ns_shopee_distance
    , base3.delivered_ns_offshopee_distance

FROM
    (
    SELECT
        base2.shipper_id
        , base2.report_date
        , base2.is_hub_qualified
        , COUNT(DISTINCT base2.uid) AS oct_cnt_total_order
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered', base2.uid, NULL)) AS oct_cnt_delivered_order
        , COUNT(DISTINCT IF(base2.order_status = 'Cancelled', base2.uid, NULL)) AS oct_cnt_cancelled_order
        , COUNT(DISTINCT IF(base2.order_status = 'Quit', base2.uid, NULL)) AS oct_cnt_quit_order

        , COUNT(DISTINCT IF(base2.order_status IN ('Delivered','Quit','Returned'), base2.uid, NULL)) AS oct_cnt_completed_order
        , COUNT(DISTINCT IF(base2.order_status IN ('Delivered','Quit','Returned') AND city_group = 'HCM', base2.uid, NULL)) AS oct_cnt_completed_order_hcm
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND city_group = 'HCM', base2.uid, NULL)) AS oct_cnt_delivered_order_hcm

        , COUNT(DISTINCT IF(base2.order_status IN ('Delivered','Quit','Returned') AND city_group = 'HN', base2.uid, NULL)) AS oct_cnt_completed_order_hn
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND city_group = 'HN', base2.uid, NULL)) AS oct_cnt_delivered_order_hn

        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.is_late_delivered_time = 1, base2.uid, NULL)) AS oct_cnt_breach_order

        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.is_arrive_merchant_on_time = 1, base2.uid, NULL)) AS cnt_total_order_delivered_food_arrive_merchant_ontime
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.is_arrive_buyer_on_time = 1, base2.uid, NULL)) AS cnt_total_order_delivered_food_arrive_buyer_ontime
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood', base2.uid, NULL)) AS cnt_total_order_delivered_food_market
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.sub_source = 'now_food', base2.uid, NULL)) AS cnt_total_order_delivered_food
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.sub_source = 'now_market', base2.uid, NULL)) AS cnt_total_order_delivered_market
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.sub_source = 'now_fresh', base2.uid, NULL)) AS cnt_total_order_delivered_fresh
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'now_ship_shopee', base2.uid, NULL)) AS cnt_total_order_delivered_ns_shopee
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source IN ('now_ship', 'now_ship_sameday','now_ship_multi_drop'), base2.uid, NULL)) AS cnt_total_order_delivered_ns_offshopee

        , SUM(IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.sub_source = 'now_food', base2.distance, 0)) AS delivered_food_distance
        , SUM(IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.sub_source = 'now_market', base2.distance, 0)) AS delivered_market_distance
        , SUM(IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.sub_source = 'now_fresh', base2.distance, 0)) AS delivered_fresh_distance
        , SUM(IF(base2.order_status = 'Delivered' AND base2.source = 'now_ship_shopee', base2.distance, 0)) AS delivered_ns_shopee_distance
        , SUM(IF(base2.order_status = 'Delivered' AND base2.source IN ('now_ship', 'now_ship_sameday','now_ship_multi_drop'), base2.distance, 0)) AS delivered_ns_offshopee_distance


        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.is_asap = 1 AND base2.is_valid_submit_to_del = 1, base2.uid, NULL)) AS cnt_total_order_del_for_lead_time_completion
        , SUM(IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.is_asap = 1 AND base2.is_valid_submit_to_del = 1, lt_completion_original, 0)) AS sum_total_leadtime_completion

        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.is_late_sla = 1 AND base2.is_valid_submit_to_del = 1, base2.uid, NULL)) AS cnt_total_order_delivered_late_sla
        , COUNT(DISTINCT IF(base2.order_status = 'Delivered' AND base2.source = 'NowFood' AND base2.is_valid_submit_to_del = 1, base2.uid, NULL)) AS cnt_total_order_delivered_valid_submit_to_del_food
    FROM
        (
        SELECT
                 base1.uid
                , base1.order_code
                , base1.is_hub_qualified
                , IF(base1.source = 'order_delivery', 'NowFood', source) AS source
                , sub_source
                , base1.shipper_id
                , base1.report_date
                , base1.city_group
                , base1.city_name
                , base1.district_name
                , base1.order_status
                , base1.is_asap
                , IF(CAST(base1.lt_completion AS DOUBLE)/60 > base1.lt_sla, 1, 0) AS is_late_sla
                , base1.lt_completion AS lt_completion_original
                , base1.lt_incharge
                , base1.is_late_delivered_time
                , base1.is_late_arrive_buyer
                , base1.lt_assign_to_arrive_at_merchant
                , base1.lt_incharge_to_arrive_at_merchant
                , base1.lt_pick_to_arrive_at_buyer
                , base1.lt_arrive_at_buyer_to_del
                , IF(base1.lt_incharge_to_arrive_at_merchant <= base1.eta_pickup, 1, 0) AS is_arrive_merchant_on_time
                , IF(base1.lt_pick_to_arrive_at_buyer <= base1.eta_dropoff, 1, 0) AS is_arrive_buyer_on_time
                , base1.distance
                , CASE
                    WHEN base1.distance <= 3 THEN '1. 0-3km'
                    WHEN base1.distance <= 4 THEN '2. 3-4km'
                    WHEN base1.distance <= 5 THEN '3. 4-5km'
                    WHEN base1.distance > 5 THEN '5. 5km+'
                ELSE NULL END AS distance_range
                , base1.is_hub_driver
                , base1.is_valid_incharge
                , base1.is_valid_submit_to_del
                , base1.is_valid_lt_arrive_at_merchant
                , base1.is_valid_lt_incharge_to_arrive_merchant
                , base1.is_valid_lt_arrive_at_buyer
                , base1.is_valid_lt_arrive_at_buyer_to_del
            FROM
                (
                SELECT
                    base.shipper_id
                    , base.city_name
                    , base.city_group
                    , base.district_name
                    , base.report_date
                    , base.created_date
                    , base.created_year_week
                    , base.created_year_month
                    , DATE(base.inflow_timestamp) inflow_date

                    , base.order_id
                    , base.order_code
                    , CONCAT(base.source, '_', CAST(base.order_id AS VARCHAR)) AS uid
                    , base.ref_order_category AS order_type
                    , base.source
                    , base.ref_order_category
                    , CASE
                        WHEN base.order_status = 400 THEN 'Delivered'
                        WHEN base.order_status = 401 THEN 'Quit'
                        WHEN base.order_status IN (402,403,404) THEN 'Cancelled'
                        WHEN base.order_status IN (405,407) THEN 'Returned'
                    ELSE 'Others' END AS order_status

                    , base.order_status_group

                    , base.first_auto_assign_timestamp
                    , base.last_delivered_timestamp
                    , base.estimated_delivered_time
                    , base.last_picked_timestamp
                    , base.max_arrived_at_merchant_timestamp
                    , base.max_arrived_at_buyer_timestamp
                    , base.inflow_timestamp
                    , DATE_FORMAT(base.inflow_timestamp, '%a') AS inflow_day_of_week

                    , HOUR(base.created_timestamp) AS created_hour
                    , HOUR(base.inflow_timestamp) AS inflow_hour
                    , CASE
                        WHEN MINUTE(base.inflow_timestamp) <= 5 THEN '01. Min 0 - 5'
                        WHEN MINUTE(base.inflow_timestamp) <= 10 THEN '02. Min 5 - 10'
                        WHEN MINUTE(base.inflow_timestamp) <= 15 THEN '03. Min 10 - 15'
                        WHEN MINUTE(base.inflow_timestamp) <= 20 THEN '04. Min 15 - 20'
                        WHEN MINUTE(base.inflow_timestamp) <= 25 THEN '05. Min 20 - 25'
                        WHEN MINUTE(base.inflow_timestamp) <= 30 THEN '06. Min 25 - 30'
                        WHEN MINUTE(base.inflow_timestamp) <= 35 THEN '07. Min 30 - 35'
                        WHEN MINUTE(base.inflow_timestamp) <= 40 THEN '08. Min 35 - 40'
                        WHEN MINUTE(base.inflow_timestamp) <= 45 THEN '09. Min 40 - 45'
                        WHEN MINUTE(base.inflow_timestamp) <= 50 THEN '10. Min 45 - 50'
                        WHEN MINUTE(base.inflow_timestamp) <= 55 THEN '11. Min 50 - 55'
                        WHEN MINUTE(base.inflow_timestamp) <= 60 THEN '12. Min 55 - 60'
                    ELSE NULL END AS inflow_minute_range

                    , base.is_asap
                    , base.delivery_distance distance
                    , CASE
                        WHEN base.delivery_distance <= 1 THEN 30
                        WHEN base.delivery_distance > 1 THEN LEAST(60,30 + 5*(ceiling(base.delivery_distance) -1))
                    ELSE NULL END AS lt_sla

                    , IF(base.first_auto_assign_timestamp < base.last_incharge_timestamp, 1, 0) AS is_valid_incharge
                    , IF(base.created_timestamp <= base.last_delivered_timestamp, 1, 0) AS is_valid_submit_to_del

                    , CAST(DATE_DIFF('second',base.first_auto_assign_timestamp,base.last_incharge_timestamp) AS DOUBLE) / 60 AS lt_incharge
                    , DATE_DIFF('second',base.created_timestamp,base.last_delivered_timestamp) AS lt_completion

                    , IF(base.last_delivered_timestamp > base.estimated_delivered_time, 1, 0) AS is_late_delivered_time
                    , IF(base.max_arrived_at_buyer_timestamp > base.estimated_delivered_time, 1, 0) AS is_late_arrive_buyer
                    , DATE_DIFF('second',base.estimated_delivered_time,base.last_delivered_timestamp) AS lt_from_promise_to_actual_delivered

                    , CAST(DATE_DIFF('second',base.first_auto_assign_timestamp,base.max_arrived_at_merchant_timestamp) AS DOUBLE) AS lt_assign_to_arrive_at_merchant
                    , CAST(DATE_DIFF('second',base.last_incharge_timestamp,base.max_arrived_at_merchant_timestamp) AS DOUBLE) AS lt_incharge_to_arrive_at_merchant
                    , CAST(DATE_DIFF('second',base.last_picked_timestamp ,base.max_arrived_at_buyer_timestamp) AS DOUBLE) AS lt_pick_to_arrive_at_buyer
                    , CAST(DATE_DIFF('second',base.max_arrived_at_buyer_timestamp ,base.last_delivered_timestamp) AS DOUBLE) AS lt_arrive_at_buyer_to_del

                    , IF(base.first_auto_assign_timestamp <= base.max_arrived_at_merchant_timestamp, 1, 0) AS is_valid_lt_arrive_at_merchant
                    , IF(base.last_incharge_timestamp <= base.max_arrived_at_merchant_timestamp, 1, 0) AS is_valid_lt_incharge_to_arrive_merchant
                    , IF(base.last_picked_timestamp <= base.max_arrived_at_buyer_timestamp, 1, 0) AS is_valid_lt_arrive_at_buyer
                    , IF(base.max_arrived_at_buyer_timestamp <= base.last_delivered_timestamp, 1, 0) AS is_valid_lt_arrive_at_buyer_to_del


                    , base.eta_pickup
                    ,base.eta_dropoff

                    , case when base.hub_id > 0 then 1 else 0 end as is_hub_qualified
                    , base.is_hub_driver
                    , IF(source = 'order_delivery', sub_source, source) AS sub_source
                FROM
                    (
                    SELECT
                        dot.uid AS shipper_id
                        , dot.ref_order_id AS order_id
                        , dot.ref_order_code AS order_code
                        , dot.ref_order_category
                        , CASE
                            WHEN dot.ref_order_category = 0 THEN 'order_delivery'
                            WHEN dot.ref_order_category = 3 THEN 'now_moto'
                            WHEN dot.ref_order_category = 4 THEN 'now_ship'
                            WHEN dot.ref_order_category = 5 THEN 'now_ship'
                            WHEN dot.ref_order_category = 6 THEN 'now_ship_shopee'
                            WHEN dot.ref_order_category = 7 THEN 'now_ship_sameday'
                            WHEN dot.ref_order_category = 8 THEN 'now_ship_multi_drop'
                        ELSE NULL END AS source
                        , dot.ref_order_status
                        , dot.order_status
                        , CASE
                            WHEN dot.order_status = 1 THEN 'Pending'
                            WHEN dot.order_status in (100,101,102) THEN 'Assigning'
                            WHEN dot.order_status in (200,201,202,203,204) THEN 'Processing'
                            WHEN dot.order_status in (300,301) THEN 'Error'
                            WHEN dot.order_status in (400,401,402,403,404,405,406,407) THEN 'Completed'
                        ELSE NULL END AS order_status_group

                        , dot.is_asap
                        , CAST(dot.delivery_distance AS DOUBLE)/1000 delivery_distance

                        , CASE
                            WHEN dot.order_status in (400,401,405,407) AND dot.real_drop_time > 0 THEN date(FROM_UNIXTIME(dot.real_drop_time - 3600))
                            WHEN dot.order_status in (402,403,404) AND CAST(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 THEN date(FROM_UNIXTIME(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
                        ELSE date(FROM_UNIXTIME(dot.submitted_time- 3600)) END AS report_date
                        , DATE(FROM_UNIXTIME(dot.submitted_time- 3600)) AS created_date
                        , FROM_UNIXTIME(dot.submitted_time- 3600) AS created_timestamp
                        , CASE
                              WHEN WEEK(DATE(from_unixtime(dot.submitted_time - 3600))) >= 52 AND MONTH(DATE(from_unixtime(dot.submitted_time - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(dot.submitted_time - 3600)))-1)*100 + WEEK(DATE(from_unixtime(dot.submitted_time - 3600)))
                              WHEN WEEK(DATE(from_unixtime(dot.submitted_time - 3600))) = 1 AND MONTH(DATE(from_unixtime(dot.submitted_time - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(dot.submitted_time - 3600)))+1)*100 + WEEK(DATE(from_unixtime(dot.submitted_time - 3600)))
                          ELSE YEAR(DATE(from_unixtime(dot.submitted_time - 3600)))*100 + WEEK(DATE(from_unixtime(dot.submitted_time - 3600))) END AS created_year_week
                        , CONCAT(CAST(YEAR(FROM_UNIXTIME(dot.submitted_time - 3600)) AS VARCHAR), '-', DATE_FORMAT(FROM_UNIXTIME(dot.submitted_time - 3600),'%b')) as created_year_month

                        , IF(dot.real_drop_time = 0, NULL, FROM_UNIXTIME(dot.real_drop_time - 3600)) AS last_delivered_timestamp
                        , IF(dot.estimated_drop_time = 0, NULL, FROM_UNIXTIME(dot.estimated_drop_time - 3600)) AS estimated_delivered_time
                        , fa.first_auto_assign_timestamp
                        , fa.last_incharge_timestamp

                        , IF(dot.is_asap = 0, fa.first_auto_assign_timestamp, FROM_UNIXTIME(dot.submitted_time- 3600)) AS inflow_timestamp

                        , district.name_en AS district_name
                        , IF(dot.pick_city_id = 238, 'Dien Bien', city.name_en) AS city_name
                        , CASE
                            WHEN dot.pick_city_id = 217 THEN 'HCM'
                            WHEN dot.pick_city_id = 218 THEN 'HN'
                            WHEN dot.pick_city_id = 219 THEN 'DN'
                            WHEN dot.pick_city_id = 220 THEN 'HP'
                        ELSE 'OTH' END AS city_group
                        , COALESCE(CAST(json_extract(dotet.order_data,'$.hub_id') AS BIGINT ),0) AS hub_id
                        , IF(driver_hub.shipper_type_id = 12, 1, 0) AS is_hub_driver

                        , fa.last_picked_timestamp
                        , IF(arrive.max_arrived_at_merchant_timestamp IS NOT NULL, arrive.max_arrived_at_merchant_timestamp, fa.last_picked_timestamp) AS max_arrived_at_merchant_timestamp
                        , CASE
                            WHEN arrive.max_arrived_at_buyer_timestamp IS NOT NULL THEN arrive.max_arrived_at_buyer_timestamp
                            WHEN dot.real_drop_time = 0 THEN NULL
                        ELSE FROM_UNIXTIME(dot.real_drop_time - 3600) END AS max_arrived_at_buyer_timestamp

                        , COALESCE(eta.t_pickup,0) AS eta_pickup
                        , COALESCE(eta.t_dropoff,0) AS eta_dropoff
                        , CASE
                            WHEN COALESCE(oct.foody_service_id,0) = 1 THEN 'now_food'
                            WHEN COALESCE(oct.foody_service_id,0) = 5 THEN 'now_fresh'
                            WHEN COALESCE(oct.foody_service_id,0) > 0 THEN 'now_market'
                            WHEN dot.ref_order_category = 0 then 'now_food'
                        ELSE 'Nowship' END AS sub_source

                    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

                    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet ON dot.id = dotet.order_id
                    LEFT JOIN shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct ON oct.id = dot.ref_order_id AND dot.ref_order_category = 0

                    LEFT JOIN
                        (SELECT
                            order_id
                            , MAX(IF(destination_key = 256, FROM_UNIXTIME(create_time - 3600), NULL)) AS max_arrived_at_merchant_timestamp
                            , MAX(IF(destination_key = 512, FROM_UNIXTIME(create_time - 3600), NULL)) AS max_arrived_at_buyer_timestamp

                        FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_arrive_log_tab_vn_da where date(dt) = current_date - interval '1' day) doal
                        WHERE 1=1
                        AND grass_schema = 'foody_partner_db'
                        GROUP BY 1
                        ) arrive ON dot.id = arrive.order_id

                    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city ON city.id = dot.pick_city_id AND city.country_id = 86

                    Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = dot.pick_district_id

                    LEFT JOIN
                        (SELECT
                            order_id
                            , 0 AS order_type
                            , MIN(IF(status = 21, FROM_UNIXTIME(create_time) - interval '1' hour, NULL)) AS first_auto_assign_timestamp
                            , MAX(IF(status = 11, FROM_UNIXTIME(create_time) - interval '1' hour, NULL)) AS last_incharge_timestamp
                            , MAX(IF(status = 6, FROM_UNIXTIME(create_time) - interval '1' hour, NULL)) AS last_picked_timestamp
                            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                            where 1=1
                            and grass_schema = 'foody_order_db'
                            group by 1,2

                        UNION ALL

                        SELECT
                            ns.order_id
                            , ns.order_type
                            , MIN(FROM_UNIXTIME(create_time - 3600)) AS first_auto_assign_timestamp
                            , MAX(IF(status IN (3,4), FROM_UNIXTIME(update_time) - interval '1' hour, NULL)) AS last_incharge_timestamp
                            , MAX(IF(status IN (3,4), FROM_UNIXTIME(update_time) - interval '1' hour, NULL)) AS last_picked_timestamp
                        FROM
                            ( SELECT order_id, order_type , create_time , update_time, status

                             from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                             and grass_schema = 'foody_partner_archive_db'

                             UNION ALL

                             SELECT order_id, order_type, create_time , update_time, status

                             from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                             and schema = 'foody_partner_db'
                             ) ns
                        GROUP BY 1,2
                        ) fa ON dot.ref_order_id = fa.order_id AND dot.ref_order_category = fa.order_type

                    LEFT JOIN
                        (SELECT
                            sm.shipper_id
                            , sm.shipper_type_id
                            , TRY_CAST(sm.grass_date AS DATE) AS report_date

                            FROM shopeefood.foody_mart__profile_shipper_master sm

                            WHERE 1=1
                            AND shipper_type_id <> 3
                            AND shipper_status_code = 1
                            AND grass_region = 'VN'
                            GROUP BY 1,2,3
                        ) driver_hub ON driver_hub.shipper_id = dot.uid AND driver_hub.report_date = CASE
                                                                                                        WHEN dot.order_status IN (400,401,405,407) AND dot.real_drop_time > 0 THEN date(FROM_UNIXTIME(dot.real_drop_time - 3600))
                                                                                                        WHEN dot.order_status IN (402,403,404) AND cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 THEN date(FROM_UNIXTIME(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
                                                                                                     ELSE date(FROM_UNIXTIME(dot.submitted_time- 3600)) END

                    LEFT JOIN
                        (SELECT
                            eta.id
                            , eta.order_id
                            , FROM_UNIXTIME(eta.create_time - 3600) AS create_time
                            , FROM_UNIXTIME(eta.update_time - 3600) AS update_time
                            , COALESCE(CAST(json_extract(eta.eta_data,'$.t_pickup.value') AS INT), 0) AS t_pickup
                            , COALESCE(CAST(json_extract(eta.eta_data,'$.t_dropoff.value') AS INT),0) AS t_dropoff
                            , eta_data

                            FROM shopeefood.data_mining_db__order_eta_data_tab__reg_daily_s0_live eta
                            WHERE grass_schema = 'data_mining_db'
                        ) eta ON eta.order_id =  dot.ref_order_id AND dot.ref_order_category = 0

                    WHERE 1=1
                    AND dot.pick_city_id <> 238
                    AND dot.grass_schema = 'foody_partner_db'
                    ) base

                WHERE 1=1
                AND base.report_date BETWEEN date'2022-01-16' and date'2022-01-29'
                AND base.order_status_group = 'Completed'

                ) base1
        ) base2
    GROUP BY 1,2,3
    ) base3

LEFT JOIN
    (SELECT
        sm.shipper_id
        , sm.city_name
        , CASE
            WHEN sm.city_name = 'HCM City' THEN 'HCM'
            WHEN sm.city_name = 'Ha Noi City' THEN 'HN'
            WHEN sm.city_name = 'Da Nang City' THEN 'DN'
            WHEN sm.city_name = 'Hai Phong City' THEN 'HP'
        ELSE 'OTH' END AS city_group
        , sm.shipper_name

    FROM shopeefood.foody_mart__profile_shipper_master sm

    WHERE 1=1
    AND grass_region = 'VN'
    AND TRY_CAST(sm.grass_date AS DATE) = current_date - interval '1' day
    ) driver ON driver.shipper_id = base3.shipper_id

LEFT JOIN
    (SELECT
        sm.shipper_id
        , sm.shipper_type_id
        , TRY_CAST(sm.grass_date AS DATE) AS report_date

    FROM shopeefood.foody_mart__profile_shipper_master sm

    WHERE 1=1
    AND sm.grass_region = 'VN'
    AND sm.grass_date != 'current'
    ) driver_type on driver_type.shipper_id = base3.shipper_id and driver_type.report_date = base3.report_date

LEFT JOIN
    (SELECT
        DATE(FROM_UNIXTIME(bonus.report_date - 3600)) AS report_date
        , bonus.uid AS shipper_id
        , driver_type.shipper_type_id
        , CASE
            WHEN driver_type.shipper_type_id = 12 THEN 'Hub'
            WHEN bonus.tier IN (1,6,11) THEN 'T1' -- as current_driver_tier
            WHEN bonus.tier IN (2,7,12) THEN 'T2'
            WHEN bonus.tier IN (3,8,13) THEN 'T3'
            WHEN bonus.tier IN (4,9,14) THEN 'T4'
            WHEN bonus.tier IN (5,10,15) THEN 'T5'
        ELSE NULL END AS current_driver_tier
    FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
    LEFT JOIN
        (SELECT
            base.shipper_id
            ,base.report_date
            ,base.shipper_type_id
        FROM
            (SELECT
                shipper_id
                , city_name
                , TRY_CAST(grass_date AS DATE) AS report_date
                , shipper_type_id

            FROM shopeefood.foody_mart__profile_shipper_master

            WHERE 1=1
            AND shipper_type_id <> 3
            AND shipper_status_code = 1
            AND shipper_type_id = 12 -- hub driver
            AND grass_region = 'VN'
            ) base
            GROUP BY 1,2,3
        ) driver_type ON driver_type.shipper_id = bonus.uid AND driver_type.report_date = DATE(FROM_UNIXTIME(bonus.report_date - 3600))
        WHERE schema = 'foody_internal_db'
    ) bonus ON base3.report_date = bonus.report_date AND base3.shipper_id = bonus.shipper_id
WHERE 1=1
) snp
LEFT JOIN violation v ON snp.shipper_id = v.shipper_id
LEFT JOIN shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live sla ON snp.shipper_id = sla.uid AND DATE(FROM_UNIXTIME(sla.report_date-3600)) = snp.report_date
WHERE 1 = 1
--and shipper_id = 20014628 
AND COALESCE(snp.cnt_total_order_delivered, 0) > 0
GROUP BY 1,2,3
)
, ranking AS
(SELECT *
FROM
    (SELECT
        *
        , RANK() OVER (ORDER BY delivered_orders DESC, delivered_distance DESC) AS ranking
    FROM (SELECT * FROM shipper_orders )
    )
--WHERE ranking <= 2022
)
SELECT
    r.ranking
    , r.shipper_id
    , r.shipper_name
    , r.city_name
    , r.delivered_qualified_orders
    , r.delivered_distance
    , sm.shipper_type_id
    , r.avg_sla
    , r.working_day 
    , snp.online_time
    , snp.working_time
    , coalesce(inshift_income,0) as inshift_income
    , SUM(total_income) * 0.05 AS tet_bonus
    , SUM(total_income) AS total_earning_before_tax


FROM 
(SELECT
    shipper_id
    , (COALESCE(food_shipping_fee, 0) + COALESCE(food_bonus, 0)) AS food_income
    , (COALESCE(ship_shipping_fee, 0) + COALESCE(ship_bonus, 0)) AS ship_income
    , (COALESCE(bonus, 0) + COALESCE(game, 0) + COALESCE(arrears, 0)) AS bonus_and_others
    , (COALESCE(ship_shipping_fee, 0) + COALESCE(food_shipping_fee, 0) + COALESCE(food_bonus, 0) + COALESCE(ship_bonus, 0) + COALESCE(bonus, 0) + COALESCE(game, 0) + COALESCE(arrears, 0)) AS total_income
FROM
    (SELECT
        user_id as shipper_id
        , CAST(SUM(IF(txn_type in (104, 906), balance + deposit, 0)) AS DOUBLE) / 100 AS food_shipping_fee
        , CAST(SUM(IF(txn_type in (117,119,112,115,129,131,133,135,110,518,519,105,101,106), balance + deposit, 0)) AS DOUBLE) / 100 AS food_bonus
        , CAST(SUM(IF(txn_type in (202,201,301,401,1000,2001,2101,302,402,1001,2002,2102), balance + deposit, 0)) AS DOUBLE) / 100 AS ship_shipping_fee
        , CAST(SUM(IF(txn_type in (204,304,404,1003,2004,2104,200,300,400,1006,2000,2100,203,303,403,2003,2005,2006,2007,2105,2106), balance + deposit, 0)) AS DOUBLE) / 100 AS ship_bonus
        , CAST(SUM(IF(txn_type in (900,512,505,907), balance + deposit, 0)) AS DOUBLE) / 100 AS bonus
        , CAST(SUM(IF(txn_type in (519)
                    AND (COALESCE(UPPER(note), 'NA') LIKE '%MINIGAME%'
                        OR COALESCE(UPPER(TRIM(note)), 'NA') = 'CHƯƠNG TRÌNH PHÁT MẪU COKE ZERO')
                    , balance + deposit, 0)) AS DOUBLE) / 100 AS game
        , CAST(SUM(IF(txn_type in (565)
                    AND (COALESCE(UPPER(note), 'NA') LIKE '%(DUMMY)%'
                        OR COALESCE(UPPER(TRIM(note)), 'NA') = 'ADJUST_SHIPPING FEE_ 04.02')
                    , balance + deposit, 0)) AS DOUBLE) / 100 AS arrears
    FROM shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live
    WHERE txn_type IN (104, 906,
                        117,119,112,115,129,131,133,135,110,518,519,105,101,106,
                        202,201,301,401,1000,2001,2101,302,402,1001,2002,2102,
                        204,304,404,1003,2004,2104,200,300,400,1006,2000,2100,203,303,403,2003,2005,2006,2007,2105,2106,
                        900,512,505,907,
                        519,
                        565
                    )
    AND date(from_unixtime(create_time - 3600)) between DATE'2022-01-16' and DATE'2022-01-29'
    GROUP BY 1
    )

)i  
inner join ranking r ON i.shipper_id = r.shipper_id

---Performance 

LEFT JOIN 
(SELECT shipper_id 
    , SUM(case when snp.cnt_total_order_delivered > 0 then snp.total_online_time else null end)*1.00/count(DISTINCT case when snp.cnt_total_order_delivered > 0 then snp.report_date else null end) as online_time 
    , SUM(case when snp.cnt_total_order_delivered > 0 then snp.total_working_time else null end)*1.00/count(DISTINCT case when snp.cnt_total_order_delivered > 0 then snp.report_date else null end) as working_time    

from 
    vnfdbi_opsndrivers.snp_foody_shipper_daily_report snp 
 WHERE report_date between DATE'2022-01-16' and DATE'2022-01-29'
 GROUP BY 1 
)snp  on snp.shipper_id = r.shipper_id

---Inshift Income 
LEFT JOIN 
(SELECT uid,sum(cast(json_extract(extra_data,'$.total_income') as bigint)) as inshift_income
FROM 
shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live 
WHERE date(from_unixtime(report_date - 3600)) between DATE'2022-01-16' and DATE'2022-01-29'

GROUP BY 1 
)hub on hub.uid = r.shipper_id 

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON r.shipper_id = sm.shipper_id AND TRY_CAST(sm.grass_date AS DATE) = current_date - interval '1' day

WHERE 1 = 1 
AND r.city_name = 'Hai Phong City'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12