SELECT * 
FROM
(SELECT
         merchant_id
       , merchant_name
       , city_name
       , COUNT(DISTINCT IF(order_status = 'Delivered', id, NULL)) AS total_delivered_orders
       , COUNT(DISTINCT IF(order_status = 'Delivered' AND is_late_delivered_time = 1, id, NULL)) AS late_delivered_orders
       , COUNT(DISTINCT IF(order_status = 'Delivered' AND is_stack_order = 1, id, NULL)) AS total_delivered_stacked_orders 
       , COUNT(DISTINCT IF(order_status = 'Delivered' AND is_stack_order = 1 AND is_late_sla = 1, id, NULL)) AS total_delivered_stacked_late_sla_orders
       , COUNT(DISTINCT IF(order_status = 'Delivered' AND actual_merchant_prep_time > predicted_merchant_prep_time, id, NULL)) AS late_predicted_merchant_prep_time_orders
       , SUM(IF(is_valid_lt_merchant_prep = 1 AND order_status != 'Quit', actual_merchant_prep_time)) AS actual_merchant_prep_time
       , SUM(IF(is_valid_lt_merchant_prep = 1 AND order_status != 'Quit', predicted_merchant_prep_time)) AS predicted_merchant_prep_time
FROM
    (SELECT
             base.id
            ,base.order_status
            ,date(created_timestamp) created_date
            ,base.merchant_id
            ,base.merchant_name
            ,base.city_name
            ,base.district_name
            ,case when base.last_delivered_timestamp > (case when base.max_eta = 0 then base.estimated_delivered_timestamp else from_unixtime(base.submit_time + base.max_eta - 3600) end) then 1 else 0 end as is_late
            ,case when base.last_delivered_timestamp > base.estimated_delivered_timestamp then 1 else 0 end as is_late_delivered_time
            ,case when base.prepare_time_actual > 0  or base.confirm_timestamp <= base.pick_timestamp then 1 else 0 end as is_valid_lt_merchant_prep
            ,case when base.prepare_time_actual > 0 then cast(base.prepare_time_actual as double) / 60 else coalesce(cast(base.lt_merchant_prep as double) / 60,0) end as actual_merchant_prep_time
            ,cast(base.t_prep as double) / 60 as predicted_merchant_prep_time
            ,case when cast(base.lt_completion as double)*1.0000/60 > base.lt_sla then 1 else 0 end as is_late_sla
            ,base.is_stack_order
    FROM
            (SELECT   oct.id
                    ,oct.submit_time
                    ,from_unixtime(oct.submit_time - 3600) AS created_timestamp
                    ,case when oct.status = 7 then 'Delivered'
                        when oct.status = 8 then 'Cancelled'
                        when oct.status = 9 then 'Quit' end as order_status
                    ,city.name_en AS city_name
                    ,oct.city_id
                    ,oct.distance
                    ,oct.foody_service_id
                    ,case when oct.foody_service_id = 1 then 'Food'
                            when oct.foody_service_id in (5) then 'Market - Fresh'
                            else 'Market - Non Fresh' end as foody_service
                    ,coalesce(osl.first_auto_assign_timestamp, from_unixtime(oct.submit_time - 3600)) inflow_timestamp
                    ,date_diff('second',osl.first_auto_assign_timestamp,osl.last_incharge_timestamp) as lt_incharge ,from_unixtime(oct.final_delivered_time - 3600) last_delivered_timestamp
                    ,date_diff('second',from_unixtime(oct.submit_time - 3600),from_unixtime(oct.final_delivered_time - 3600)) as lt_completion
                    ,from_unixtime(oct.estimated_delivered_time - 3600) estimated_delivered_timestamp

                    ,from_unixtime(go.confirm_timestamp - 3600) confirm_timestamp
                    ,from_unixtime(go.pick_timestamp - 3600) pick_timestamp
                    ,date_diff('second',from_unixtime(go.confirm_timestamp - 3600),from_unixtime(go.pick_timestamp - 3600)) lt_merchant_prep

                    ,district.name_en as district_name
                    ,oct.restaurant_id as merchant_id
                    ,mpm.merchant_name
                    ,eta.max_eta
                    ,eta.t_prep
                    ,coalesce(a_prep.prepare_time_actual,0) prepare_time_actual
                    ,case when dot.delivery_distance <= 1 then 30
                        when dot.delivery_distance > 1 then least(60,30 + 5*(ceiling(dot.delivery_distance) -1))
                        else null end as lt_sla
                    ,case
                        when dot.group_id > 0 and coalesce(group_order.order_type,0) <> 200 then 1
                        when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time != ogi.create_time then 1
                        else 0 end as is_stack_order

            from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
            left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on oct.id = dot.ref_order_id and dot.ref_order_category = 0
            left join (select id, prepare_time_actual from shopeefood.foody_order_db__order_completed_merchant_search_tab__reg_daily_s0_live where grass_schema = 'foody_order_db' and order_status = 7) a_prep on a_prep.id = oct.id
            left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86

            left join shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = oct.id
            Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = oct.district_id
            left join shopeefood.foody_mart__profile_merchant_master mpm on oct.restaurant_id = mpm.merchant_id and mpm.grass_date = 'current'
            left join
                (SELECT order_id
                    ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                    ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                    ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
                    ,min(case when status = 13 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_confirmed_timestamp
                    ,max(case when status = 9 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_cancel_timestamp
                from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                where 1=1
                group by order_id
                )osl on osl.order_id = oct.id

           LEFT JOIN
                    (SELECT
                        eta.id
                        ,eta.order_id
                        ,from_unixtime(eta.create_time - 3600) as create_time
                        ,from_unixtime(eta.update_time - 3600) as update_time
                        ,coalesce(cast(json_extract(eta.eta_data,'$.t_assign.value') as INT),0) as t_assign
                        ,coalesce(cast(json_extract(eta.eta_data,'$.t_confirm.value') as INT),0) as t_confirm
                        ,coalesce(cast(json_extract(eta.eta_data,'$.t_pickup.value') as INT),0) as t_pickup
                        ,coalesce(cast(json_extract(eta.eta_data,'$.t_prep.value') as INT),0) as t_prep
                        ,coalesce(cast(json_extract(eta.eta_data,'$.t_dropoff.value') as INT),0) as t_dropoff
                        ,coalesce(cast(json_extract(eta.eta_data,'$.eta_range.max') as INT),0) as max_eta
                        ,coalesce(cast(json_extract(eta.eta_data,'$.eta_range.min') as INT),0) as min_eta
                        ,eta_data
                        from shopeefood.data_mining_db__order_eta_data_tab__reg_daily_s0_live eta
                    )eta on eta.order_id = oct.id
           LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category

                LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                                    and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                        and ogm_filter.create_time >  ogm.create_time
                LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
           LEFT JOIN
                    (SELECT
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

                    from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                            from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                            where status in (3,4) UNION

                            SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                            from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                            where status in (3,4) )a

                        LEFT JOIN
                                (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                                from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                                where status in (3,4) UNION

                                SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                                from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                where status in (3,4) )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time

                        LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

                    where 1=1
                    and a_filter.order_id is null and a.order_type = 200
                    GROUP BY 1,2,3,4,5,6,7,8
                    ) group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category


            WHERE 1=1
            and date(from_unixtime(oct.submit_time - 3600)) BETWEEN from_iso8601_date('2021-10-31') AND from_iso8601_date('2021-11-01')
            and ogm_filter.create_time is null
            --and city.name_en IN ('HCM City', 'Ha Noi City')
            ) base
    )
GROUP BY 1,2,3)
where total_delivered_stacked_orders >0 