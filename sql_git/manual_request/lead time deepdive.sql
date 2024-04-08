SELECT     base3.source
          ,base3.created_date

        --   ,case when base3.created_date = date('${ref_date_1}') then cast(base3.created_date as varchar)
        --         when base3.created_date = date('${ref_date_2}') then cast(base3.created_date as varchar)
        --         -- when base3.created_date = date('2022-01-15') then cast(base3.created_date as varchar)
        --         -- when base3.created_date = date('2022-02-25') then cast(base3.created_date as varchar)
        --         else 'D7' end as date_group
          ,base3.created_hour
       --   ,base3.is_asap
          ,base3.city_group

          ,base3.is_stack_order
          ,base3.is_group_order
          ,base3.order_type
          ,base3.driver_policy
       --   ,base3.range_lt_from_promise_to_actual_delivered
        --  ,base3.is_late
        --  ,base3.late_break

          ,sum(base3.total_order) total_order
          ,sum(case when base3.is_del = 1 then base3.total_order else 0 end) total_order_delivered --base3.is_valid_submit_to_del = 1
          ,sum(case when base3.is_del = 1 then base3.total_order_del_same_day else 0 end) total_order_delivered_same_day

          ,sum(case when base3.is_asap = 1 and base3.is_del = 1 then base3.total_order else 0 end) total_order_delivered_asap
          ,sum(case when base3.is_asap = 1 and base3.is_del = 1 then base3.total_order_del_same_day else 0 end) total_order_delivered_same_day_asap

          ,sum(case when base3.is_cancel = 1 then base3.total_order else 0 end) total_order_cancelled
          ,sum(case when base3.is_del = 1 and base3.is_late = 1 then base3.total_order else 0 end) total_order_delivered_late
          ,sum(case when base3.is_del = 1 and base3.is_late = 1 and base3.late_break = 'b. Late > 5 mins' then base3.total_order else 0 end) total_order_delivered_late_more_than_5_min

          ,sum(case when base3.is_del = 1 and base3.is_late_eta_max = 1 then base3.total_order else 0 end) total_order_delivered_late_eta_max
          ,sum(case when base3.is_del = 1 and base3.is_late_eta_max = 1 and base3.late_break_eta_max = 'b. Late > 5 mins' then base3.total_order else 0 end) total_order_delivered_late_more_than_5_min_eta_max

          ,sum(case when base3.is_asap = 1 and base3.is_del = 1 then base3.lt_completion_original else 0 end) total_lt_completion_original
          ,sum(case when base3.is_asap = 1 and base3.is_del = 1 then base3.lt_completion_adjusted else 0 end) total_lt_completion_adjusted
          ,sum(case when base3.is_asap = 1 and base3.is_del = 1 then base3.lt_incharge else 0 end) total_lt_incharge
          ,sum(case when base3.is_del = 1 then base3.distance else 0 end) total_distance
          ,sum(case when base3.is_del = 1 then base3.shipper_rating else 0 end) total_shipper_rating
          ,sum(case when base3.is_del = 1 then base3.shipping_fee else 0 end) total_shipping_fee


FROM
(
SELECT     base2.source
          ,base2.created_date
          ,base2.created_year_week
          ,base2.created_year_month
          ,base2.created_hour

          ,base2.is_stack_order
          ,base2.is_group_order
          ,base2.is_asap
          ,base2.order_status
          ,case when order_status = 'Delivered' then 1 else 0 end as is_del
          ,case when order_status = 'Cancelled' then 1 else 0 end as is_cancel
          ,base2.is_valid_incharge
          ,base2.is_valid_submit_to_del
          ,base2.city_group
          ,case when base2.hub_id > 0 then 'Hub order' else 'Non hub order' end as order_type
          ,case when base2.policy = 2 then 'Inshift' else 'Out shift'  end as driver_policy


          ,base2.range_lt_from_promise_to_actual_delivered
          ,base2.is_breached_customer_promise is_late
          ,base2.late_break
          ,base2.is_late_eta_max
          ,base2.late_break_eta_max

          ,count(distinct case when date(base2.last_delivered_timestamp) = base2.created_date then base2.uid else null end) total_order_del_same_day
          ,count(distinct base2.uid) total_order
          ,sum(case when date(base2.last_delivered_timestamp) = base2.created_date then base2.lt_completion_adjusted else null end)*1.0000/60 lt_completion_adjusted
          ,sum(case when date(base2.last_delivered_timestamp) = base2.created_date then base2.lt_completion_original else null end)*1.0000/60 lt_completion_original
          ,sum(base2.lt_incharge)*1.0000/60 lt_incharge
          ,sum(base2.distance) distance
          ,sum(base2.shipper_rating) shipper_rating
          ,sum(base2.shipping_fee) shipping_fee
FROM
(
SELECT
         base1.uid
        ,base1.order_code
        ,case when base1.source = 'order_delivery' then 'NowFood' else 'NowShip' end as source
        ,base1.group_id
        ,base1.group_code
        ,base1.created_date
        ,base1.created_year_week
        ,base1.created_year_month
        ,base1.inflow_date
        ,base1.inflow_day_of_week
        ,base1.report_date
        ,base1.created_hour
        ,base1.inflow_hour

        ,base1.city_group
        ,base1.city_name
        ,base1.district_name
        ,base1.order_status
        ,base1.is_asap
        ,base1.is_stack_order
        ,base1.is_group_order
        ,base1.estimated_delivered_time
        ,base1.last_delivered_timestamp

        ,case when (base1.is_stack_order = 0 or base1.is_group_order = 0) then base1.lt_completion
              when (base1.is_stack_order = 1 or base1.is_group_order = 1) then date_diff('second',base1.group_stack_min_created_timestamp,base1.group_stack_max_last_delivered_timestamp)/cast(base1.total_order_in_group as double)
              else base1.lt_completion end as lt_completion_adjusted

        ,base1.lt_completion lt_completion_original
        ,base1.lt_incharge
        ,base1.is_breached_customer_promise
        ,base1.is_late_eta_max
        ,case when base1.lt_from_promise_to_actual_delivered is null then null
              when base1.lt_from_promise_to_actual_delivered < -10*60 then '4. Early 10+ mins'
              when base1.lt_from_promise_to_actual_delivered < -5*60 then '5. Early 5-10 mins'
              when base1.lt_from_promise_to_actual_delivered <= 0*60 then '6. Early 0-5 mins'
              when base1.lt_from_promise_to_actual_delivered <= 10*60 then '1. Late 0-10 mins'
              when base1.lt_from_promise_to_actual_delivered <= 20*60 then '2. Late 10-20 mins'
              when base1.lt_from_promise_to_actual_delivered > 20*60 then '3. Late 20+ mins'
              else null end as range_lt_from_promise_to_actual_delivered

        ,case when base1.lt_from_promise_to_actual_delivered is null then null
              when base1.lt_from_promise_to_actual_delivered < -10*60 then 'a. Late < 5 min'
              when base1.lt_from_promise_to_actual_delivered < -5*60 then 'a. Late < 5 min'
              when base1.lt_from_promise_to_actual_delivered <= 0*60 then 'a. Late < 5 min'
              when base1.lt_from_promise_to_actual_delivered <= 5*60 then 'a. Late < 5 min'
              when base1.lt_from_promise_to_actual_delivered <= 10*60 then 'b. Late > 5 mins'
              when base1.lt_from_promise_to_actual_delivered > 10*60 then 'b. Late > 5 mins'
              else null end as late_break
        ,case when base1.lt_from_eta_max_to_actual_delivered is null then null
              when base1.lt_from_eta_max_to_actual_delivered < -10*60 then 'a. Late < 5 min'
              when base1.lt_from_eta_max_to_actual_delivered < -5*60 then 'a. Late < 5 min'
              when base1.lt_from_eta_max_to_actual_delivered <= 0*60 then 'a. Late < 5 min'
              when base1.lt_from_eta_max_to_actual_delivered <= 5*60 then 'a. Late < 5 min'
              when base1.lt_from_eta_max_to_actual_delivered <= 10*60 then 'b. Late > 5 mins'
              when base1.lt_from_eta_max_to_actual_delivered > 10*60 then 'b. Late > 5 mins'
              else null end as late_break_eta_max
        ,base1.distance
        ,base1.hub_id
        ,base1.policy
        ,base1.shipper_rating
        ,base1.shipping_fee

        ,base1.is_valid_incharge
        ,base1.is_valid_submit_to_del
FROM
        (
        SELECT base.shipper_id
              ,base.city_name
              ,base.city_group
              ,base.district_name
              ,base.report_date
              ,base.created_date
              ,base.created_year_week
              ,base.created_year_month
              ,date(base.inflow_timestamp) inflow_date

              ,base.order_id
              ,base.order_code
              ,concat(base.source,'_',cast(base.order_id as varchar)) as uid
              ,base.ref_order_category order_type
              ,base.source

              ,case when base.order_status = 400 then 'Delivered'
                    when base.order_status = 401 then 'Quit'
                    when base.order_status in (402,403,404) then 'Cancelled'
                    when base.order_status in (405,406,407) then 'Others'
                    else 'Others' end as order_status

              ,base.order_status_group
              ,base.is_stack_order
              ,base.is_group_order
              ,base.total_order_in_group
              ,base.group_code
              ,base.group_id

              ,base.first_auto_assign_timestamp
              ,base.last_delivered_timestamp
              ,base.estimated_delivered_time
              ,base.inflow_timestamp
              ,date_format(base.inflow_timestamp,'%a') inflow_day_of_week

              ,HOUR(base.created_timestamp) created_hour
              ,HOUR(base.inflow_timestamp) inflow_hour
              ,case when MINUTE(base.inflow_timestamp) <= 5 then '01. Min 0 - 5'
                    when MINUTE(base.inflow_timestamp) <= 10 then '02. Min 5 - 10'
                    when MINUTE(base.inflow_timestamp) <= 15 then '03. Min 10 - 15'
                    when MINUTE(base.inflow_timestamp) <= 20 then '04. Min 15 - 20'
                    when MINUTE(base.inflow_timestamp) <= 25 then '05. Min 20 - 25'
                    when MINUTE(base.inflow_timestamp) <= 30 then '06. Min 25 - 30'
                    when MINUTE(base.inflow_timestamp) <= 35 then '07. Min 30 - 35'
                    when MINUTE(base.inflow_timestamp) <= 40 then '08. Min 35 - 40'
                    when MINUTE(base.inflow_timestamp) <= 45 then '09. Min 40 - 45'
                    when MINUTE(base.inflow_timestamp) <= 50 then '10. Min 45 - 50'
                    when MINUTE(base.inflow_timestamp) <= 55 then '11. Min 50 - 55'
                    when MINUTE(base.inflow_timestamp) <= 60 then '12. Min 55 - 60'
                    else null end inflow_minute_range

              ,base.is_asap
              ,case when (base.is_stack_order = 0 or base.is_group_order = 0) then base.delivery_distance
                    when (base.is_stack_order = 1 or base.is_group_order = 1) then base.overall_distance * 1.0000/base.total_order_in_group
                    else null end as adjusted_distance
              ,base.overall_distance distance
              ,base.group_stack_min_created_timestamp
              ,base.group_stack_max_last_delivered_timestamp

              ,case when base.first_auto_assign_timestamp < base.last_incharge_timestamp then 1 else 0 end as is_valid_incharge
              ,case when base.created_timestamp < base.last_delivered_timestamp then 1 else 0 end as is_valid_submit_to_del

              ,date_diff('second',base.first_auto_assign_timestamp,base.last_incharge_timestamp) as lt_incharge
              ,date_diff('second',base.created_timestamp,base.last_delivered_timestamp) as lt_completion

              ,case when base.last_delivered_timestamp > (case when base.max_eta = 0 then base.estimated_delivered_time else from_unixtime(base.submitted_time + base.max_eta - 60*60) end) then 1 else 0 end as is_late_eta_max
              ,case when base.last_delivered_timestamp > base.estimated_delivered_time then 1 else 0 end as is_breached_customer_promise
              ,date_diff('second',base.estimated_delivered_time,base.last_delivered_timestamp) lt_from_promise_to_actual_delivered
              ,date_diff('second',(case when base.max_eta = 0 then base.estimated_delivered_time else from_unixtime(base.submitted_time + base.max_eta - 60*60) end),base.last_delivered_timestamp) lt_from_eta_max_to_actual_delivered


              ,base.hub_id
              ,base.shipper_rating
              ,base.shipping_fee
              ,base.policy
        FROM
                    (
                    SELECT dot.uid as shipper_id
                          ,dot.ref_order_id as order_id
                          ,dot.ref_order_code as order_code
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

                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 then 1 else 0 end as  is_group_order
                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 1 else 0 end as  is_stack_order 
                          ,ogi.group_code
                          ,ogm.group_id
                          ,dot.submitted_time
                          ,dot.is_asap
                          ,ogi.distance*1.0000/(100*1000) overall_distance
                          ,dot.delivery_distance*1.0000/1000 delivery_distance

                          ,case when dot.is_asap = 0 and dot.ref_order_status in (7,11) then date(from_unixtime(dot.real_drop_time - 60*60)) else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
                          ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
                          ,from_unixtime(dot.submitted_time- 60*60) created_timestamp
                          ,case when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
                                when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                else YEAR(cast(from_unixtime(dot.submitted_time - 60*60) as date))*100 + WEEK(cast(from_unixtime(dot.submitted_time - 60*60) as date)) end as created_year_week
                          ,concat(cast(YEAR(from_unixtime(dot.submitted_time - 60*60)) as VARCHAR),'-',date_format(from_unixtime(dot.submitted_time - 60*60),'%b')) as created_year_month

                          ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
                          ,case when dot.estimated_drop_time = 0 then null else from_unixtime(dot.estimated_drop_time - 60*60) end as estimated_delivered_time
                          ,fa.first_auto_assign_timestamp
                          ,fa.last_incharge_timestamp

                          ,case when dot.is_asap = 0 then fa.first_auto_assign_timestamp else from_unixtime(dot.submitted_time- 60*60) end as inflow_timestamp

                          ,order_rank.min_created_timestamp as group_stack_min_created_timestamp
                          ,order_rank.max_last_delivered_timestamp as group_stack_max_last_delivered_timestamp
                          ,COALESCE(order_rank.total_order_in_group,0) total_order_in_group

                          ,district.name_en as district_name
                          ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
                          ,case when dot.pick_city_id = 217 then 'HCM'
                                when dot.pick_city_id = 218 then 'HN'
                                when dot.pick_city_id = 219 then 'DN'
                                ELSE 'OTH' end as city_group
                          ,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
                          ,COALESCE(cast(json_extract(dotet.order_data,'$.shipper_policy.type') as BIGINT ),0) as policy
                          ,COALESCE(srate.shipper_rating,5) shipper_rating

                         -- ,dot.delivery_cost*1.0000/100 as shipping_fee
                          ,COALESCE(sf.shipping_fee,0) shipping_fee
                         -- eta max
                          ,eta.max_eta
                    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

                    left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
                    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category
                    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                                        and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                                        and ogm_filter.create_time >  ogm.create_time
                    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id

                    left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

                    Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = dot.pick_district_id

                    LEFT JOIN
                            (SELECT order_id
                                  ,shipper_uid as shipper_id
                                  ,case when cfo.shipper_rate = 0 then null
                                        when cfo.shipper_rate = 1 or cfo.shipper_rate = 101 then 1
                                        when cfo.shipper_rate = 2 or cfo.shipper_rate = 102 then 2
                                        when cfo.shipper_rate = 3 or cfo.shipper_rate = 103 then 3
                                        when cfo.shipper_rate = 104 then 4
                                        when cfo.shipper_rate = 105 then 5
                                        else null end as shipper_rating
                                  ,from_unixtime(cfo.create_time - 60*60) as create_ts

                            FROM  shopeefood.foody_user_activity_db__customer_feedback_order_tab__reg_daily_s0_live cfo
                            )srate ON dot.ref_order_id = srate.order_id and dot.uid = srate.shipper_id


                    LEFT JOIN
                            (SELECT ogm.group_id
                                   ,ogi.group_code
                                   ,min(dot.created_timestamp) as min_created_timestamp
                                   ,min(dot.last_picked_timestamp) as min_last_picked_timestamp
                                   ,max(dot.last_delivered_timestamp) as max_last_delivered_timestamp
                                   ,count (distinct dot.ref_order_id) as total_order_in_group
                             FROM
                                (
                                SELECT dot.uid as shipper_id
                                      ,dot.ref_order_id
                                      ,dot.ref_order_code
                                      ,dot.ref_order_category
                                      ,case when dot.ref_order_category = 0 then 'order_delivery'
                                            when dot.ref_order_category = 3 then 'now_moto'
                                            when dot.ref_order_category = 4 then 'now_ship'
                                            when dot.ref_order_category = 5 then 'now_ship'
                                            when dot.ref_order_category = 6 then 'now_ship_shopee'
                                            when dot.ref_order_category = 7 then 'now_ship_sameday'
                                                else null end source
                                      ,dot.ref_order_status
                                      ,dot.group_id
                                      ,case when dot.group_id > 0 then 1 else 0 end as is_stack
                                      ,dot.delivery_distance*1.0000/1000 delivery_distance
                                      ,from_unixtime(dot.submitted_time - 60*60) created_timestamp
                                      ,from_unixtime(dot.real_drop_time - 60*60) last_delivered_timestamp
                                      ,from_unixtime(dot.real_pick_time - 60*60) last_picked_timestamp
                                      ,dot.is_asap
                                      ,case when dot.is_asap = 0 and dot.ref_order_status in (7,11) then date(from_unixtime(dot.real_drop_time - 60*60)) else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
                                      ,date(from_unixtime(dot.submitted_time- 60*60)) created_date

                                FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
                                )dot
                                LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category
                                LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                                                    and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                                                    and ogm_filter.create_time >  ogm.create_time
                                        LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
                              WHERE 1=1
                              and ogm.group_id is not null
                              and ogm_filter.create_time is null

                              GROUP BY 1,2
                             )order_rank on order_rank.group_id = ogm.group_id

                    LEFT JOIN
                    (
                    SELECT   order_id , 0 as order_type
                            ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                            ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live

                            group by 1,2

                    UNION

                    SELECT   ns.order_id, ns.order_type ,min(from_unixtime(create_time - 60*60)) first_auto_assign_timestamp
                            ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                    FROM
                            ( SELECT order_id, order_type , create_time , update_time, status

                             from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day

                             UNION

                             SELECT order_id, order_type, create_time , update_time, status

                             from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                             )ns
                    GROUP BY 1,2
                    )fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type


                    -----
                    LEFT JOIN
                    (
                    SELECT   id as order_id , 0 as order_type
                            ,delivery_cost_amount as shipping_fee
                            from shopeefood.foody_mart__fact_gross_order_join_detail

                            group by 1,2,3

                    UNION

                    SELECT   ns.id as order_id
                            ,case when ns.booking_type = 2 and ns.booking_service_type = 1 then 4
                                  when ns.booking_type = 3 and ns.booking_service_type = 1 then 5
                                  when ns.booking_type = 4 and ns.booking_service_type = 1 then 6
                                  when ns.booking_type = 2 and ns.booking_service_type = 2 then 7
                                  else 10 end as order_type
                            ,ns.shipping_fee*1.00/100 as shipping_fee
                    FROM
                            (SELECT id,concat('now_ship_',cast(id as VARCHAR)) as uid, booking_type,shipper_id, distance,code,create_time, status, payment_method,'now_ship' as original_source,city_id,cast(json_extract(extra_data,'$.pick_address_info.district_id') as DOUBLE) as district_id , pick_real_time,drop_real_time,shipping_fee
                                 ,booking_service_type
                                from shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live
                                where is_deleted = 0
                            UNION

                            SELECT id,concat('now_ship_shopee_',cast(id as VARCHAR)) as uid, 4 as booking_type, shipper_id,distance,code,create_time,status,1 as payment_method,'now_ship_shopee' as original_source,city_id,cast(json_extract(extra_data,'$.sender_info.district_id') as DOUBLE) as district_id, pick_real_time,drop_real_time,shipping_fee
                                  ,booking_service_type
                                from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live

                            )ns
                    GROUP BY 1,2,3
                    )sf on dot.ref_order_id = sf.order_id and dot.ref_order_category = sf.order_type

                    --- eta for each stage
                        LEFT JOIN
                            (SELECT -- date(from_unixtime(create_time - 60*60)) as date_
                                -- ,count(*) as test
                                eta.id
                                ,eta.order_id
                                ,from_unixtime(eta.create_time - 60*60) as create_time
                                ,from_unixtime(eta.update_time - 60*60) as update_time
                                ,coalesce(cast(json_extract(eta.eta_data,'$.t_assign.value') as INT),0) as t_assign
                                ,coalesce(cast(json_extract(eta.eta_data,'$.t_confirm.value') as INT),0) as t_confirm
                                ,coalesce(cast(json_extract(eta.eta_data,'$.t_pickup.value') as INT),0) as t_pickup
                                ,coalesce(cast(json_extract(eta.eta_data,'$.t_prep.value') as INT),0) as t_prep
                                ,coalesce(cast(json_extract(eta.eta_data,'$.t_dropoff.value') as INT),0) as t_dropoff
                                ,coalesce(cast(json_extract(eta.eta_data,'$.eta_range.max') as INT),0) as max_eta
                                ,coalesce(cast(json_extract(eta.eta_data,'$.eta_range.min') as INT),0) as min_eta
                                ,eta_data

                                from shopeefood.data_mining_db__order_eta_data_tab__reg_daily_s0_live eta
                              --  where order_id > 181625182
                                --where date(from_unixtime(create_time - 60*60)) >= date('2020-09-01')

                            --    limit 1000
                                 -- id = 15787525

                                --GROUP BY 1 ORDER BY 1 ASC
                            )eta on eta.order_id =  dot.ref_order_id  -- oct.id
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
                                    where status in (3,4) -- shipper incharge
                            
                                    UNION
                                
                                    SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                            
                                    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                    where status in (3,4) -- shipper incharge
                                )a                                
                                
                                -- take last incharge
                                LEFT JOIN 
                                        (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
                                
                                        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                                        where status in (3,4) -- shipper incharge
                                
                                        UNION
                                    
                                        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
                                
                                        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                        where status in (3,4) -- shipper incharge
                                    )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time
                                    
                                -- auto accept 
                                
                               LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end
                            
                            where 1=1
                            and a_filter.order_id is null -- take last incharge
                            and a.order_type = 200
                            
                            GROUP BY 1,2,3,4,5,6,7,8
                            
                            )group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category 

                    WHERE 1=1
                    and ogm_filter.create_time is null

                    --and dot.ref_order_status in (7,11)

                    )base

        WHERE 1=1
        and base.created_date >= date('2021-11-11')
        and base.created_date < date(current_date)
        and base.order_status_group = 'Completed'
      --  and base.source = 'order_delivery'
        )base1

WHERE 1=1

--and base1.inflow_date >= date('2020-10-26') and base1.inflow_date < date(current_date)
--and base1.hub <> 'Non-Hub'
)base2

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
)base3

WHERE 1=1

and base3.created_date  between ${start_date} and ${end_date}

GROUP BY 1,2,3,4,5,6,7,8

--- cal: total_lt_completion_adjusted/total_order_delivered_asap
