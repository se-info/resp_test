SELECT *

FROM
(

SELECT      base3.created_date
          ,case when base3.created_date between date('2022-01-01') and date('2022-01-02') then 202152
                when base3.created_date = date'2023-01-01' then 202252
      else base3.created_year_week end as created_year_week
          ,base3.city_group
          ,base3.city_name

          ,base3.is_stack_order

          ,sum(base3.total_order) total_order
          ,sum(case when base3.is_del = 1 and base3.is_valid_submit_to_del = 1 then base3.total_order else 0 end) total_order_delivered
          ,sum(case when base3.is_cancel = 1 then base3.total_order else 0 end) total_order_cancelled

          ,sum(case when base3.is_assigned = 1 then base3.total_order else 0 end) total_order_assigned

          ,sum(case when base3.is_assigned_success = 1 then base3.total_order else 0 end) total_order_assigned_success
          ,sum(case when base3.is_1st_assigned_success = 1 then base3.total_order else 0 end) total_order_1st_assigned_success

          ,sum(case when base3.is_assigned_failed = 1 then base3.total_order else 0 end) total_order_assigned_failed
          ,sum(case when base3.is_assigned_failed_once = 1 then base3.total_order else 0 end) total_order_assigned_failed_once
          ,sum(case when base3.is_assigned_failed_more_than_once = 1 then base3.total_order else 0 end) total_order_assigned_failed_more_than_once

          ,sum(case when base3.is_assign_failed_no_timeout = 1 then base3.total_order else 0 end) total_order_assign_failed_no_timeout
          ,sum(case when base3.is_assign_failed_have_timeout = 1 then base3.total_order else 0 end) total_order_assign_failed_have_timeout

          ,sum(base3.total_assign_times) sum_total_assign_times
          ,sum(base3.no_incharged) sum_cnt_incharged
          ,sum(base3.no_ignored) sum_cnt_ignored
          ,sum(base3.no_deny) sum_cnt_deny
          ,sum(base3.no_shipper_checkout) sum_cnt_shipper_checkout
          ,sum(base3.no_incharge_error) sum_cnt_incharge_error
          ,sum(base3.no_timeout) sum_cnt_timeout
          ,sum(base3.no_other_assign_status) sum_cnt_other_assign_status
FROM
(
SELECT     base2.source
          ,base2.created_date
          ,base2.created_year_week
          ,base2.created_year_month

          ,base2.is_asap
          ,base2.order_status
          ,case when order_status = 'Delivered' then 1 else 0 end as is_del
          ,case when order_status = 'Cancelled' then 1 else 0 end as is_cancel

          ,base2.city_group
          ,base2.city_name
          ,base2.is_stack_order
          ,base2.is_valid_submit_to_del

          ,base2.is_assigned
          ,base2.is_1st_assigned_success
          ,base2.is_assigned_success

          ,base2.is_assigned_failed
          ,base2.is_assigned_failed_once
          ,base2.is_assigned_failed_more_than_once

          ,base2.is_assign_failed_no_timeout
          ,base2.is_assign_failed_have_timeout

          ,count(distinct base2.uid) total_order

          ,sum(base2.total_assign_times) total_assign_times
          ,sum(base2.no_incharged) no_incharged
          ,sum(base2.no_ignored) no_ignored
          ,sum(base2.no_deny) no_deny
          ,sum(base2.no_shipper_checkout) no_shipper_checkout
          ,sum(base2.no_incharge_error) no_incharge_error
          ,sum(base2.no_timeout) no_timeout
          ,sum(base2.no_other_assign_status) no_other_assign_status
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
        ,base1.estimated_delivered_time
        ,base1.last_delivered_timestamp
        ,base1.last_picked_timestamp
        ,base1.max_arrived_at_merchant_timestamp
        ,base1.max_arrived_at_buyer_timestamp
        ,case when base1.is_stack_order = 0 then base1.lt_completion
              when base1.is_stack_order = 1 then date_diff('second',base1.group_stack_min_created_timestamp,base1.group_stack_max_last_delivered_timestamp)*1.0000/2
              else base1.lt_completion end as lt_completion_adjusted

        ,base1.lt_completion lt_completion_original
        ,base1.lt_incharge
        ,base1.is_late_delivered_time
        ,base1.is_late_arrive_buyer
        ,case when base1.lt_from_promise_to_actual_delivered is null then null
              when base1.lt_from_promise_to_actual_delivered < -10*60 then '4. Early 10+ mins'
              when base1.lt_from_promise_to_actual_delivered < -5*60 then '5. Early 5-10 mins'
              when base1.lt_from_promise_to_actual_delivered <= 0*60 then '6. Early 0-5 mins'
              when base1.lt_from_promise_to_actual_delivered <= 10*60 then '1. Late 0-10 mins'
              when base1.lt_from_promise_to_actual_delivered <= 20*60 then '2. Late 10-20 mins'
              when base1.lt_from_promise_to_actual_delivered > 20*60 then '3. Late 20+ mins'
              else null end as range_lt_from_promise_to_actual_delivered

        ,base1.lt_assign_to_arrive_at_merchant
        ,base1.lt_incharge_to_arrive_at_merchant
        ,base1.lt_pick_to_arrive_at_buyer
        ,base1.lt_arrive_at_buyer_to_del
        ,base1.lt_arrive_at_merchant_to_pick

        ,base1.distance
        ,case when base1.distance <=3 then 1 else 0 end as is_order_less_than_3km
        ,case when base1.distance <= 3 then '1. 0-3km'
              when base1.distance <= 4 then '2. 3-4km'
              when base1.distance <= 5 then '3. 4-5km'
              when base1.distance > 5 then '5. 5km+'
              else null end as distance_range
        ,base1.hub_id
        ,base1.pick_hub_id
        ,base1.hub_name
        ,base1.pick_hub_name
        ,base1.is_hub_driver
        ,base1.shipper_rating
        ,base1.shipping_fee

        ,base1.is_valid_incharge
        ,base1.is_valid_submit_to_del
        ,base1.is_valid_lt_arrive_at_merchant
        ,base1.is_valid_lt_incharge_arrive_at_merchant
        ,base1.is_valid_lt_arrive_at_buyer
        ,base1.is_valid_lt_arrive_at_buyer_to_del
        ,base1.is_valid_lt_arrive_at_merchant_to_pick

        ,case when (base1.cnt_timeout + coalesce(assign.no_assign,0) > 0) or (base1.cnt_timeout + coalesce(assign.no_assign,0) = 0 and cancel.cancel_reason = 'No driver') then 1 else 0 end as is_assigned
        ,case when coalesce(assign.no_incharged,0) > 0 and (base1.cnt_timeout + coalesce(assign.no_assign,0)) = 1 then 1 else 0 end as is_1st_assigned_success
        ,case when coalesce(assign.no_incharged,0) > 0 and (base1.cnt_timeout + coalesce(assign.no_assign,0)) > 0 then 1 else 0 end as is_assigned_success

        ,case when (coalesce(assign.no_incharged,0) = 0 and (base1.cnt_timeout + coalesce(assign.no_assign,0)) > 0) or (base1.cnt_timeout + coalesce(assign.no_assign,0) = 0 and cancel.cancel_reason = 'No driver') then 1 else 0 end as is_assigned_failed
        ,case when coalesce(assign.no_incharged,0) = 0 and (base1.cnt_timeout + coalesce(assign.no_assign,0)) = 1 then 1 else 0 end as is_assigned_failed_once
        ,case when coalesce(assign.no_incharged,0) = 0 and (base1.cnt_timeout + coalesce(assign.no_assign,0)) > 1 then 1 else 0 end as is_assigned_failed_more_than_once

        ,case when (coalesce(assign.no_incharged,0) = 0 and (base1.cnt_timeout + coalesce(assign.no_assign,0)) > 0 and base1.cnt_timeout = 0) or (base1.cnt_timeout + coalesce(assign.no_assign,0) = 0 and cancel.cancel_reason = 'No driver') then 1 else 0 end as is_assign_failed_no_timeout
        ,case when coalesce(assign.no_incharged,0) = 0 and (base1.cnt_timeout + coalesce(assign.no_assign,0)) > 0 and base1.cnt_timeout > 0 then 1 else 0 end as is_assign_failed_have_timeout

        ,case when base1.cnt_timeout > 0 then coalesce(assign.no_assign,0) + base1.cnt_timeout else coalesce(assign.no_assign,0) end total_assign_times
        ,coalesce(assign.no_incharged,0) no_incharged
        ,coalesce(assign.no_ignored,0) no_ignored
        ,coalesce(assign.no_deny,0) no_deny
        ,coalesce(assign.no_shipper_checkout,0) no_shipper_checkout
        ,coalesce(assign.no_incharge_error,0) no_incharge_error
        ,coalesce(assign.no_other_assign_status,0) no_other_assign_status
        ,base1.cnt_timeout as no_timeout
        ,cancel.cancel_reason
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
              ,base.is_stack is_stack_order
              ,base.group_code
              ,base.group_id

              ,base.first_auto_assign_timestamp
              ,base.last_incharge_timestamp
              ,base.last_delivered_timestamp
              ,base.estimated_delivered_time
              ,base.last_picked_timestamp
              ,base.max_arrived_at_merchant_timestamp
              ,base.max_arrived_at_buyer_timestamp
              ,base.inflow_timestamp
              ,date_format(base.inflow_timestamp,'%a') inflow_day_of_week

              ,EXTRACT(HOUR from base.created_timestamp) created_hour
              ,EXTRACT(HOUR from base.inflow_timestamp) inflow_hour
              ,case when EXTRACT(MINUTE from base.inflow_timestamp) <= 5 then '01. Min 0 - 5'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 10 then '02. Min 5 - 10'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 15 then '03. Min 10 - 15'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 20 then '04. Min 15 - 20'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 25 then '05. Min 20 - 25'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 30 then '06. Min 25 - 30'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 35 then '07. Min 30 - 35'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 40 then '08. Min 35 - 40'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 45 then '09. Min 40 - 45'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 50 then '10. Min 45 - 50'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 55 then '11. Min 50 - 55'
                    when EXTRACT(MINUTE from base.inflow_timestamp) <= 60 then '12. Min 55 - 60'
                    else null end inflow_minute_range

              ,base.is_asap
              ,case when base.is_stack = 0 then base.delivery_distance
                    when base.is_stack = 1 then base.overall_distance * 1.0000/base.total_order_in_group
                    else null end as adjusted_distance
              ,base.delivery_distance distance
              ,base.group_stack_min_created_timestamp
              ,base.group_stack_max_last_delivered_timestamp

              ,case when base.first_auto_assign_timestamp < base.last_incharge_timestamp then 1 else 0 end as is_valid_incharge
              ,case when base.created_timestamp <= base.last_delivered_timestamp then 1 else 0 end as is_valid_submit_to_del

              ,date_diff('second',base.first_auto_assign_timestamp,base.last_incharge_timestamp)*1.0000/60 as lt_incharge
              ,date_diff('second',base.created_timestamp,base.last_delivered_timestamp) as lt_completion

              ,case when base.last_delivered_timestamp > base.estimated_delivered_time then 1 else 0 end as is_late_delivered_time
              ,case when base.max_arrived_at_buyer_timestamp > base.estimated_delivered_time then 1 else 0 end as is_late_arrive_buyer
              ,date_diff('second',base.estimated_delivered_time,base.last_delivered_timestamp) lt_from_promise_to_actual_delivered

              ,date_diff('second',base.first_auto_assign_timestamp,base.max_arrived_at_merchant_timestamp)*1.0000/60 as lt_assign_to_arrive_at_merchant
              ,date_diff('second',base.last_incharge_timestamp,base.max_arrived_at_merchant_timestamp)*1.0000/60 as lt_incharge_to_arrive_at_merchant
              ,date_diff('second',base.last_picked_timestamp ,base.max_arrived_at_buyer_timestamp)*1.0000/60 as lt_pick_to_arrive_at_buyer
              ,date_diff('second',base.max_arrived_at_buyer_timestamp ,base.last_delivered_timestamp)*1.0000/60 as lt_arrive_at_buyer_to_del
              ,date_diff('second',base.max_arrived_at_merchant_timestamp ,base.last_picked_timestamp)*1.0000/60 as lt_arrive_at_merchant_to_pick

              ,case when base.first_auto_assign_timestamp <= base.max_arrived_at_merchant_timestamp then 1 else 0 end as is_valid_lt_arrive_at_merchant
              ,case when base.last_incharge_timestamp <= base.max_arrived_at_merchant_timestamp then 1 else 0 end as is_valid_lt_incharge_arrive_at_merchant
              ,case when base.last_picked_timestamp <= base.max_arrived_at_buyer_timestamp then 1 else 0 end as is_valid_lt_arrive_at_buyer
              ,case when base.max_arrived_at_buyer_timestamp <= base.last_delivered_timestamp then 1 else 0 end as is_valid_lt_arrive_at_buyer_to_del
              ,case when base.max_arrived_at_merchant_timestamp <= base.last_picked_timestamp then 1 else 0 end as is_valid_lt_arrive_at_merchant_to_pick

              ,base.hub_id
              ,base.pick_hub_id

              ,case when base.hub_id = 6 then 'Hub D4'
                    when base.hub_id = 7 then 'Hub D10'
                    when base.hub_id = 9 then 'Hub D5'
                    when base.hub_id = 10 then 'Hub Dong Da A'
                    when base.hub_id = 11 then 'Hub Dong Da B'
                    when base.hub_id = 19 then 'Hub D3'
                    when base.hub_id = 20 then 'Hub Tan Binh A'
                    when base.hub_id = 21 then 'Hub Hoan Kiem'
                    when base.hub_id = 32 then 'Binh Thanh A'
                    when base.hub_id = 33 then 'Binh Thanh B'
                    when base.hub_id = 34 then 'Binh Thanh C'
                    else null end as hub_name

              ,case when base.pick_hub_id = 6 or base.district_name = 'District 4' then 'Hub D4'
                    when base.pick_hub_id = 7 or base.district_name = 'District 10' then 'Hub D10'
                    when base.pick_hub_id = 9 or base.district_name = 'District 5' then 'Hub D5'
                    when base.pick_hub_id = 10 then 'Hub Dong Da A'
                    when base.pick_hub_id = 11 then 'Hub Dong Da B'
                    when base.pick_hub_id = 19 then 'Hub D3'
                    when base.pick_hub_id = 20 then 'Hub Tan Binh A'
                    when base.pick_hub_id = 21 then 'Hub Hoan Kiem'
                    when base.pick_hub_id = 32 then 'Binh Thanh A'
                    when base.pick_hub_id = 33 then 'Binh Thanh B'
                    when base.pick_hub_id = 34 then 'Binh Thanh C'
                    else null end as pick_hub_name

              ,base.is_hub_driver
              ,base.shipper_rating
              ,base.shipping_fee
              ,base.cnt_timeout
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

                          ,case when dot.group_id > 0 then 1 else 0 end as is_stack
                          ,ogi.group_code
                          ,ogm.group_id

                          ,dot.is_asap
                          ,ogi.distance*1.0000/(100*1000) overall_distance
                          ,dot.delivery_distance*1.0000/1000 delivery_distance

                          ,case when dot.is_asap = 0 and dot.ref_order_status in (7,11) then date(from_unixtime(dot.real_drop_time - 3600)) else date(from_unixtime(dot.submitted_time- 3600)) end as report_date
                          ,date(from_unixtime(dot.submitted_time- 3600)) created_date
                          ,from_unixtime(dot.submitted_time- 3600) created_timestamp
                          ,case when cast(from_unixtime(dot.submitted_time - 3600) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
                                when cast(from_unixtime(dot.submitted_time - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                when cast(from_unixtime(dot.submitted_time - 3600) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                                else YEAR(cast(from_unixtime(dot.submitted_time - 3600) as date))*100 + WEEK(cast(from_unixtime(dot.submitted_time - 3600) as date)) end as created_year_week
                          ,concat(cast(YEAR(from_unixtime(dot.submitted_time - 3600)) as VARCHAR),'-',date_format(from_unixtime(dot.submitted_time - 3600),'%b')) as created_year_month

                          ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 3600) end as last_delivered_timestamp
                          ,case when dot.estimated_drop_time = 0 then null else from_unixtime(dot.estimated_drop_time - 3600) end as estimated_delivered_time
                          ,fa.first_auto_assign_timestamp
                          ,fa.last_incharge_timestamp

                          ,case when dot.is_asap = 0 then fa.first_auto_assign_timestamp else from_unixtime(dot.submitted_time- 3600) end as inflow_timestamp

                          ,order_rank.min_created_timestamp as group_stack_min_created_timestamp
                          ,order_rank.max_last_delivered_timestamp as group_stack_max_last_delivered_timestamp
                          ,COALESCE(order_rank.total_order_in_group,0) total_order_in_group

                          ,district.name_en as district_name
                          ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
                          ,case when dot.pick_city_id  = 217 then 'HCM'
                                when dot.pick_city_id  = 218 then 'HN'
                                when dot.pick_city_id  = 219 then 'DN'
                                when dot.pick_city_id  = 220 then 'HP'
                                when dot.pick_city_id  = 221 then 'CT'
                                when dot.pick_city_id  = 222 then 'DNAI'
                                when dot.pick_city_id  = 223 then 'VT'
                                when dot.pick_city_id  = 230 then 'BD'
                                when dot.pick_city_id  = 273 then 'HUE'else 'OTH'
                                end as city_group
                          ,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
                          ,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
                          ,case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver
                          ,COALESCE(srate.shipper_rating,5) shipper_rating
                          ,COALESCE(sf.shipping_fee,0) shipping_fee

                          ,fa.last_picked_timestamp
                          ,case when arrive.max_arrived_at_merchant_timestamp is not null then arrive.max_arrived_at_merchant_timestamp else fa.last_picked_timestamp  end as max_arrived_at_merchant_timestamp
                          ,case when arrive.max_arrived_at_buyer_timestamp is not null then arrive.max_arrived_at_buyer_timestamp
                                when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 3600) end as max_arrived_at_buyer_timestamp

                          ,coalesce(fa.cnt_timeout,0) cnt_timeout
                    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

                    left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
                    LEFT JOIN shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category

                    LEFT JOIN shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                                        and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                                        and ogm_filter.create_time >  ogm.create_time
                    LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id = ogm.group_id

                    LEFT JOIN

                            (SELECT order_id
                                   ,max(case when destination_key = 256 then from_unixtime(create_time - 3600) else null end) max_arrived_at_merchant_timestamp
                                   ,max(case when destination_key = 512 then from_unixtime(create_time - 3600) else null end) max_arrived_at_buyer_timestamp

                             FROM shopeefood.foody_partner_db__driver_order_arrive_log_tab__reg_daily_s0_live doal

                             WHERE 1=1
                             group by 1
                            )arrive on dot.id = arrive.order_id

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
                                  ,from_unixtime(cfo.create_time - 3600) as create_ts

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
                                      ,from_unixtime(dot.submitted_time - 3600) created_timestamp
                                      ,from_unixtime(dot.real_drop_time - 3600) last_delivered_timestamp
                                      ,from_unixtime(dot.real_pick_time - 3600) last_picked_timestamp
                                      ,dot.is_asap
                                      ,case when dot.is_asap = 0 and dot.ref_order_status in (7,11) then date(from_unixtime(dot.real_drop_time - 3600)) else date(from_unixtime(dot.submitted_time- 3600)) end as report_date
                                      ,date(from_unixtime(dot.submitted_time- 3600)) created_date

                                FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

                                WHERE 1=1
                                -- and grass_schema = 'foody_partner_db'
                                )dot

                                LEFT JOIN shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category

                                LEFT JOIN shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                                                    and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                                        and ogm_filter.create_time >  ogm.create_time
                                 LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id = ogm.group_id
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
                            ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                            ,count(case when status = 22 then order_id else null end) cnt_timeout
                            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                            where 1=1
                            -- and grass_schema = 'foody_order_db'
                            group by 1,2

                    UNION

                    SELECT   ns.order_id, ns.order_type ,min(from_unixtime(create_time - 3600)) first_auto_assign_timestamp
                            ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                            ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                            ,0 as cnt_timeout
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
                            where 1=1
                            and grass_region = 'VN'
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


                    LEFT JOIN
                            (
                             SELECT  sm.shipper_id
                                    ,sm.shipper_type_id
                                    ,case when sm.grass_date = 'current' then date(current_date)
                                        else cast(sm.grass_date as date) end as report_date

                                    from shopeefood.foody_mart__profile_shipper_master sm

                                    where 1=1
                                    and shipper_type_id <> 3
                                    and shipper_status_code = 1
                                    and grass_region = 'VN'
                                    GROUP BY 1,2,3
                            )driver_hub on driver_hub.shipper_id = dot.uid and driver_hub.report_date = case when dot.is_asap = 0 and dot.ref_order_status in (7,9,11) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
                                                                                                             when dot.is_asap = 1 and dot.ref_order_status in (7,9,11) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
                                                                                                                  else date(from_unixtime(dot.submitted_time- 3600)) end



                    WHERE 1=1
                    and ogm_filter.create_time is null
                    and dot.pick_city_id <> 238
                    )base

        WHERE 1=1
        and base.created_date >= date(current_date) - interval '30' day
        and base.created_date < date(current_date)
        and base.order_status_group = 'Completed'

        )base1

        LEFT JOIN

                (
                SELECT   ns.order_id, ns.order_type
                        ,count(ns.order_id) no_assign
                        ,count(case when status in (3,4) then order_id else null end) no_incharged
                        ,count(case when status in (8,9) then order_id else null end) no_ignored
                        ,count(case when order_type in (4,5,6,7) and status in (2,7,14,15) then order_id
                                    when order_type = 0 and status in (2,14,15) then order_id else null end) no_deny
                        ,count(case when status in (13) then order_id else null end) no_shipper_checkout
                        ,count(case when status in (16) then order_id else null end) no_incharge_error
                        ,count(case when status not in (3,4,7,8,9,2,13,14,15,16) then order_id else null end) no_other_assign_status


                FROM
                        ( SELECT order_id, order_type , create_time , assign_type, update_time, status

                         from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live

                         where 1=1 --order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                        -- and status in (3,4,7,8,9,2,13,14,15,16)

                         UNION

                         SELECT order_id, order_type, create_time , assign_type, update_time, status

                         from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                         where 1=1 --order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                      --   and status in (3,4,7,8,9,2,13,14,15,16)
                         )ns

                WHERE 1=1
                GROUP BY 1,2
                )assign on base1.order_id = assign.order_id and base1.order_type = assign.order_type

        LEFT JOIN

                (SELECT   oct.id
                        ,case when oct.status not in (8) then null
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Quán đã đóng cửa','Quán đóng cửa','Shop was closed','Shop is closed','Shop was no longer operating','Quán cúp diện','Driver reported Merchant closed','Tài xế báo Quán đóng cửa') then 'Shop closed'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('No driver','không có driver','Đơn hàng chưa có Tài xế nhận','No Drivers found','Không có tài xế nhận giao hàng','Lack of shipper') then 'No driver'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Out of stock of all order items','Out of Stock','Quan het mon','Hết tẩt cả món trong đơn hàng','Quán hết món','Merchant/Driver reported out of stock','Cửa hàng/ Tài xế báo hết món') then 'Out of stock'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Make another order again','Customer wanted to cancel order','Want to cancel') then 'Make another order'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) = 'I inputted the wrong information contact' then 'Customer put wrong contact info'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Shop was busy','Quán làm không kịp','Shop could not prepare in time','Cannot prepage') then 'Shop busy'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Cancelled_Napas','Cancelled_Credit Card','Cancelled_VNPay','Cancelled_vnpay','Cancelled_cybersource','Customer payment failed','Payment failed','Lỗi thanh toán','Payment is failed') then 'Payment failed'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) LIKE '%deliver on time%' then trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note')))
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Take too long to confirm order','Đơn hàng xác nhận quá lâu','Confirmed the order too late','Xác nhận đơn hàng chậm quá') then 'Confirmed the order too late'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Shop did not confirm order') then 'Shop did not confirm'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Too high item price/shipping fees','Giá món/ chi phí cao') then 'Think again on price and fees'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('I want to change item/Merchant','Tôi muốn đổi món/Quán') then 'Change item/Merchant'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('I want to change delivery time','Tôi muốn đổi giờ giao') then 'Change delivery time'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi muốn đổi thông tin liên hệ','I want to change phone number') then 'Change phone number'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi muốn đổi hình thức thanh toán','I want to change payment method') then 'Change payment method'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi muốn đổi thông tin liên hệ','I want to change phone number') then 'Change phone number'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi đặt trùng đơn','I made duplicate orders') then 'I made duplicate orders'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note')))in ('Tôi bận nên không thể nhận hàng','I am busy and cannot receive order','Có việc đột xuất') then 'I am busy and cannot receive order'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Ngân hàng chưa xác nhận thanh toán','Pending payment status from bank') then 'Pending status from bank'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Khách hàng chưa hoàn thành thanh toán','Incomplete payment process','User closed payment page')then 'Incomplete payment'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi quên nhập Mã Code khuyến mãi','I forgot inputting the discount code','I forgot inputting discount code') then 'Forgot inputting discount code'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Wrong price','Sai giá món') then 'Wrong price'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) = '' then 'Others'
                                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) is null then 'Others'
                                else 'Others' end as cancel_reason

                from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct

                left join shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int) -- note_ids: cancel_reason

                WHERE 1=1
                and date(from_unixtime(oct.submit_time - 3600)) >= date(current_date) - interval '30' day
                and date(from_unixtime(oct.submit_time - 3600)) < date(current_date)
                and oct.status = 8
                ) cancel on cancel.id = base1.order_id and base1.source = 'order_delivery'


)base2

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

)base3

WHERE 1=1
and source = 'NowFood'
GROUP BY 1,2,3,4,5
)base4