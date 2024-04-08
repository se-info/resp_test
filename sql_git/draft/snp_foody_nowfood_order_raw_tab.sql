SELECT
         base1.uid
        ,base1.shipper_id
        ,base1.order_id
        ,base1.order_code
        ,base1.order_type
        ,case when base1.source = 'order_delivery' then 'NowFood' else 'NowShip' end as source
        ,base1.group_id
        ,base1.group_code
        ,base1.order_create_time
        ,base1.group_create_time
        ,base1.total_order_in_group
        ,base1.total_order_in_group_original
        ,base1.created_date
        ,base1.created_year_week
        ,base1.created_year_month
        ,base1.inflow_date
        ,base1.report_date
        ,base1.created_hour
        ,base1.inflow_hour

        ,base1.city_group
        ,base1.city_name
        ,base1.district_name
        ,base1.city_id
        ,base1.district_id
        ,base1.order_status
        ,base1.is_asap
        ,base1.is_stack_order
        ,base1.is_group_order
        ,base1.inflow_timestamp
        ,base1.estimated_delivered_time
        ,base1.last_delivered_timestamp
        ,base1.last_picked_timestamp

        ,base1.lt_completion lt_completion_original
        ,base1.lt_eta
        ,base1.lt_eta_max
        ,base1.lt_sla
        ,base1.lt_incharge

        ,case when base1.lt_completion*1.000000/60 > base1.lt_sla then 1 else 0 end as is_late_sla
        ,base1.is_late_delivered_time
        ,base1.is_late_delivered_time_eta_max
        ,base1.is_late_arrive_buyer

        ,base1.distance
        ,case when base1.distance <= 3 then '1. 0-3km'
              when base1.distance <= 4 then '2. 3-4km'
              when base1.distance <= 5 then '3. 4-5km'
              when base1.distance > 5 then '5. 5km+'
              else null end as distance_range

        ,base1.is_hub_driver
        ,base1.is_order_in_hub_shift

        ,base1.is_valid_incharge as is_valid_lt_incharge
        ,base1.is_valid_submit_to_del
        ,base1.is_valid_submit_to_eta
        ,base1.is_valid_submit_to_eta_max

        ,base1.food_service
FROM
        (
        SELECT base.shipper_id
              ,base.city_name
              ,base.city_group
              ,base.city_id
              ,base.district_id
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
                    when base.order_status in (405) then 'Returned'
                    else 'Others' end as order_status

              ,base.order_status_group
              ,base.is_stack_order
              ,base.is_group_order
              ,base.group_code
              ,base.group_id
              ,base.order_create_time
              ,base.group_create_time
              ,base.total_order_in_group
              ,base.total_order_in_group_original
              ,base.first_auto_assign_timestamp
              ,base.last_delivered_timestamp
              ,base.estimated_delivered_time
              ,base.last_picked_timestamp

              ,base.inflow_timestamp

              ,EXTRACT(HOUR from base.created_timestamp) created_hour
              ,EXTRACT(HOUR from base.inflow_timestamp) inflow_hour


              ,base.is_asap
              ,base.delivery_distance distance

              ,case when base.first_auto_assign_timestamp < base.last_incharge_timestamp then 1 else 0 end as is_valid_incharge
              ,case when base.created_timestamp <= base.last_delivered_timestamp then 1 else 0 end as is_valid_submit_to_del
              ,case when base.created_timestamp <= base.estimated_delivered_time then 1 else 0 end as is_valid_submit_to_eta
              ,case when base.created_timestamp <= case when base.max_eta = 0 then base.estimated_delivered_time else from_unixtime(base.submitted_time + base.max_eta - 60*60) end then 1 else 0 end as is_valid_submit_to_eta_max

              ,date_diff('second',base.first_auto_assign_timestamp,base.last_incharge_timestamp)*1.0000/60 as lt_incharge
              ,date_diff('second',base.created_timestamp,base.last_delivered_timestamp)*1.0000/60 as lt_completion
              ,date_diff('second',base.created_timestamp,base.estimated_delivered_time)*1.0000/60 as lt_eta
              ,date_diff('second',base.created_timestamp,(case when base.max_eta = 0 then base.estimated_delivered_time else from_unixtime(base.submitted_time + base.max_eta - 60*60) end))*1.0000/60 as lt_eta_max
              ,case when base.delivery_distance <= 1 then 30
                    when base.delivery_distance > 1 then least(60,30 + 5*(ceiling(base.delivery_distance) -1))
                    else null end as lt_sla

              ,case when base.last_delivered_timestamp > (case when base.max_eta = 0 then base.estimated_delivered_time else from_unixtime(base.submitted_time + base.max_eta - 60*60) end) then 1 else 0 end as is_late_delivered_time_eta_max
              ,case when base.last_delivered_timestamp > base.estimated_delivered_time then 1 else 0 end as is_late_delivered_time
              ,case when base.max_arrived_at_buyer_timestamp > base.estimated_delivered_time then 1 else 0 end as is_late_arrive_buyer
              ,date_diff('second',base.estimated_delivered_time,base.last_delivered_timestamp) lt_from_promise_to_actual_delivered


              ,base.is_hub_driver
              ,case when base.report_date between date('2021-07-09') and date('2021-10-05') and base.city_id = 217 and is_hub_driver = 1 then 1
                    when base.report_date between date('2021-07-24') and date('2021-10-04') and base.city_id = 218 and is_hub_driver = 1 then 1
                    when base.driver_payment_policy = 2 then 1 else 0 end as is_order_in_hub_shift

              ,base.food_service
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

                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time = ogi.create_time then 1 else 0 end as is_group_order
                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 1
                                when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time != ogi.create_time then 1
                                else 0 end as is_stack_order
                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time = ogi.create_time then COALESCE(order_rank.total_order_in_group_original,0) else 0 end as total_order_in_group_original
                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then COALESCE(order_rank.total_order_in_group,0)
                                when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time != ogi.create_time then COALESCE(order_rank.total_order_in_group,0) - COALESCE(order_rank.total_order_in_group_original,0)
                                else 0 end as total_order_in_group
                          ,ogi.group_code
                          ,ogm.group_id
                          ,ogm.create_time as order_create_time
                          ,ogi.create_time as group_create_time
                          ,dot.is_asap
                          ,ogi.distance*1.0000/(100*1000) overall_distance
                          ,dot.delivery_distance*1.0000/1000 delivery_distance

                          ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                                when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                                else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
                          ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
                          ,from_unixtime(dot.submitted_time- 60*60) created_timestamp
                          ,dot.submitted_time
                          ,case when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
                                when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                                else YEAR(cast(from_unixtime(dot.submitted_time - 60*60) as date))*100 + WEEK(cast(from_unixtime(dot.submitted_time - 60*60) as date)) end as created_year_week
                          ,concat(cast(YEAR(from_unixtime(dot.submitted_time - 60*60)) as VARCHAR),'-',date_format(from_unixtime(dot.submitted_time - 60*60),'%b')) as created_year_month

                          ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
                          ,case when dot.estimated_drop_time = 0 then null else from_unixtime(dot.estimated_drop_time - 60*60) end as estimated_delivered_time
                          ,fa.first_auto_assign_timestamp
                          ,fa.last_incharge_timestamp
                          ,fa.last_picked_timestamp
                          ,case when arrive.max_arrived_at_merchant_timestamp is not null then arrive.max_arrived_at_merchant_timestamp else fa.last_picked_timestamp  end as max_arrived_at_merchant_timestamp
                          ,case when arrive.max_arrived_at_buyer_timestamp is not null then arrive.max_arrived_at_buyer_timestamp
                                when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as max_arrived_at_buyer_timestamp
                          ,coalesce(fa.first_auto_assign_timestamp,from_unixtime(dot.submitted_time- 60*60)) as inflow_timestamp

                          ,order_rank.min_created_timestamp as group_stack_min_created_timestamp
                          ,order_rank.max_last_delivered_timestamp as group_stack_max_last_delivered_timestamp

                          ,district.name_en as district_name
                          ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
                          ,case when dot.pick_city_id = 217 then 'HCM'
                                when dot.pick_city_id = 218 then 'HN'
                                when dot.pick_city_id = 219 then 'DN'
                                when dot.pick_city_id = 220 then 'HP'
                                ELSE 'OTH' end as city_group
                          ,dot.pick_city_id as city_id
                          ,dot.pick_district_id as district_id

                          ,case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver

                          ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy

                          -- eta max
                          ,eta.max_eta
                          ,case when COALESCE(oct.foody_service_id,0) = 1 then 'Food'
                                when COALESCE(oct.foody_service_id,0) in (5) then 'Fresh'
                                when COALESCE(oct.foody_service_id,0) > 0 then 'Market'
                                when dot.ref_order_category = 0 then 'Food - Others'
                                else 'Nowship' end as food_service

                    FROM (select * from shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live where grass_schema = 'foody_partner_db' and submitted_time > 1617408000) dot

                    left join shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet on dot.id = dotet.order_id
                    left join (select * from shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live where submit_time > 1617408000) oct on oct.id = dot.ref_order_id and dot.ref_order_category = 0
                    -- stack group_code
                    LEFT JOIN shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category

                    LEFT JOIN shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                                        and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                                        and ogm_filter.create_time >  ogm.create_time
                    LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id = ogm.group_id

                    ---arrive at buyer/merchant timestamp
                    LEFT JOIN

                            (SELECT order_id
                                   ,max(case when destination_key = 256 then from_unixtime(create_time - 60*60) else null end) max_arrived_at_merchant_timestamp
                                   ,max(case when destination_key = 512 then from_unixtime(create_time - 60*60) else null end) max_arrived_at_buyer_timestamp

                             FROM shopeefood.foody_partner_db__driver_order_arrive_log_tab__reg_daily_s0_live doal

                             WHERE 1=1
                             and grass_schema = 'foody_partner_db'
                             and create_time > 1617408000
                             group by 1
                            )arrive on dot.id = arrive.order_id

                    --- city, district of merchant
                    left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

                    Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = dot.pick_district_id

                    --- data on stack order
                    LEFT JOIN
                            (SELECT ogm.group_id
                                   ,ogi.group_code
                                   ,min(dot.created_timestamp) as min_created_timestamp
                			 	   ,min(dot.last_picked_timestamp) as min_last_picked_timestamp
                				   ,max(dot.last_delivered_timestamp) as max_last_delivered_timestamp
                                   ,count (distinct dot.ref_order_id) as total_order_in_group
                                   ,count (distinct case when ogi.create_time = ogm.create_time then dot.ref_order_id else null end) total_order_in_group_original
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

                                FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot

                                WHERE 1=1
                                and grass_schema = 'foody_partner_db'
                                and dot.submitted_time > 1617408000
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

                    --- timestamp of order
                    LEFT JOIN
                            (
                            SELECT   order_id , 0 as order_type
                                    ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                                    ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                                    ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                                    from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                                    where 1=1
                                    and grass_schema = 'foody_order_db'
                                    group by 1,2

                            UNION

                            SELECT   ns.order_id, ns.order_type ,min(from_unixtime(create_time - 60*60)) first_auto_assign_timestamp
                                    ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                                    ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                            FROM
                                    ( SELECT order_id, order_type , create_time , update_time, status

                                     from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                                     where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                                     and grass_schema = 'foody_partner_archive_db'
                                     UNION

                                     SELECT order_id, order_type, create_time , update_time, status

                                     from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                     where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                                     and schema = 'foody_partner_db'
                                     )ns
                            GROUP BY 1,2
                            )fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type

                    --- find whether driver is hub driver or not
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
                            )driver_hub on driver_hub.shipper_id = dot.uid and driver_hub.report_date = case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                                                                                                             when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                                                                                                             else date(from_unixtime(dot.submitted_time- 60*60)) end

                    --- eta for each stage
                    LEFT JOIN
                        (SELECT
                             eta.id
                            ,eta.order_id
                            ,from_unixtime(eta.create_time - 60*60) as create_time
                            ,from_unixtime(eta.update_time - 60*60) as update_time
                            ,coalesce(cast(json_extract(eta.eta_data,'$.t_assign.value') as INT),0) as t_assign
                            ,coalesce(cast(json_extract(eta.eta_data,'$.t_confirm.value') as INT),0) as t_confirm
                            ,coalesce(cast(json_extract(eta.eta_data,'$.t_pickup.value') as INT),0) as t_pickup
                            ,coalesce(cast(json_extract(eta.eta_data,'$.t_prep.value') as INT),0) as t_prep
                            ,coalesce(cast(json_extract(eta.eta_data,'$.t_dropoff.value') as INT),0) as t_dropoff
                            ,coalesce(cast(json_extract(eta.eta_data,'$.t_customer_wait.value') as INT),0) as t_customer_wait
                            ,coalesce(cast(json_extract(eta.eta_data,'$.t_arrive_customer.value') as INT),0) as t_arrive_customer
                            ,coalesce(cast(json_extract(eta.eta_data,'$.t_arrive_merchant.value') as INT),0) as t_arrive_merchant
                            ,coalesce(cast(json_extract(eta.eta_data,'$.eta_range.max') as INT),0) as max_eta
                            ,coalesce(cast(json_extract(eta.eta_data,'$.eta_range.min') as INT),0) as min_eta
                            ,eta_data

                            from shopeefood.data_mining_db__order_eta_data_tab__reg_daily_s0_live eta
                            where eta.create_time > 1617408000
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

                               LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

                            where 1=1
                            and a_filter.order_id is null -- take last incharge
                            -- and a.order_id = 9490679
                            and a.order_type = 200

                            GROUP BY 1,2,3,4,5,6,7,8

                            )group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category


                    WHERE 1=1
                    and ogm_filter.create_time is null
                    and dot.pick_city_id not in (238,469)
                    and dot.grass_schema = 'foody_partner_db'
                    and dot.ref_order_category = 0
                    and dot.ref_order_status = 7
                    and date(from_unixtime(dot.submitted_time- 60*60)) between date(current_date) - interval '35' day and date(current_date) - interval '1' day
                    and dot.order_status in (400,401,402,403,404,405,406,407)
                    )base


        WHERE 1=1
        )base1
