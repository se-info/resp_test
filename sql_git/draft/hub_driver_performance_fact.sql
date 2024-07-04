with hub AS
        (SELECT shipper_id
                ,min(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) first_day_in_hub
                ,max(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) last_day_in_hub
                 from shopeefood.foody_mart__profile_shipper_master

                 where 1=1
                 and shipper_type_id = 12
                 and grass_region = 'VN'
                 group by 1
        )
,arrive as
(SELECT order_id
        ,max(case when destination_key = 256 then from_unixtime(create_time - 3600) else null end) max_arrived_at_merchant_timestamp
        ,max(case when destination_key = 512 then from_unixtime(create_time - 3600) else null end) max_arrived_at_buyer_timestamp

    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_arrive_log_tab_vn_da where date(dt) = current_date - interval '1' day) doal

    WHERE 1=1

    group by 1
)
,srate as
(SELECT order_id
    ,shipper_uid as shipper_id
    ,case when cfo.shipper_rate = 0 then null
        when cfo.shipper_rate = 1 or cfo.shipper_rate = 101 then 1
        when cfo.shipper_rate = 102 then 2
        when cfo.shipper_rate = 2 or cfo.shipper_rate = 103 then 3
        when cfo.shipper_rate = 104 then 4
        when cfo.shipper_rate = 3 or cfo.shipper_rate = 105 then 5
        else null end as shipper_rating
    ,from_unixtime(cfo.create_time - 3600) as create_ts

FROM  shopeefood.foody_user_activity_db__customer_feedback_order_tab__reg_daily_s0_live cfo
)
,dot as 
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
)
,order_rank as 
(SELECT ogm.group_id
        ,ogi.group_code
        ,min(dot.created_timestamp) as min_created_timestamp
        ,min(dot.last_picked_timestamp) as min_last_picked_timestamp
        ,max(dot.last_delivered_timestamp) as max_last_delivered_timestamp
        ,count (distinct dot.ref_order_id) as total_order_in_group
    FROM dot
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                        and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                        and ogm_filter.create_time >  ogm.create_time
            LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
    WHERE 1=1
    and ogm.group_id is not null
and ogm_filter.create_time is null

GROUP BY 1,2
)
,fa as
(
SELECT   order_id , 0 as order_type
        ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
        ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
        ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
        from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live

        group by 1,2

UNION

SELECT   ns.order_id, ns.order_type ,min(from_unixtime(create_time - 3600)) first_auto_assign_timestamp
        ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
        ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
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
)
,driver_hub as
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
)
,eta as
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
    ,coalesce(cast(json_extract(eta.eta_data,'$.t_customer_wait.value') as INT),0) as t_customer_wait
    ,coalesce(cast(json_extract(eta.eta_data,'$.t_arrive_customer.value') as INT),0) as t_arrive_customer
    ,coalesce(cast(json_extract(eta.eta_data,'$.t_arrive_merchant.value') as INT),0) as t_arrive_merchant
    ,coalesce(cast(json_extract(eta.eta_data,'$.eta_range.max') as INT),0) as max_eta
    ,coalesce(cast(json_extract(eta.eta_data,'$.eta_range.min') as INT),0) as min_eta
    ,eta_data

    from shopeefood.data_mining_db__order_eta_data_tab__reg_daily_s0_live eta
)
,base as
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
            when dot.ref_order_category = 7 then 'now_ship_multi_drop'
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

        ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
            else date(from_unixtime(dot.submitted_time- 3600)) end as report_date
        ,date(from_unixtime(dot.submitted_time- 3600)) created_date
        ,from_unixtime(dot.submitted_time- 3600) created_timestamp
        ,CASE
            WHEN WEEK(DATE(from_unixtime(dot.submitted_time - 3600))) >= 52 AND MONTH(DATE(from_unixtime(dot.submitted_time - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(dot.submitted_time - 3600)))-1)*100 + WEEK(DATE(from_unixtime(dot.submitted_time - 3600)))
            WHEN WEEK(DATE(from_unixtime(dot.submitted_time - 3600))) = 1 AND MONTH(DATE(from_unixtime(dot.submitted_time - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(dot.submitted_time - 3600)))+1)*100 + WEEK(DATE(from_unixtime(dot.submitted_time - 3600)))
        ELSE YEAR(DATE(from_unixtime(dot.submitted_time - 3600)))*100 + WEEK(DATE(from_unixtime(dot.submitted_time - 3600))) END as created_year_week
        ,concat(cast(YEAR(from_unixtime(dot.submitted_time - 3600)) as VARCHAR),'-',date_format(from_unixtime(dot.submitted_time - 3600),'%b')) as created_year_month
        ,dot.submitted_time
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
        ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            when dot.pick_city_id = 220 then 'HP'
            ELSE 'OTH' end as city_group
        ,dot.pick_city_id as city_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
        ,case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver
        ,COALESCE(srate.shipper_rating,5) shipper_rating

        ,fa.last_picked_timestamp
        ,case when arrive.max_arrived_at_merchant_timestamp is not null then arrive.max_arrived_at_merchant_timestamp else fa.last_picked_timestamp  end as max_arrived_at_merchant_timestamp
        ,case when arrive.max_arrived_at_buyer_timestamp is not null then arrive.max_arrived_at_buyer_timestamp
            when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 3600) end as max_arrived_at_buyer_timestamp

        ,eta.t_pickup
        ,eta.t_customer_wait
        ,eta.t_arrive_customer
        ,eta.t_arrive_merchant
        ,eta.max_eta
        ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                    and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                    and ogm_filter.create_time >  ogm.create_time
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id

LEFT JOIN arrive on dot.id = arrive.order_id

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = dot.pick_district_id

LEFT JOIN srate ON dot.ref_order_id = srate.order_id and dot.uid = srate.shipper_id


LEFT JOIN order_rank on order_rank.group_id = ogm.group_id

LEFT JOIN fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type

LEFT JOIN driver_hub on driver_hub.shipper_id = dot.uid and driver_hub.report_date = case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
                                                                                            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
                                                                                            else date(from_unixtime(dot.submitted_time- 3600)) end

LEFT JOIN eta on eta.order_id =  dot.ref_order_id and dot.ref_order_category = 0


WHERE 1=1
and ogm_filter.create_time is null
and dot.pick_city_id <> 238
and case
        when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
        when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
    else date(from_unixtime(dot.submitted_time- 3600)) end BETWEEN current_date - interval '90' day AND current_date - interval '1' day
and dot.order_status in (400,401,402,403,404,405,406,407) -- Completed
)
,base1 as 
(
SELECT base.shipper_id
        ,base.report_date
        ,concat(base.source,'_',cast(base.order_id as varchar)) as uid
        ,base.ref_order_category order_type
        ,base.source

        ,case when base.order_status = 400 then 'Delivered'
            when base.order_status = 401 then 'Quit'
            when base.order_status in (402,403,404) then 'Cancelled'
            when base.order_status in (405,406,407) then 'Others'
            else 'Others' end as order_status

        ,base.is_stack is_stack_order
        ,base.is_asap
        ,base.delivery_distance distance
        ,base.group_stack_min_created_timestamp
        ,base.group_stack_max_last_delivered_timestamp

        ,case when base.first_auto_assign_timestamp < base.last_incharge_timestamp then 1 else 0 end as is_valid_incharge
        ,case when base.created_timestamp <= base.last_delivered_timestamp then 1 else 0 end as is_valid_submit_to_del

        ,date_diff('second',base.first_auto_assign_timestamp,base.last_incharge_timestamp)*1.0000 as lt_incharge
        ,date_diff('second',base.created_timestamp,base.last_delivered_timestamp) as lt_completion
        ,date_diff('second',base.created_timestamp,base.estimated_delivered_time) as lt_eta
        ,date_diff('second',base.created_timestamp,(case when base.max_eta = 0 then base.estimated_delivered_time else from_unixtime(base.submitted_time + base.max_eta - 3600) end)) as lt_eta_max

        ,case when base.last_delivered_timestamp > base.estimated_delivered_time then 1 else 0 end as is_late_delivered_time
        ,case when base.max_arrived_at_buyer_timestamp > base.estimated_delivered_time then 1 else 0 end as is_late_arrive_buyer
        ,date_diff('second',base.estimated_delivered_time,base.last_delivered_timestamp) lt_from_promise_to_actual_delivered

        ,date_diff('second',base.first_auto_assign_timestamp,base.max_arrived_at_merchant_timestamp)*1.0000 as lt_assign_to_arrive_at_merchant
        ,date_diff('second',base.last_incharge_timestamp,base.max_arrived_at_merchant_timestamp)*1.0000 as lt_incharge_to_arrive_at_merchant
        ,date_diff('second',base.last_picked_timestamp ,base.max_arrived_at_buyer_timestamp)*1.0000 as lt_pick_to_arrive_at_buyer
        ,date_diff('second',base.max_arrived_at_buyer_timestamp ,base.last_delivered_timestamp)*1.0000 as lt_arrive_at_buyer_to_del

        ,case when base.first_auto_assign_timestamp <= base.max_arrived_at_merchant_timestamp then 1 else 0 end as is_valid_lt_arrive_at_merchant
        ,case when base.last_incharge_timestamp <= base.max_arrived_at_merchant_timestamp then 1 else 0 end as is_valid_lt_incharge_to_arrive_merchant
        ,case when base.last_picked_timestamp <= base.max_arrived_at_buyer_timestamp then 1 else 0 end as is_valid_lt_arrive_at_buyer
        ,case when base.max_arrived_at_buyer_timestamp <= base.last_delivered_timestamp then 1 else 0 end as is_valid_lt_arrive_at_buyer_to_del

        ,base.hub_id
        ,base.is_hub_driver

        ,case when base.report_date between date('2021-07-09') and date('2021-10-05') and is_hub_driver = 1 and base.city_id = 217 then 1
                when base.report_date between date('2021-07-24') and date('2021-10-04') and is_hub_driver = 1 and base.city_id = 218 then 1
                when base.driver_payment_policy = 2 then 1 else 0 end as is_order_in_hub_shift
        ,base.shipper_rating
        ,base.t_pickup
        ,base.t_customer_wait
        ,base.t_arrive_customer
        ,base.t_arrive_merchant
FROM base
)
,base2 as 
(
SELECT
            base1.uid
        ,case when base1.source = 'order_delivery' then 'NowFood' else 'NowShip' end as source
        ,base1.shipper_id
        ,base1.report_date
        ,base1.order_status
        ,base1.is_asap
        ,base1.is_stack_order
        ,case when base1.is_stack_order = 0 then base1.lt_completion
                when base1.is_stack_order = 1 then date_diff('second',base1.group_stack_min_created_timestamp,base1.group_stack_max_last_delivered_timestamp)*1.0000/2
                else base1.lt_completion end as lt_completion_adjusted
        ,base1.lt_completion lt_completion
        ,base1.lt_eta
        ,base1.lt_eta_max
        ,base1.lt_incharge
        ,base1.is_late_delivered_time
        ,base1.is_late_arrive_buyer
        ,base1.lt_assign_to_arrive_at_merchant
        ,base1.lt_incharge_to_arrive_at_merchant
        ,base1.lt_pick_to_arrive_at_buyer
        ,base1.lt_arrive_at_buyer_to_del
        ,case when base1.lt_incharge_to_arrive_at_merchant <= base1.t_arrive_merchant then 1 else 0 end as is_arrive_merchant_on_time
        ,case when base1.lt_pick_to_arrive_at_buyer <= base1.t_arrive_customer then 1 else 0 end as is_arrive_buyer_on_time
        ,base1.distance
        ,case when base1.distance <= 3 then '1. 0-3km'
                when base1.distance <= 4 then '2. 3-4km'
                when base1.distance <= 5 then '3. 4-5km'
                when base1.distance > 5 then '5. 5km+'
                else null end as distance_range
        ,base1.is_hub_driver
        ,base1.is_order_in_hub_shift
        ,base1.shipper_rating
        ,base1.is_valid_incharge
        ,base1.is_valid_submit_to_del
        ,base1.is_valid_lt_arrive_at_merchant
        ,base1.is_valid_lt_incharge_to_arrive_merchant
        ,base1.is_valid_lt_arrive_at_buyer
        ,base1.is_valid_lt_arrive_at_buyer_to_del
        ,case when order_status = 'Delivered' then 1 else 0 end as is_del
        ,case when order_status = 'Cancelled'  then 1 else 0 end as is_cancel
        ,case when order_status = 'Quit' then 1 else 0 end as is_quit
        ,case when order_status = 'Returned' then 1 else 0 end as is_return
        ,case when source = 'order_delivery' then 1 else 0 end as is_now_food

FROM base1
)

,base3 as
(
    SELECT base2.shipper_id
    ,base2.report_date

    -- in hub shift
    --overall
    ,count(distinct case when base2.is_order_in_hub_shift = 1 then base2.uid else null end) oct_cnt_total_order_in_shift
    ,count(distinct case when base2.is_del = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_delivered_order_in_shift
    ,count(distinct case when base2.is_cancel = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_cancelled_order_in_shift
    ,count(distinct case when base2.is_quit = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_quit_order_in_shift
    ,count(distinct case when (base2.is_del = 1 and base2.is_late_delivered_time = 1 and base2.is_order_in_hub_shift = 1) then base2.uid else null end) oct_cnt_breach_order_in_shift

    --food
    ,count(distinct case when base2.is_now_food = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end) oct_cnt_total_order_in_shift_food
    ,count(distinct case when base2.is_now_food = 1 and base2.is_del = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_delivered_order_in_shift_food
    ,count(distinct case when base2.is_now_food = 1 and base2.is_cancel = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_cancelled_order_in_shift_food
    ,count(distinct case when base2.is_now_food = 1 and base2.is_quit = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_quit_order_in_shift_food
    ,count(distinct case when base2.is_now_food = 1 and base2.is_del = 1 and base2.is_late_delivered_time = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end) oct_cnt_breach_order_in_shift_food
    ,sum(case when base2.is_now_food = 1 and base2.is_del = 1 and base2.is_order_in_hub_shift = 1 then base2.shipper_rating else 0 end) oct_sum_shipper_rating_in_shift_food
    ,sum(case when base2.is_now_food = 1 and base2.is_del = 1 and base2.is_order_in_hub_shift = 1 then base2.distance else 0 end) oct_sum_distance_in_shift_food

    --ns
    ,count(distinct case when base2.is_now_food = 0 and base2.is_order_in_hub_shift = 1 then base2.uid else null end) oct_cnt_total_order_in_shift_ns
    ,count(distinct case when base2.is_now_food = 0 and base2.is_del = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_delivered_order_in_shift_ns
    ,count(distinct case when base2.is_now_food = 0 and base2.is_cancel = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_cancelled_order_in_shift_ns
    ,count(distinct case when base2.is_now_food = 0 and base2.is_return = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_returned_order_in_shift_ns
    ,count(distinct case when base2.is_now_food = 0 and base2.is_del = 1 and base2.is_late_delivered_time = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end) oct_cnt_breach_order_in_shift_ns

    -- completion time
    ,count(distinct case when base2.is_now_food = 1 and base2.is_del = 1 and base2.is_order_in_hub_shift = 1 and base2.is_asap = 1 and base2.is_valid_submit_to_del = 1 then base2.uid else null end ) cnt_delivered_order_in_shift_asap_food
    ,sum(case when base2.is_now_food = 1 and base2.is_del = 1 and base2.is_order_in_hub_shift = 1 and base2.is_asap = 1 and base2.is_valid_submit_to_del = 1 then base2.lt_completion else 0 end)*1.000000/60 total_lt_completion_food
    -- incharge time
    ,sum(case when base2.is_now_food = 1 and base2.is_del = 1 and base2.is_order_in_hub_shift = 1 and base2.is_valid_submit_to_del = 1 then base2.lt_incharge else 0 end)*1.000000/60 total_lt_incharge_food

    -- overall
    ,count(distinct base2.uid ) cnt_total_order_all
    ,count(distinct case when base2.is_del = 1 then base2.uid else null end ) cnt_delivered_order_all
    ,count(distinct case when base2.is_cancel = 1 then base2.uid else null end ) cnt_cancelled_order_all
    ,count(distinct case when base2.is_quit = 1 then base2.uid else null end ) cnt_quit_order_all
    ,count(distinct case when base2.is_return = 1 then base2.uid else null end ) cnt_return_order_all
    ,count(distinct case when base2.is_del = 1 and base2.is_late_delivered_time = 1 then base2.uid else null end) cnt_breach_order_all

    ,count(distinct case when base2.is_now_food = 1 then base2.uid else null end) cnt_total_order_food_all
    ,count(distinct case when base2.is_now_food = 1 and base2.is_del = 1 then base2.uid else null end ) cnt_delivered_order_food_all
    ,count(distinct case when base2.is_now_food = 1 and base2.is_cancel = 1 then base2.uid else null end ) cnt_cancelled_order_food_all
    ,count(distinct case when base2.is_now_food = 1 and base2.is_quit = 1 then base2.uid else null end ) cnt_quit_order_food_all
    ,count(distinct case when base2.is_now_food = 1 and base2.is_del = 1 and base2.is_late_delivered_time = 1 then base2.uid else null end) cnt_breach_order_food_all
    ,sum(case when base2.is_now_food = 1 and base2.is_del = 1 then base2.shipper_rating else 0 end) sum_shipper_rating_food_all

    ,count(distinct case when base2.is_now_food = 0 then base2.uid else null end) cnt_total_order_ns_all
    ,count(distinct case when base2.is_now_food = 0 and base2.is_del = 1 then base2.uid else null end ) cnt_delivered_order_ns_all
    ,count(distinct case when base2.is_now_food = 0 and base2.is_cancel = 1 then base2.uid else null end ) cnt_cancelled_order_ns_all
    ,count(distinct case when base2.is_now_food = 0 and base2.is_del = 1 and base2.is_late_delivered_time = 1 then base2.uid else null end) cnt_breach_order_ns_all
    --
    -- ,count(distinct case when base2.order_status = 'Delivered' and base2.source = 'NowFood' and base2.is_arrive_merchant_on_time = 1 then base2.uid else null end ) cnt_total_order_delivered_food_arrive_merchant_ontime
    --  ,count(distinct case when base2.order_status = 'Delivered' and base2.source = 'NowFood' and base2.is_arrive_buyer_on_time = 1 then base2.uid else null end ) cnt_total_order_delivered_food_arrive_buyer_ontime

    FROM base2
    GROUP BY 1,2
)
,driver as
(
    SELECT  sm.shipper_id
        ,sm.city_name
        ,case when sm.city_name = 'HCM City' then 'HCM'
            when sm.city_name = 'Ha Noi City' then 'HN'
            when sm.city_name = 'Da Nang City' then 'DN'
            when sm.city_name = 'Hai Phong City' then 'HP'
            else 'OTH' end as city_group
        ,case when sm.grass_date = 'current' then date(current_date)
            else cast(sm.grass_date as date) end as report_date
        ,sm.shipper_name

        from shopeefood.foody_mart__profile_shipper_master sm

        where 1=1
        and grass_region = 'VN'
    --     GROUP BY 1,2,3,4,5
)

,shift AS
(
SELECT
    shipper_id
    ,shipper_type_id
    ,report_date
    ,shipper_shift_id
    ,cast(round(start_shift,0) as int) as start_shift
    ,cast(round(end_shift,0) as int) as end_shift
    ,off_weekdays
    ,registration_status
    ,if(report_date < DATE'2021-10-22' and off_date is null and registration_status is null, off_date_1, off_date) as off_date
FROM
    (
    SELECT  sm.shipper_id
            ,sm.shipper_type_id
            ,try_cast(sm.grass_date as date) as report_date
            ,sm.shipper_shift_id
            ,case
                when try_cast(sm.grass_date as date) < date'2021-10-22' then
                    case
                        when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                ) then if((ss2.end_time - ss2.start_time)*1.0000/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.0000/3600 < 10.00, (ss2.end_time - 28800)*1.0000/3600, ss2.start_time*1.0000/3600)
                        when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                ) then null
                        else if((ss1.end_time - ss1.start_time)*1.0000/3600 > 5.00 and (ss1.end_time - ss1.start_time)*1.0000/3600 < 10.00, (ss1.end_time - 28800)*1.0000/3600, ss1.start_time*1.0000/3600)
                    end
                else
                    if((ss2.end_time - ss2.start_time)*1.0000/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.0000/3600 < 10.00, (ss2.end_time - 28800)*1.0000/3600, ss2.start_time*1.0000/3600)
            end as start_shift


            ,case
                when try_cast(sm.grass_date as date) < date'2021-10-22' then
                    case
                        when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                ) then ss2.end_time*1.0000/3600
                        when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                ) then null
                        else ss1.end_time*1.0000/3600
                    end
                else
                    ss2.end_time*1.0000/3600
            end as end_shift

            ,case
                when ss2.registration_status = 1 then 'Registered'
                when ss2.registration_status = 2 then 'Off'
                when ss2.registration_status = 3 then 'Work'
            else
                case
                    when try_cast(sm.grass_date as date) < date'2021-10-22' then null
                else 'Off' end
            end as registration_status
            ,case
                when try_cast(sm.grass_date as date) < date'2021-10-22' then
                    case
                        when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                ) then if(ss2.registration_status = 2, case
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                    end, null)
                        when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                ) then case
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                    end
                        else ss1.off_weekdays
                    end
                else
                    if(ss2.end_time is not null, if(ss2.registration_status = 2, case
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                    end, null), case
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                    end)
            end as off_weekdays
            ,case
                when try_cast(sm.grass_date as date) < date'2021-10-22' then
                    case
                        when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                or
                                (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                ) then if(ss2.registration_status = 2, date_format(try_cast(sm.grass_date as date), '%a'), null)
                        when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                or
                                (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                ) then date_format(try_cast(sm.grass_date as date), '%a')
                        else null
                    end
                else
                    if(ss2.end_time is not null, if(ss2.registration_status = 2, date_format(try_cast(sm.grass_date as date), '%a'), null)
                    , date_format(try_cast(sm.grass_date as date), '%a'))
            end as off_date
            ,array_join(array_agg(cast(d_.cha_date as VARCHAR)),', ') as off_date_1

            from shopeefood.foody_mart__profile_shipper_master sm
            left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss1 on ss1.id = sm.shipper_shift_id and try_cast(sm.grass_date as date) < DATE'2021-10-22'
            left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ss2 on ss2.uid = sm.shipper_id and date(from_unixtime(ss2.date_ts-3600)) = try_cast(sm.grass_date as date)
            -- left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss3 on sm.shipper_shift_id = ss3.id
            left join
                        (SELECT
                                case when off_weekdays = '1' then 'Mon'
                                    when off_weekdays = '2' then 'Tue'
                                    when off_weekdays = '3' then 'Wed'
                                    when off_weekdays = '4' then 'Thu'
                                    when off_weekdays = '5' then 'Fri'
                                    when off_weekdays = '6' then 'Sat'
                                    when off_weekdays = '7' then 'Sun'
                                    else 'No off date' end as cha_date
                                ,off_weekdays as num_date

                        FROM shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live
                        WHERE 1=1
                        and off_weekdays in ('1','2','3','4','5','6','7')
                        GROUP BY 1,2
                        )d_ on regexp_like(ss1.off_weekdays,cast(d_.num_date  as varchar)) = true

            where 1=1
            and sm.grass_region = 'VN'
            and sm.grass_date != 'current'
            GROUP BY 1,2,3,4,5,6,7,8,9
    )
)
,driver_type as
(SELECT
        b.shipper_id
        ,b.shipper_type_id
        ,b.report_date
        ,b.shipper_shift_id
        ,cast(coalesce(b.start_shift,b.start_shift_previous,round(case when b.shipper_type_id = 12 then ss3.start_time*1.0000/3600 else null end,0)) as int) start_shift
        ,cast(coalesce(b.end_shift,b.end_shift_previous,round(case when b.shipper_type_id = 12 then ss3.end_time*1.0000/3600 else null end,0)) as int) as end_shift
        ,b.off_weekdays
        ,b.registration_status
        ,b.off_date
    FROM
        (SELECT
            b1.shipper_id
            ,b1.shipper_type_id
            ,b1.report_date
            ,b1.shipper_shift_id
            ,b1.start_shift
            ,b1.end_shift
            ,b1.off_weekdays
            ,b1.registration_status
            ,b1.off_date
            ,MAX_BY(if(b2.shipper_type_id = 12, b2.start_shift,null), b2.report_date) AS start_shift_previous
            ,MAX_BY(if(b2.shipper_type_id = 12, b2.end_shift,null), b2.report_date) AS end_shift_previous
        FROM shift b1
        LEFT JOIN shift b2 ON b1.report_date > b2.report_date AND b2.start_shift IS NOT NULL AND b1.shipper_type_id = 12 AND b1.shipper_id = b2.shipper_id
        GROUP BY 1,2,3,4,5,6,7,8,9) b
        LEFT JOIN shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss3 on b.shipper_shift_id = ss3.id
)
,assginment_base as
(
SELECT  a.order_uid
        ,a.order_id
        ,case when a.order_type = 0 then '1. Food/Market'
            when a.order_type = 4 then '2. NowShip Instant'
            when a.order_type = 5 then '3. NowShip Food Mex'
            when a.order_type = 6 then '4. NowShip Shopee'
            when a.order_type = 7 then '5. NowShip Same Day'
            when a.order_type = 8 then '6. NowShip Multi Drop'
            when a.order_type = 200 and ogi.ref_order_category = 0 then '1. Food/Market'
            when a.order_type = 200 and ogi.ref_order_category = 6 then '4. NowShip Shopee'
            when a.order_type = 200 and ogi.ref_order_category = 7 then '5. NowShip Same Day'
            else 'Others' end as order_source
        ,a.order_type
        ,case when a.order_type <> 200 then a.order_type else ogi.ref_order_category end as order_category
        ,case when a.order_type = 200 then 'Group Order' else 'Single Order' end as order_group_type
        ,a.city_id
        ,city.name_en as city_name
        ,case when a.city_id  = 217 then 'HCM'
            when a.city_id  = 218 then 'HN'
            when a.city_id  = 219 then 'DN'
            when a.city_id  = 220 then 'HP'
            else 'OTH'
            end as city_group

        ,case when a.assign_type = 1 then '1. Single Assign'
                when a.assign_type in (2,4) then '2. Multi Assign'
                when a.assign_type = 3 then '3. Well-Stack Assign'
                when a.assign_type = 5 then '4. Free Pick'
                when a.assign_type = 6 then '5. Manual'
                when a.assign_type in (7,8) then '6. New Stack Assign'
                else null end as assign_type
        ,from_unixtime(a.create_time - 3600) as assign_time
        ,date(from_unixtime(a.create_time - 3600)) as date_
        ,CASE
                WHEN WEEK(DATE(from_unixtime(a.create_time - 3600))) >= 52 AND MONTH(DATE(from_unixtime(a.create_time - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(a.create_time - 3600)))-1)*100 + WEEK(DATE(from_unixtime(a.create_time - 3600)))
                WHEN WEEK(DATE(from_unixtime(a.create_time - 3600))) = 1 AND MONTH(DATE(from_unixtime(a.create_time - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(a.create_time - 3600)))+1)*100 + WEEK(DATE(from_unixtime(a.create_time - 3600)))
            ELSE YEAR(DATE(from_unixtime(a.create_time - 3600)))*100 + WEEK(DATE(from_unixtime(a.create_time - 3600))) END as year_week
        ,a.status
        ,case when a.experiment_group in (3,4,7,8) then 1 else 0 end as is_auto_accepted
        ,a.shipper_id
        ,date_add('hour',shift.start_shift,cast(date(from_unixtime(a.create_time - 3600)) as TIMESTAMP)) as start_shift_time
        ,date_add('hour',shift.end_shift,cast(date(from_unixtime(a.create_time - 3600)) as TIMESTAMP)) as end_shift_time
        ,case when dod.deny_type = 1 then 1 else 0 end as is_deny_driver_fault

FROM (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group,shipper_uid as shipper_id

        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        where status in (2,3,4,8,9,14,15) -- shipper incharge + deny + ignore

        UNION

        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group,shipper_uid as shipper_id

        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
        where status in (2,3,4,8,9,14,15) -- shipper incharge + deny + ignore
    )a

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = a.city_id and city.country_id = 86

left join
        (
        SELECT  sm.shipper_id
                ,sm.shipper_type_id
                ,try_cast(sm.grass_date as date) as report_date
                ,sm.shipper_shift_id
                ,case
                    when try_cast(sm.grass_date as date) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                    (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                    (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                    (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                    ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                    ) then if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                    (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                    (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                    (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                    ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                    ) then null
                            else if((ss1.end_time - ss1.start_time)*1.00/3600 > 5.00 and (ss1.end_time - ss1.start_time)*1.00/3600 < 10.00, (ss1.end_time - 28800)/3600, ss1.start_time/3600)
                        end
                    else
                        if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                end as start_shift
                ,case
                    when try_cast(sm.grass_date as date) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                    (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                    (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                    (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                    ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                    ) then ss2.end_time/3600
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                    (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                    (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                    (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                    ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                    ) then null
                            else ss1.end_time/3600
                        end
                    else
                        ss2.end_time/3600
                end as end_shift
                ,case
                    when ss2.registration_status = 1 then 'Registered'
                    when ss2.registration_status = 2 then 'Off'
                    when ss2.registration_status = 3 then 'Work'
                else
                    case
                        when try_cast(sm.grass_date as date) < date'2021-10-22' then null
                    else 'Off' end
                end as registration_status

                from shopeefood.foody_mart__profile_shipper_master sm
                left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss1 on ss1.id = sm.shipper_shift_id and try_cast(sm.grass_date as date) < date'2021-10-22'
                left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ss2 on ss2.uid = sm.shipper_id and date(from_unixtime(ss2.date_ts-3600)) = try_cast(sm.grass_date as date)


                where 1=1
                and sm.grass_region = 'VN'
                and sm.grass_date != 'current'
        )shift on shift.shipper_id = a.shipper_id and shift.report_date = date(from_unixtime(a.create_time - 3600))

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

LEFT JOIN   (SELECT dod.*, dot.ref_order_id, dot.ref_order_category,dot.group_id


                FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod
                LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dod.order_id = dot.id
            ) dod on a.order_id = (case when dod.ref_order_category <> 7 then dod.ref_order_id
                                            when dod.ref_order_category = 7 and dod.group_id = 0 then dod.ref_order_id
                                            else dod.group_id end) and a.order_type = dod.ref_order_category and a.shipper_id = dod.uid and a.status in (2,14,15)


WHERE 1=1
AND date(from_unixtime(a.create_time - 3600)) between current_date - interval '90' day and current_date - interval '1' day
)
,assign as
(
SELECT base.date_
        ,base.year_week
        ,base.shipper_id
        ,count(distinct base.order_uid) cnt_total_assign_order
        ,count(distinct case when base.assign_type <> '6. New Stack Assign' then base.order_uid else null end) cnt_total_assign_order_excl_stack
        ,count(distinct case when base.status in (3,4) then base.order_uid else null end) as cnt_total_incharge
        ,count(distinct case when base.is_auto_accepted = 1 then base.order_uid else null end ) as cnt_auto_accept_order
        ,count(distinct case when base.status in (2,14,15) then base.order_uid else null end) as cnt_deny_total
        ,count(distinct case when base.status in (2,14,15) and is_deny_driver_fault = 1 then base.order_uid else null end) as cnt_deny_non_acceptable
        ,count(distinct case when base.status in (2,14,15) and is_deny_driver_fault = 0 then base.order_uid else null end) as cnt_deny_acceptable
        ,count(distinct case when base.status in (8,9) then base.order_uid else null end) as cnt_ignore_total

        ,count(distinct case when (assign_time between start_shift_time and end_shift_time) then base.order_uid else null end) cnt_total_assign_order_in_shift
        ,count(distinct case when base.assign_type <> '6. New Stack Assign' and (assign_time between start_shift_time and end_shift_time) then base.order_uid else null end) cnt_total_assign_order_excl_stack_in_shift
        ,count(distinct case when base.status in (3,4) and (assign_time between start_shift_time and end_shift_time) then base.order_uid else null end) as cnt_total_incharge_in_shift
        ,count(distinct case when base.is_auto_accepted = 1 and (assign_time between start_shift_time and end_shift_time) then base.order_uid else null end ) as cnt_auto_accept_order_in_shift
        ,count(distinct case when base.status in (2,14,15) and (assign_time between start_shift_time and end_shift_time) then base.order_uid else null end) as cnt_deny_total_in_shift
        ,count(distinct case when base.status in (2,14,15) and is_deny_driver_fault = 1 and (assign_time between start_shift_time and end_shift_time) then base.order_uid else null end) as cnt_deny_non_acceptable_in_shift
        ,count(distinct case when base.status in (2,14,15) and is_deny_driver_fault = 0 and (assign_time between start_shift_time and end_shift_time)  then base.order_uid else null end) as cnt_deny_acceptable_in_shift
        ,count(distinct case when base.status in (8,9) and (assign_time between start_shift_time and end_shift_time)  then base.order_uid else null end) as cnt_ignore_total_in_shift

        ,count(distinct case when (assign_time < start_shift_time) then base.order_uid else null end) cnt_total_assign_order_before_shift
        ,count(distinct case when base.assign_type <> '6. New Stack Assign' and (assign_time < start_shift_time) then base.order_uid else null end) cnt_total_assign_order_excl_stack_before_shift
        ,count(distinct case when base.status in (3,4) and (assign_time < start_shift_time) then base.order_uid else null end) as cnt_total_incharge_before_shift
        ,count(distinct case when base.is_auto_accepted = 1 and (assign_time < start_shift_time) then base.order_uid else null end ) as cnt_auto_accept_order_before_shift
        ,count(distinct case when base.status in (2,14,15) and (assign_time < start_shift_time) then base.order_uid else null end) as cnt_deny_total_before_shift
        ,count(distinct case when base.status in (2,14,15) and is_deny_driver_fault = 1 and (assign_time < start_shift_time) then base.order_uid else null end) as cnt_deny_non_acceptable_before_shift
        ,count(distinct case when base.status in (2,14,15) and is_deny_driver_fault = 0 and (assign_time < start_shift_time) then base.order_uid else null end) as cnt_deny_acceptable_before_shift
        ,count(distinct case when base.status in (8,9) and (assign_time < start_shift_time) then base.order_uid else null end) as cnt_ignore_total_before_shift

        ,count(distinct case when (assign_time > end_shift_time) then base.order_uid else null end) cnt_total_assign_order_after_shift
        ,count(distinct case when base.assign_type <> '6. New Stack Assign' and (assign_time > end_shift_time) then base.order_uid else null end) cnt_total_assign_order_excl_stack_after_shift
        ,count(distinct case when base.status in (3,4) and (assign_time > end_shift_time) then base.order_uid else null end) as cnt_total_incharge_after_shift
        ,count(distinct case when base.is_auto_accepted = 1 and (assign_time > end_shift_time) then base.order_uid else null end ) as cnt_auto_accept_order_after_shift
        ,count(distinct case when base.status in (2,14,15) and (assign_time > end_shift_time) then base.order_uid else null end) as cnt_deny_total_after_shift
        ,count(distinct case when base.status in (2,14,15) and is_deny_driver_fault = 1 and (assign_time > end_shift_time) then base.order_uid else null end) as cnt_deny_non_acceptable_after_shift
        ,count(distinct case when base.status in (2,14,15) and is_deny_driver_fault = 0 and (assign_time > end_shift_time) then base.order_uid else null end) as cnt_deny_acceptable_after_shift
        ,count(distinct case when base.status in (8,9) and (assign_time > end_shift_time) then base.order_uid else null end) as cnt_ignore_total_after_shift
FROM assginment_base as base
WHERE 1=1
GROUP BY 1,2,3
)
,bonus as
(
SELECT cast(from_unixtime(bonus.report_date - 3600) as date) as report_date
    ,bonus.uid as shipper_id
    ,driver_type.shipper_type_id

    ,case when driver_type.shipper_type_id = 12 then 'Hub'
        when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
        when bonus.tier in (2,7,12) then 'T2'
        when bonus.tier in (3,8,13) then 'T3'
        when bonus.tier in (4,9,14) then 'T4'
        when bonus.tier in (5,10,15) then 'T5'
        else null end as current_driver_tier

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
left join
            (SELECT base.shipper_id
                ,base.report_date
                ,base.shipper_type_id

                From
                (SELECT shipper_id
                    ,city_name
                    ,case when grass_date = 'current' then date(current_date)
                        else cast(grass_date as date) end as report_date
                    ,shipper_type_id

                    from shopeefood.foody_mart__profile_shipper_master

                    where 1=1
                    -- and (grass_date = 'current' OR cast(grass_date as date) >= date(current_date) - interval '60' day)
                    and shipper_type_id <> 3
                    and shipper_status_code = 1
                    and shipper_type_id = 12 -- hub driver
                    and grass_region = 'VN'
                )base
                GROUP BY 1,2,3

            )driver_type on driver_type.shipper_id = bonus.uid and driver_type.report_date =  cast(from_unixtime(bonus.report_date - 3600) as date)

)        
,base4 as
(
SELECT base3.shipper_id
      ,base3.report_date
      ,CASE
          WHEN WEEK(base3.report_date) >= 52 AND MONTH(base3.report_date) = 1 THEN (YEAR(base3.report_date)-1)*100 + WEEK(base3.report_date)
          WHEN WEEK(base3.report_date) = 1 AND MONTH(base3.report_date) = 12 THEN (YEAR(base3.report_date)+1)*100 + WEEK(base3.report_date)
      ELSE YEAR(base3.report_date)*100 + WEEK(base3.report_date) END as report_week
      ,cast(date_format(cast(base3.report_date as TIMESTAMP),'%a') as varchar) days_of_week
      ,case when driver_type.shipper_type_id = 1 then 'full-time'
            when driver_type.shipper_type_id = 3 then 'tester'
            when driver_type.shipper_type_id = 11 then 'part-time'
            when driver_type.shipper_type_id = 12 then 'hub'
            else 'part-time' end as current_shipper_type
      ,driver.shipper_name
      ,driver.city_name
      ,driver.city_group
      ,case when driver.city_group in ('HCM', 'HN') then COALESCE(bonus.current_driver_tier,'full-time')
            else 'part-time' end as  current_driver_tier
      ,driver_type.start_shift
      ,driver_type.end_shift
      ,driver_type.off_date

      -- overall
      ,base3.cnt_total_order_all
      ,base3.cnt_delivered_order_all
      ,base3.cnt_cancelled_order_all
      ,base3.cnt_quit_order_all
      ,base3.cnt_return_order_all
      ,base3.cnt_breach_order_all

      ,base3.cnt_total_order_food_all
      ,base3.cnt_delivered_order_food_all
      ,base3.cnt_cancelled_order_food_all
      ,base3.cnt_quit_order_food_all
      ,base3.cnt_breach_order_food_all
      ,base3.sum_shipper_rating_food_all

      ,base3.cnt_total_order_ns_all
      ,base3.cnt_delivered_order_ns_all
      ,base3.cnt_cancelled_order_ns_all
      ,base3.cnt_breach_order_ns_all

      --inshift
      ,base3.oct_cnt_total_order_in_shift
      ,base3.oct_cnt_delivered_order_in_shift
      ,base3.oct_cnt_cancelled_order_in_shift
      ,base3.oct_cnt_quit_order_in_shift
      ,base3.oct_cnt_breach_order_in_shift

      ,base3.oct_cnt_total_order_in_shift_food
      ,base3.oct_cnt_delivered_order_in_shift_food
      ,base3.oct_cnt_cancelled_order_in_shift_food
      ,base3.oct_cnt_quit_order_in_shift_food
      ,base3.oct_cnt_breach_order_in_shift_food
      ,base3.oct_sum_shipper_rating_in_shift_food
      ,base3.oct_sum_distance_in_shift_food
      ,base3.cnt_delivered_order_in_shift_asap_food

      ,base3.oct_cnt_total_order_in_shift_ns
      ,base3.oct_cnt_delivered_order_in_shift_ns
      ,base3.oct_cnt_cancelled_order_in_shift_ns
      ,base3.oct_cnt_returned_order_in_shift_ns
      ,base3.oct_cnt_breach_order_in_shift_ns

      --leadtime
      ,base3.total_lt_completion_food
      ,base3.total_lt_incharge_food


      --assign
      ,coalesce(assign.cnt_total_assign_order,0) cnt_total_assign_order
      ,coalesce(assign.cnt_total_assign_order_excl_stack,0) cnt_total_assign_order_excl_stack
      ,coalesce(assign.cnt_total_incharge,0) cnt_total_incharge
      ,coalesce(assign.cnt_auto_accept_order,0) cnt_auto_accept_order
      ,coalesce(assign.cnt_deny_total,0) cnt_deny_total
      ,coalesce(assign.cnt_deny_non_acceptable,0) cnt_deny_non_acceptable
      ,coalesce(assign.cnt_deny_acceptable,0) cnt_deny_acceptable
      ,coalesce(assign.cnt_ignore_total,0) cnt_ignore_total

      ,coalesce(assign.cnt_total_assign_order_in_shift,0) cnt_total_assign_order_in_shift
      ,coalesce(assign.cnt_total_assign_order_excl_stack_in_shift,0) cnt_total_assign_order_excl_stack_in_shift
      ,coalesce(assign.cnt_total_incharge_in_shift,0) cnt_total_incharge_in_shift
      ,coalesce(assign.cnt_auto_accept_order_in_shift,0) cnt_auto_accept_order_in_shift
      ,coalesce(assign.cnt_deny_total_in_shift,0) cnt_deny_total_in_shift
      ,coalesce(assign.cnt_deny_non_acceptable_in_shift,0) cnt_deny_non_acceptable_in_shift
      ,coalesce(assign.cnt_deny_acceptable_in_shift,0) cnt_deny_acceptable_in_shift
      ,coalesce(assign.cnt_ignore_total_in_shift,0) cnt_ignore_total_in_shift

      ,coalesce(assign.cnt_total_assign_order_before_shift,0) cnt_total_assign_order_before_shift
      ,coalesce(assign.cnt_total_assign_order_excl_stack_before_shift,0) cnt_total_assign_order_excl_stack_before_shift
      ,coalesce(assign.cnt_total_incharge_before_shift,0) cnt_total_incharge_before_shift
      ,coalesce(assign.cnt_auto_accept_order_before_shift,0) cnt_auto_accept_order_before_shift
      ,coalesce(assign.cnt_deny_total_before_shift,0) cnt_deny_total_before_shift
      ,coalesce(assign.cnt_deny_non_acceptable_before_shift,0) cnt_deny_non_acceptable_before_shift
      ,coalesce(assign.cnt_deny_acceptable_before_shift,0) cnt_deny_acceptable_before_shift
      ,coalesce(assign.cnt_ignore_total_before_shift,0) cnt_ignore_total_before_shift

      ,coalesce(assign.cnt_total_assign_order_after_shift,0) cnt_total_assign_order_after_shift
      ,coalesce(assign.cnt_total_assign_order_excl_stack_after_shift,0) cnt_total_assign_order_excl_stack_after_shift
      ,coalesce(assign.cnt_total_incharge_after_shift,0) cnt_total_incharge_after_shift
      ,coalesce(assign.cnt_auto_accept_order_after_shift,0) cnt_auto_accept_order_after_shift
      ,coalesce(assign.cnt_deny_total_after_shift,0) cnt_deny_total_after_shift
      ,coalesce(assign.cnt_deny_non_acceptable_after_shift,0) cnt_deny_non_acceptable_after_shift
      ,coalesce(assign.cnt_deny_acceptable_after_shift,0) cnt_deny_acceptable_after_shift
      ,coalesce(assign.cnt_ignore_total_after_shift,0) cnt_ignore_total_after_shift


---- driver - total order completed, cancel, quit overview (inshift and overall)
FROM base3


---- most recent driver city and driver name
LEFT JOIN driver on driver.shipper_id = base3.shipper_id and driver.report_date = date(current_date) - interval '1' day

---- driver shift, start shift & end shift time , off_date , driver type in each report_date
LEFT JOIN driver_type on driver_type.shipper_id = base3.shipper_id and driver_type.report_date = base3.report_date

----- driver assignment performance
LEFT JOIN assign on base3.report_date = assign.date_ and base3.shipper_id = assign.shipper_id

---- driver tier on each report_date
LEFT JOIN bonus on base3.report_date = bonus.report_date and base3.shipper_id = bonus.shipper_id
WHERE 1=1
)
,working_time_base as
(
    SELECT uid as shipper_id
        ,date(from_unixtime(create_time - 3600)) as create_date

        -- important timestamp
        ,from_unixtime(check_in_time - 3600) as check_in_time
        ,from_unixtime(check_out_time - 3600) as check_out_time
        ,from_unixtime(order_start_time - 3600) as order_start_time
        ,from_unixtime(order_end_time - 3600) as order_end_time

        -- for checking
        ,check_in_time as check_in_time_original
        ,check_out_time as check_out_time_original
        ,order_start_time as order_start_time_original
        ,order_end_time as order_end_time_original
        ------------------

        ,total_online_seconds*1.00/(3600) as total_online_hours
        ,(check_out_time - check_in_time)*1.00/(3600) as online
        ,total_work_seconds*1.00/(3600) as total_work_hours
        ,(order_end_time - order_start_time)*1.00/(3600) as work

        -- actual use
        ,from_unixtime(check_in_time - 3600) as actual_start_time_online
        ,greatest(from_unixtime(check_out_time - 3600),from_unixtime(order_end_time - 3600)) as actual_end_time_online
        ,from_unixtime(check_out_time - 3600) as original_end_time_online

        ,case when order_start_time = 0 then from_unixtime(check_in_time - 3600) else from_unixtime(order_start_time - 3600) end as actual_start_time_work
        ,case when order_end_time = 0 then from_unixtime(check_in_time - 3600) else from_unixtime(order_end_time - 3600) end as actual_end_time_work

        ,date_add('hour',shift.start_shift,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP)) as start_shift_time
        ,date_add('hour',shift.end_shift,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP)) as end_shift_time

        ,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP) + interval '11' hour as noon_peak_start
        ,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP) + interval '12' hour as noon_peak_end
        ,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP) + interval '18' hour as night_peak_start
        ,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP) + interval '19' hour as night_peak_end

        from shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live sts

        left join
                (
                SELECT  sm.shipper_id
                        ,sm.shipper_type_id
                        ,try_cast(sm.grass_date as date) as report_date
                        ,sm.shipper_shift_id
                        ,case
                            when try_cast(sm.grass_date as date) < date'2021-10-22' then
                                case
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                            or
                                            (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                            or
                                            (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                            or
                                            (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                            ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                            ) then if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                            or
                                            (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                            or
                                            (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                            or
                                            (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                            ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                            ) then null
                                    else if((ss1.end_time - ss1.start_time)*1.00/3600 > 5.00 and (ss1.end_time - ss1.start_time)*1.00/3600 < 10.00, (ss1.end_time - 28800)/3600, ss1.start_time/3600)
                                end
                            else
                                if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                        end as start_shift
                        ,case
                            when try_cast(sm.grass_date as date) < date'2021-10-22' then
                                case
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                            or
                                            (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                            or
                                            (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                            or
                                            (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                            ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                            ) then ss2.end_time/3600
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                            or
                                            (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                            or
                                            (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                            or
                                            (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                            ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                            ) then null
                                    else ss1.end_time/3600
                                end
                            else
                                ss2.end_time/3600
                        end as end_shift
                        ,case
                            when ss2.registration_status = 1 then 'Registered'
                            when ss2.registration_status = 2 then 'Off'
                            when ss2.registration_status = 3 then 'Work'
                        else
                            case
                                when try_cast(sm.grass_date as date) < date'2021-10-22' then null
                            else 'Off' end
                        end as registration_status

                        from shopeefood.foody_mart__profile_shipper_master sm
                        left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss1 on ss1.id = sm.shipper_shift_id and try_cast(sm.grass_date as date) < DATE'2021-10-22'
                        left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ss2 on ss2.uid = sm.shipper_id and date(from_unixtime(ss2.date_ts-3600)) = try_cast(sm.grass_date as date)


                        where 1=1
                        and sm.grass_region = 'VN'
                        and sm.grass_date != 'current'
                )shift on shift.shipper_id = sts.uid and shift.report_date = date(from_unixtime(create_time - 3600))
        where 1=1
        and date(from_unixtime(create_time - 3600)) between current_date - interval '90' day and current_date - interval '1' day -- date('2020-06-30')
        and check_in_time > 0
        and check_out_time > 0
        and check_out_time >= check_in_time
        and order_end_time >= order_start_time
        and ((order_start_time = 0 and order_end_time = 0)
            OR (order_start_time > 0 and order_end_time > 0 and order_start_time >= check_in_time and order_start_time <= check_out_time)
            )
        )
,working_time_base1 as
(SELECT base.shipper_id
        ,base.create_date

        -- total
        ,date_diff('second',base.actual_start_time_online,base.actual_end_time_online)*1.0000/(3600) as total_online_time
        ,date_diff('second',base.actual_start_time_work,base.actual_end_time_work)*1.0000/(3600) as total_working_time

        ,date_diff('second',base.actual_start_time_online,base.original_end_time_online)*1.0000/(3600) as total_online_time_original

        --- shift
        ,case when base.actual_end_time_online < base.start_shift_time then 0
            when base.actual_start_time_online > base.end_shift_time then 0
            else date_diff('second',   greatest(base.start_shift_time,base.actual_start_time_online)   ,   least(base.end_shift_time,base.actual_end_time_online)   )*1.0000/(3600)
            end as in_shift_online_time

        --- shift
        ,case when base.original_end_time_online < base.start_shift_time then 0
            when base.actual_start_time_online > base.end_shift_time then 0
            else date_diff('second',   greatest(base.start_shift_time,base.actual_start_time_online)   ,   least(base.end_shift_time,base.original_end_time_online)   )*1.0000/(3600)
            end as in_shift_online_time_original


        ,case when base.actual_end_time_work < base.start_shift_time then 0
            when base.actual_start_time_work > base.end_shift_time then 0
            else date_diff('second',   greatest(base.start_shift_time,base.actual_start_time_work)   ,   least(base.end_shift_time,base.actual_end_time_work)   )*1.0000/(3600)
            end as in_shift_work_time


        -- peak
        --- peak noon
        ,case when base.original_end_time_online < base.noon_peak_start then 0
            when base.actual_start_time_online > base.noon_peak_end then 0
            else date_diff('second',   greatest(base.noon_peak_start,base.actual_start_time_online)   ,   least(base.noon_peak_end,base.original_end_time_online)   )*1.0000/(3600)
            end as noon_peak_online_time

        ,case when base.actual_end_time_work < base.noon_peak_start then 0
            when base.actual_start_time_work > base.noon_peak_end then 0
            else date_diff('second',   greatest(base.noon_peak_start,base.actual_start_time_work)   ,   least(base.noon_peak_end,base.actual_end_time_work)   )*1.0000/(3600)
            end as noon_peak_work_time

        --- peak night
        ,case when base.original_end_time_online < base.night_peak_start then 0
            when base.actual_start_time_online > base.night_peak_end then 0
            else date_diff('second',   greatest(base.night_peak_start,base.actual_start_time_online)   ,   least(base.night_peak_end,base.original_end_time_online)   )*1.0000/(3600)
            end as night_peak_online_time

        ,case when base.actual_end_time_work < base.night_peak_start then 0
            when base.actual_start_time_work > base.night_peak_end then 0
            else date_diff('second',   greatest(base.night_peak_start,base.actual_start_time_work)   ,   least(base.night_peak_end,base.actual_end_time_work)   )*1.0000/(3600)
            end as night_peak_work_time

        from working_time_base as base
        )
        
,work_time as
(SELECT  base1.create_date
        ,base1.shipper_id

        -- total
        ,sum(base1.total_online_time) as total_online_time
        ,sum(base1.total_working_time) as total_working_time
        ,sum(base1.total_online_time_original) total_online_time_original

        -- in_shift
        ,sum(base1.in_shift_online_time) as in_shift_online_time
        ,sum(base1.in_shift_work_time) as in_shift_work_time
        ,sum(base1.in_shift_online_time_original) in_shift_online_time_original

        ,sum(coalesce(base1.noon_peak_online_time,0) + coalesce(base1.night_peak_online_time,0)) as peak_online_time
        ,sum(coalesce(base1.noon_peak_work_time,0) + coalesce(base1.night_peak_work_time,0)) as peak_work_time
        ,sum(coalesce(base1.noon_peak_online_time,0)) noon_peak_online_time
        ,sum(coalesce(base1.night_peak_online_time,0)) night_peak_online_time
from working_time_base1 as base1
group by 1,2
)
,bonus_tab as
(SELECT cast(from_unixtime(bonus.report_date - 3600) as date) as report_date
        ,bonus.uid as shipper_id
        ,case
            when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
            when bonus.tier in (2,7,12) then 'T2'
            when bonus.tier in (3,8,13) then 'T3'
            when bonus.tier in (4,9,14) then 'T4'
            when bonus.tier in (5,10,15) then 'T5'
            else null end as current_driver_tier

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

WHERE schema = 'foody_internal_db'

)
,hub_base1 as
(SELECT a.shipper_id
        ,a.first_day_in_hub
        ,CASE
            WHEN WEEK(first_day_in_hub) >= 52 AND MONTH(first_day_in_hub) = 1 THEN (YEAR(first_day_in_hub)-1)*100 + WEEK(first_day_in_hub)
            WHEN WEEK(first_day_in_hub) = 1 AND MONTH(first_day_in_hub) = 12 THEN (YEAR(first_day_in_hub)+1)*100 + WEEK(first_day_in_hub)
        ELSE YEAR(first_day_in_hub)*100 + WEEK(first_day_in_hub) END as week_join_hub

        ,coalesce(bonus.current_driver_tier,'full-time') as tier_before_hub
FROM hub as a

LEFT JOIN bonus_tab as bonus on a.shipper_id = bonus.shipper_id and bonus.report_date = a.first_day_in_hub - interval '1' day

)
,fresh as
(select cast(id as bigint) as shipper_id
        ,cast(date_onboard as date) as date_onboard
from vnfdbi_opsndrivers.foody_vn_bi_team__hub_fresh_driver_list_tab
)
,income as    
(SELECT uid
    ,date(from_unixtime(report_date)) date_
    ,cast(json_extract(extra_data, '$.total_shipping_shared') as double) total_shipping_shared
    ,cast(json_extract(extra_data,'$.calculated_shipping_shared') as double) calculated_shipping_shared
    ,cast(json_extract(extra_data, '$.total_order') as bigint) total_order_completed_in_shift
    ,case when cast(json_extract(extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then 1 else 0 end as is_compensated_min_fee
FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live
)
,base5 as
(
SELECT base4.*
      ,case when (regexp_like(off_date,base4.days_of_week) = false or oct_cnt_total_order_in_shift > 0) then 'Working Date' else 'Off Date' end as date_type
      ,coalesce(work_time.total_online_time,0) total_online_time
      ,coalesce(work_time.total_working_time,0) total_working_time
      ,coalesce(work_time.total_online_time_original,0) total_online_time_original
      ,case when base4.report_date between date('2021-07-09') and date('2021-10-05') and city_group = 'HCM' then coalesce(work_time.total_online_time,0)
            when base4.report_date between date('2021-07-24') and date('2021-10-04') and city_group = 'HN' then coalesce(work_time.total_online_time,0)
            else coalesce(work_time.in_shift_online_time,0) end in_shift_online_time
      ,case when base4.report_date between date('2021-07-09') and date('2021-10-05') and city_group = 'HCM' then coalesce(work_time.total_working_time,0)
            when base4.report_date between date('2021-07-24') and date('2021-10-04') and city_group = 'HN' then coalesce(work_time.total_working_time,0)
            else coalesce(work_time.in_shift_work_time,0) end in_shift_work_time
      ,case when base4.report_date between date('2021-07-09') and date('2021-10-05') and city_group = 'HCM' then coalesce(work_time.total_online_time_original,0)
            when base4.report_date between date('2021-07-24') and date('2021-10-04') and city_group = 'HN' then coalesce(work_time.total_online_time_original,0)
            else coalesce(work_time.in_shift_online_time_original,0) end in_shift_online_time_original
      ,coalesce(work_time.peak_online_time,0) peak_online_time
      ,coalesce(work_time.peak_work_time,0) peak_work_time

      ,case when base4.end_shift - base4.start_shift = 10 then coalesce(work_time.peak_online_time,0)
            when base4.end_shift - base4.start_shift = 8 then coalesce(work_time.peak_online_time,0)
            when base4.end_shift - base4.start_shift = 5 and base4.start_shift < 11 then coalesce(work_time.noon_peak_online_time,0)
            when base4.end_shift - base4.start_shift = 5 and base4.start_shift > 11 then coalesce(work_time.night_peak_online_time,0)
            else null end as in_shift_peak_online_time

      ,case when base4.end_shift - base4.start_shift = 10 then coalesce(work_time.noon_peak_online_time,0)
            when base4.end_shift - base4.start_shift = 8 then coalesce(work_time.noon_peak_online_time,0)
            when base4.end_shift - base4.start_shift = 5 and base4.start_shift < 11 then coalesce(work_time.noon_peak_online_time,0)
            else null end as in_shift_peak_noon_online_time

      ,case when base4.end_shift - base4.start_shift = 10 then coalesce(work_time.night_peak_online_time,0)
            when base4.end_shift - base4.start_shift = 8 then coalesce(work_time.night_peak_online_time,0)
            when base4.end_shift - base4.start_shift = 5 and base4.start_shift > 11 then coalesce(work_time.night_peak_online_time,0)
            else null end as in_shift_peak_night_online_time

      ,hub.first_day_in_hub
      ,case when hub.week_join_hub < (YEAR(base4.report_date)*100 + WEEK(base4.report_date)) - 1 then 'Current Driver'
            when hub.week_join_hub >= (YEAR(base4.report_date)*100 + WEEK(base4.report_date)) - 1 then 'New Driver'
            else null end as hub_seniority
      ,hub.tier_before_hub

      ,hub_dim.hub_type
      ,hub_dim.hub_type_v2

      ,case when fresh.shipper_id > 0 then 1 else 0 end as is_fresh_driver
      ,coalesce(income.total_shipping_shared,0) total_shipping_shared
      ,coalesce(income.is_compensated_min_fee,0) is_compensated_min_fee
      ,coalesce(income.total_order_completed_in_shift,0) total_order_completed_in_shift
FROM base4

-- driver working time, online time in shift and overall
LEFT JOIN work_time on work_time.shipper_id = base4.shipper_id and work_time.create_date = base4.report_date

-- driver tier before joining hub, driver join hub date
LEFT JOIN hub_base1 as hub on hub.shipper_id = base4.shipper_id

LEFT JOIN fresh on fresh.shipper_id = base4.shipper_id

-- driver shipping share in each report_date
LEFT JOIN income on income.uid = base4.shipper_id and income.date_ = base4.report_date

-- get hub_type_dim
LEFT JOIN vnfdbi_opsndrivers.ingest_hub_type_dim_tab AS hub_dim 
    on base4.end_shift = cast(hub_dim.end_shift as int) and base4.start_shift  = cast(hub_dim.start_shift as int) 
    and base4.report_date between cast (hub_dim.start_date as date)  and  coalesce(try_cast(hub_dim.end_date as date) ,current_date)

where 1=1
and base4.current_shipper_type  = 'hub'

)

SELECT 
        b.*
        ,case when date_type = 'Working Date' and greatest(oct_cnt_delivered_order_in_shift,total_order_completed_in_shift) < cast(config.min_order as int) then 1 else 0 end is_under_min
        ,case when date_type = 'Working Date' and greatest(oct_cnt_delivered_order_in_shift,total_order_completed_in_shift) = cast(config.min_order as int) then 1 else 0 end is_pass_min
        ,case when date_type = 'Working Date' and greatest(oct_cnt_delivered_order_in_shift,total_order_completed_in_shift) > cast(config.min_order as int) then 1 else 0 end is_over_min

from base5 b
left join vnfdbi_opsndrivers.ingest_hub_config_min_order config
    on lower(b.hub_type_v2) = lower(config.hub_type) and lower(b.city_group) = lower(config.city_group) and b.report_date between cast (config.start_date as date)  and  coalesce(try_cast(config.end_date as date) ,current_date)

-- limit 100
-- where shipper_id = 21722238 -- check shipper
/*
2022.03.23 
update: get start_shift and end_shift from master_shipper
2022.03.24 
update: mapping with config_min_order: https://docs.google.com/spreadsheets/d/1awl5JNXmGwtLKf62pJAa71zD0lmj77xwRMb8iQTNivc/edit#gid=0
update: mapping with ingest_hub_type_dim_tab
2022.03.29: update logic previous_shift: MAX_BY(if(b2.shipper_type_id = 12, b2.start_shift,null), b2.report_date)
*/