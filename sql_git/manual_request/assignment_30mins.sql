with free_pick as 
(
SELECT base.order_id 
      ,base.order_type
      ,base.order_code
      ,base.city_name
      ,base.city_group
      ,base.assign_type
      ,create_time
      ,update_time
      ,create_hour
      ,minute_range
      ,date_
      ,year_week
      ,shipper_id

from
(SELECT a.order_uid
,a.order_id
,case when a.order_type = 0 then '1. Food/Market'
    when a.order_type in (4,5) then '2. NS'
    when a.order_type = 6 then '3. NSS'
    -- when a.order_type = 3 then '1. Food/Market'
    else 'Others' end as order_type  
,case when a.order_type in (7,200) then 7 else a.order_type end as order_code          
,a.city_id
,city.city_name
,case when a.city_id  = 217 then '1. HCM'
    when a.city_id  = 218 then '2. HN'
    when a.city_id  = 219 then '3. DN' else '4. OTH' 
    end as city_group
-- ,a.assign_type as at    
,case when a.assign_type = 1 then '1. Single Assign'
      when a.assign_type in (2,4) then '2. Multi Assign'
      when a.assign_type = 3 then '3. Well-Stack Assign'
      when a.assign_type = 5 then '4. Free Pick'
      when a.assign_type = 6 then '5. Manual'
      when a.assign_type in (7,8) then '6. New Stack Assign'
      else null end as assign_type
      
-- ,a.update_time
-- ,a.create_time
,from_unixtime(a.create_time - 60*60) as create_time
,from_unixtime(a.update_time - 60*60) as update_time
,extract(hour from from_unixtime(a.create_time - 60*60)) create_hour
,case when extract(minute from from_unixtime(a.create_time - 60*60)) < 30 then '1. 0 - 30 min'
      when extract(minute from from_unixtime(a.create_time - 60*60)) >= 30 then '2. 30 - 60 min'
      else null end as minute_range
,date(from_unixtime(a.create_time - 60*60)) as date_
,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
      when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
        else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week
,a.status
,case when a.experiment_group in (3,4,7,8) then 1 else 0 end as is_auto_accepted
,case when a.experiment_group in (5,6,7,8) then 1 else 0 end as is_ca
-- ,sa.total_single_assign_turn
,case when sa.total_single_assign_turn = 0 or sa.total_single_assign_turn is null then '# 0' 
    when sa.total_single_assign_turn = 1 then '# SA 1'
    when sa.total_single_assign_turn = 2 then '# SA 2'
    when sa.total_single_assign_turn = 3 then '# SA 3'
    when sa.total_single_assign_turn > 3 then '# SA 3+'
    else null end as total_single_assign_turn
,a.shipper_uid as shipper_id    
-- ,a_filter.order_id as f_


from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        where status in (3,4) -- shipper incharge

        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
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
    
        
    -- count # single assign for each order 
    LEFT JOIN 
            (SELECT a.order_uid
                ,count(case when assign_type = 1 then a.order_id else null end) as total_single_assign_turn
            
            from
                (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
        
                    from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        
                UNION
            
                SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
        
                    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                )a
                
                GROUP By 1
            )sa on sa.order_uid = a.order_uid
            
    -- location
    left join (SELECT city_id
                    ,city_name
                    
                    from shopeefood.foody_mart__fact_gross_order_join_detail
                    where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP)) 
                    
                    GROUP BY city_id
                    ,city_name
                   )city on city.city_id = a.city_id
    
        
where 1=1
and a_filter.order_id is null -- take last incharge
-- and a.order_id = 109630183

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

-- LIMIT 100
    
)base    


where base.date_ between date('2021-04-01') and date(now()) - interval '1' day

and order_type = '1. Food/Market'
and assign_type = '4. Free Pick'

)
,

free_pick_order as 
(
 SELECT shipper_uid, order_id
       ,coalesce(max(case when status = 11 then create_time else null end),0) as second_incharge_timestamp

 FROM shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live

 GROUP BY 1,2
)
,
check_maxhandle as 
(
 SELECT shipper_uid, order_id
       ,coalesce(max(case when status = 11 then create_time else null end),9999999999) as first_incharge_timestamp
       ,coalesce(max(case when status = 7 then create_time else null end),0) as first_delivered_timestamp
       ,coalesce(max(case when status = 10 then create_time else null end),9999999999) as first_reassign_timestamp
       ,coalesce(max(case when status = 12 then create_time else null end),9999999999) as first_deny_timestamp
       ,coalesce(max(case when status = 8 then create_time else null end),9999999999) as first_cancel_timestamp
       
 FROM shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live

 GROUP BY 1,2
)

, all_freepick AS
(
SELECT *
      ,case when date_diff('second',second_incharge_timestamp,first_delivered_timestamp) <= 30 then '1. 0 - 30s'
            when date_diff('second',second_incharge_timestamp,first_delivered_timestamp) <= 60 then '2. 30 - 60s'
            when date_diff('second',second_incharge_timestamp,first_delivered_timestamp) <= 300 then '3. 60 - 300s'
            when date_diff('second',second_incharge_timestamp,first_delivered_timestamp) > 300 then '4. 300s++'
            else null end as diff_2nd_incharge_1st_del_group
FROM
(
SELECT a.order_id as second_order_id
      ,a.order_type
      ,a.order_code
      ,a.city_name
      ,a.city_group
      ,a.assign_type
      ,a.create_time
      ,a.update_time
      ,a.create_hour
      ,a.minute_range
      ,a.date_
      ,a.year_week
      ,a.shipper_id
      ,from_unixtime(b.second_incharge_timestamp - 60*60) as second_incharge_timestamp
      ,c.order_id as first_order_id
     ,from_unixtime(c.first_incharge_timestamp - 60*60) first_incharge_timestamp
     ,from_unixtime(c.first_delivered_timestamp - 60*60) first_delivered_timestamp
     ,from_unixtime(c.first_reassign_timestamp - 60*60) first_reassign_timestamp
     ,from_unixtime(c.first_deny_timestamp - 60*60) first_deny_timestamp
     ,from_unixtime(c.first_cancel_timestamp - 60*60) first_cancel_timestamp
     ,row_number() over(partition by a.order_id order by from_unixtime(c.first_incharge_timestamp - 60*60) asc) rank
FROM (select * from free_pick) a

LEFT JOIN (select * from free_pick_order) b on a.order_id = b.order_id and a.shipper_id = b.shipper_uid

LEFT JOIN (select * from check_maxhandle) c on b.shipper_uid = c.shipper_uid and ((b.second_incharge_timestamp > c.first_incharge_timestamp and b.second_incharge_timestamp < c.first_delivered_timestamp)
                                                                  and (b.second_incharge_timestamp < c.first_reassign_timestamp) 
                                                                  and (b.second_incharge_timestamp < c.first_deny_timestamp) 
                                                                  and (b.second_incharge_timestamp < c.first_cancel_timestamp) 
                                                                    )
WHERE b.second_incharge_timestamp > 0        
and c.order_id > 0
)base

where 1=1
and rank = 1
--group by 1
)

, all_order as
(
SELECT order_id 
      ,city_group
      ,city_name
      ,created_date
      ,report_date
      ,created_year_week
      ,created_hour
      ,created_timestamp
      ,is_hub
      ,is_hub_shift
      ,is_stack
      ,is_group_order
      ,is_late_sla
      ,is_late_eta_max
      ,lt_completion
      ,lt_sla
      ,lt_eta_max
      ,is_valid_submit_to_del
      ,lt_submit_to_incharge 
      ,lt_incharge_to_pick               
      ,lt_pick_to_del
      ,is_valid_lt_submit_to_incharge           
      ,is_valid_lt_incharge_to_pick 
      ,is_valid_lt_pick_to_del 
      
      ,distance_range
FROM
(
SELECT   base1.*
        
        ,case when base1.lt_completion > base1.lt_sla then 1 else 0 end as is_late_sla
        ,case when base1.lt_completion*60 - base1.lt_sla*60 is null then null
              when base1.lt_completion*60 - base1.lt_sla*60 < -10*60 then '4. Early 10+ mins'
              when base1.lt_completion*60 - base1.lt_sla*60 < -5*60 then '5. Early 5-10 mins'
              when base1.lt_completion*60 - base1.lt_sla*60 <= 0*60 then '6. Early 0-5 mins'
              when base1.lt_completion*60 - base1.lt_sla*60 <= 5*60 then '1. Late SLA 0-5 mins'
              when base1.lt_completion*60 - base1.lt_sla*60 <= 10*60 then '2. Late SLA 5-10 mins'
              when base1.lt_completion*60 - base1.lt_sla*60 <= 15*60 then '3. Late SLA 10 - 15 mins'
              when base1.lt_completion*60 - base1.lt_sla*60 > 15*60 then '4. Late SLA 15+ mins'
              else null end as range_lt_from_sla_to_actual_delivered

        ,case when base1.distance <= 1 then '1. 0-1km'
              when base1.distance <= 2 then '2. 1-2km'
              when base1.distance <= 3 then '3. 2-3km'
              when base1.distance <= 4 then '4. 3-4km'
              when base1.distance <= 5 then '5. 4-5km'
              when base1.distance > 5 then '6. 5km+'
              else null end as distance_range
        ,case when is_hub_driver = 1 and is_order_in_hub_shift = 1 then 1 else 0 end as is_hub


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
              ,case when base.source = 'order_delivery' then 'NowFood' else 'NowShip' end as source
              ,case when base.order_status = 400 then 'Delivered'
                    when base.order_status = 401 then 'Quit'
                    when base.order_status in (402,403,404) then 'Cancelled'
                    when base.order_status in (405) then 'Returned'
                    else 'Others' end as order_status
                    
              ,base.order_status_group
              ,base.is_stack_order is_stack
              ,base.is_group_order
              ,base.group_code
              ,base.group_id 

              ,base.created_timestamp
              ,base.first_auto_assign_timestamp
              ,base.last_delivered_timestamp
              ,base.estimated_delivered_time
              ,base.last_picked_timestamp
              ,base.inflow_timestamp
              ,date_format(base.inflow_timestamp,'%a') inflow_day_of_week
            --   ,base.created_timestamp
              ,EXTRACT(HOUR from base.created_timestamp) created_hour
              ,EXTRACT(HOUR from base.inflow_timestamp) inflow_hour
               
              ,base.is_asap 

              ,base.delivery_distance distance
              ,case when base.delivery_distance <= 1 then 30
                    when base.delivery_distance > 1 then least(60,30 + 5*(ceiling(base.delivery_distance) -1))
                    else null end as lt_sla

              ,case when base.first_auto_assign_timestamp < base.last_incharge_timestamp then 1 else 0 end as is_valid_incharge
              ,case when base.created_timestamp <= base.last_delivered_timestamp then 1 else 0 end as is_valid_submit_to_del
              ,case when base.created_timestamp <= base.estimated_delivered_time then 1 else 0 end as is_valid_submit_to_eta
              
              ,date_diff('second',base.first_auto_assign_timestamp,base.last_incharge_timestamp)*1.0000/60 as lt_incharge
              ,date_diff('second',base.created_timestamp,base.last_delivered_timestamp)*1.0000/60 as lt_completion
              ,date_diff('second',base.created_timestamp,base.estimated_delivered_time)*1.0000/60 as lt_eta
              ,date_diff('second',base.created_timestamp,(case when base.max_eta = 0 then base.estimated_delivered_time else from_unixtime(base.submitted_time + base.max_eta - 60*60) end))*1.0000/60 as lt_eta_max
              ,case when base.last_delivered_timestamp > (case when base.max_eta = 0 then base.estimated_delivered_time else from_unixtime(base.submitted_time + base.max_eta - 60*60) end) then 1 else 0 end as is_late_eta_max
              ,date_diff('second',base.estimated_delivered_time,base.last_delivered_timestamp) lt_from_promise_to_actual_delivered
                    
              ,base.is_hub_driver
              ,case when driver_payment_policy = 2 then 1 else 0 end as is_hub_shift
              ,case when (base.driver_payment_policy = 3) or (base.driver_payment_policy = 1 and (EXTRACT(HOUR from base.last_incharge_timestamp) <=7 or EXTRACT(HOUR from base.last_incharge_timestamp) >= 22 )) then 0 else 1 end as is_order_in_hub_shift

              ,date_diff('second',base.created_timestamp ,base.confirm_timestamp)*1.0000 as lt_submit_to_confirm -- t_confirm            
              ,date_diff('second',base.confirm_timestamp,base.last_picked_timestamp)*1.0000 as lt_merchant_prep -- t_prep 
              ,date_diff('second',base.created_timestamp ,base.last_incharge_timestamp)*1.0000 as lt_submit_to_incharge -- t_assign
              ,date_diff('second',base.last_incharge_timestamp ,base.last_picked_timestamp)*1.0000 as lt_incharge_to_pick -- t_pickup               
              ,date_diff('second',base.last_picked_timestamp ,base.last_delivered_timestamp)*1.0000 as lt_pick_to_del -- t_drop_off

              ,case when base.created_timestamp <= base.confirm_timestamp then 1 else 0 end as is_valid_lt_submit_to_confirm -- t_confirm             
              ,case when base.confirm_timestamp <= base.last_picked_timestamp then 1 else 0 end as is_valid_lt_merchant_prep -- t_prep
              ,case when base.created_timestamp <= base.last_incharge_timestamp then 1 else 0 end as is_valid_lt_submit_to_incharge -- t_assign              
              ,case when base.last_incharge_timestamp <= base.last_picked_timestamp then 1 else 0 end as is_valid_lt_incharge_to_pick -- t_pickup
              ,case when base.last_picked_timestamp <= base.last_delivered_timestamp then 1 else 0 end as is_valid_lt_pick_to_del -- t_drop_off
              
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
                          ,dot.group_id
                          ,dot.is_asap 
                          ,dot.delivery_distance*1.0000/1000 delivery_distance
                                                                                                                     
                          ,case when dot.ref_order_status in (7,9,11) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60)) else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
                          ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
                          ,from_unixtime(dot.submitted_time- 60*60) created_timestamp
                          ,case when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
                                when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                                else YEAR(cast(from_unixtime(dot.submitted_time - 60*60) as date))*100 + WEEK(cast(from_unixtime(dot.submitted_time - 60*60) as date)) end as created_year_week
                          ,concat(cast(YEAR(from_unixtime(dot.submitted_time - 60*60)) as VARCHAR),'-',date_format(from_unixtime(dot.submitted_time - 60*60),'%b')) as created_year_month
                          ,dot.submitted_time
                          ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
                          ,case when dot.estimated_drop_time = 0 then null else from_unixtime(dot.estimated_drop_time - 60*60) end as estimated_delivered_time
                          ,fa.first_auto_assign_timestamp 
                          ,fa.last_incharge_timestamp
                          ,fa.last_picked_timestamp 
                          ,case when dot.is_asap = 0 then fa.first_auto_assign_timestamp else from_unixtime(dot.submitted_time- 60*60) end as inflow_timestamp
                          
                          ,district.name_en as district_name 
                          ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
                          ,case when dot.pick_city_id = 217 then '1. HCM'
                                when dot.pick_city_id = 218 then '2. HN'
                                when dot.pick_city_id = 219 then '3. DN'
                                -- when dot.pick_city_id = 222 then '4. Dong Nai'
                                ELSE '5. OTH' end as city_group
                          ,dot.pick_city_id as city_id 
                          ,dot.pick_district_id as district_id
                          ,case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver

                          ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
                          ,eta.max_eta
                          ,from_unixtime(go.confirm_timestamp) confirm_timestamp
                          ,from_unixtime(go.pick_timestamp) pick_timestamp
                    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
                    left join (select * from shopeefood.foody_mart__fact_gross_order_join_detail where grass_region = 'VN') go on go.id = dot.ref_order_id and dot.ref_order_category = 0
                    left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
                    LEFT JOIN 
                             (SELECT *
                             
                              FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day)
                              WHERE grass_schema = 'foody_partner_db'
                              
                             )ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category 
                             
                    LEFT JOIN 
                             (SELECT *
                             
                              FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day)
                              WHERE grass_schema = 'foody_partner_db'
                              
                             )ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11 
                                                                                                                                        and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                                        and ogm_filter.create_time >  ogm.create_time 
                    LEFT JOIN 
                             (SELECT *
                              
                              FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day)
                              WHERE grass_schema = 'foody_partner_db'
                             )ogi on ogi.id = ogm.group_id 
                    
                    
                    left join (SELECT city_id
                                ,city_name
                                
                                from shopeefood.foody_mart__fact_gross_order_join_detail
                                where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP))
                                and grass_region = 'VN'
                                GROUP BY city_id
                                        ,city_name
                                )city on city.city_id = dot.pick_city_id
                    
                    Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = dot.pick_district_id
                    
                    
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
                             and grass_schema = 'foody_partner_db'
                             )ns
                    GROUP BY 1,2
                    )fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type
                    
                    
                    
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
                            )driver_hub on driver_hub.shipper_id = dot.uid and driver_hub.report_date = case when dot.is_asap = 0 and dot.ref_order_status in (7,9,11) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60)) 
                                                                                                             when dot.is_asap = 1 and dot.ref_order_status in (7,9,11) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))    
                                                                                                                  else date(from_unixtime(dot.submitted_time- 60*60)) end   
                                            
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
                            ,coalesce(cast(json_extract(eta.eta_data,'$.estimated_time_delta') as INT),0) as late_offset
                            ,eta_data
                            
                            from shopeefood.data_mining_db__order_eta_data_tab__reg_daily_s0_live eta
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
                            -- and a.order_id = 9490679
                            and a.order_type = 200
                            
                            GROUP BY 1,2,3,4,5,6,7,8
                            
                            )group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category 
                            
                    WHERE 1=1
                    and ogm_filter.create_time is null
                    and dot.pick_city_id <> 238
                    and dot.grass_schema = 'foody_partner_db'

                    )base
        
        
        WHERE 1=1
        and base.created_date >= date((current_date) - interval '15' day)
        and base.created_date < date(current_date)
        and base.order_status_group = 'Completed'
      
        )base1


)base2
where base2.source = 'NowFood'
-- and base2.city_group = '4. Dong Nai'
-- and base2.is_asap = 1
and base2.order_status = 'Delivered'
)
,report_date_time as 
(
SELECT
     DATE(report_date) AS report_date
    ,sequence(cast(t.report_date as timestamp ) ,cast(t.report_date as timestamp) + interval '86400' second, interval '1799.99' second  ) dt_array 
    ,1 as mapping
FROM
    (
(
SELECT sequence(current_date - interval '14' day, current_date - interval '1' day) bar)
CROSS JOIN

    unnest (bar) as t(report_date)
)
)
,final_dt as 
(select 
       t1.mapping
      ,t1.report_date  
      ,t2.dt_array_unnest as start_time 
      ,t2.dt_array_unnest + interval '1798.99' second as end_time 



from report_date_time t1 

cross join unnest (dt_array) as t2(dt_array_unnest) 

order by 2,1 desc)

SELECT 
         a.report_date
        ,a.start_time
        ,a.end_time
        ,base1.city_group
        -- ,base1.created_date
        -- ,base1.created_year_week
        -- ,base1.created_hour
        -- ,base1.created_timestamp
        , base1.is_hub
        , case when is_late_sla = 1 then 'Late SLA' else 'Non Late' end as late_sla
        , count(distinct case when base1.is_valid_submit_to_del = 1 then base1.order_id else null end) total_del
        , count(distinct case when base1.is_valid_submit_to_del = 1 and is_hub_shift = 1 then base1.order_id else null end) total_del_inshift
        , count(distinct case when base1.is_valid_submit_to_del = 1 and base1.is_stack = 1 then base1.order_id else null end) total_stack
        , count(distinct case when base1.is_valid_submit_to_del = 1 and base1.is_fp_maxhandle = 1  then base1.order_id else null end) total_fp_del
        , count(distinct case when base1.is_valid_submit_to_del = 1 and base1.is_group_order = 1 then base1.order_id else null end) total_group_del

        -- , sum(CASE WHEN (base1.is_stack = 1) THEN base1.total_del ELSE null END) total_stack_del
        -- , sum(CASE WHEN (base1.is_fp_maxhandle = 1) THEN base1.total_del ELSE null END) total_fp_del
        -- , sum(CASE WHEN (base1.is_group_order = 1) THEN base1.total_del ELSE null END) total_group_del
        -- ,base1.is_stack
        -- ,base1.is_group_order
        -- ,base1.is_fp_maxhandle
        -- ,base1.distance_range
        -- ,base1.is_late_sla
        -- ,count(distinct case when base1.is_valid_submit_to_del = 1 and base1.is_late_eta_max = 1 then base1.order_id else null end) total_late_eta_max
        -- ,count(distinct case when base1.is_valid_submit_to_del = 1 and base1.is_late_sla = 1 then base1.order_id else null end) total_late_sla
        -- ,count(distinct case when base1.is_valid_submit_to_del = 1 then base1.order_id else null end) total_del
        -- ,sum(case when base1.is_valid_submit_to_del = 1 then base1.lt_completion else 0 end) sum_lt_completion

        -- ,sum(case when base1.is_valid_submit_to_del = 1 and base1.is_valid_lt_submit_to_incharge = 1 then base1.lt_submit_to_incharge else 0 end) sum_lt_submit_to_incharge
        -- ,sum(case when base1.is_valid_submit_to_del = 1 and base1.is_valid_lt_incharge_to_pick = 1 then base1.lt_incharge_to_pick else 0 end) sum_lt_incharge_to_pick
        -- ,sum(case when base1.is_valid_submit_to_del = 1 and base1.is_valid_lt_pick_to_del = 1 then base1.lt_pick_to_del else 0 end) sum_lt_pick_to_del

        --      ,count(distinct case when is_valid_submit_to_del = 1 and is_valid_lt_submit_to_incharge = 1 then order_id else null end) total_del_submit_to_incharge
            -- ,count(distinct case when is_valid_submit_to_del = 1 and is_valid_lt_incharge_to_pick = 1 then order_id else null end) total_del_incharge_to_pick
        --   ,count(distinct case when is_valid_submit_to_del = 1 and is_valid_lt_pick_to_del = 1 then order_id else null end) total_del_pick_to_del

FROM final_dt a 

inner join
(
SELECT base.*
      ,least(1,is_fp_max_handle_2nd_order + is_fp_max_handle_1st_order) is_fp_maxhandle

FROM
(
SELECT a.*
      ,case when b.second_order_id is not null and b.first_order_id is not null then 1 else 0 end as is_fp_max_handle_2nd_order 
      ,case when c.first_order_id is not null then 1 else 0 end as is_fp_max_handle_1st_order
      ,b.diff_2nd_incharge_1st_del_group

FROM all_order a

LEFT JOIN all_freepick b on a.order_id = b.second_order_id 

LEFT JOIN (SELECT distinct first_order_id FROM all_freepick) c on a.order_id = c.first_order_id
)base
)base1 on base1.report_date = a.report_date and base1.created_timestamp between a.start_time and a.end_time
-- where is_group_order = 1 
-- where created_date between current_date - interval '1' day and current_date - interval '1' day

group by 1,2,3,4,5,6
