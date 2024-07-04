with base as
(SELECT dot.uid as shipper_id
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
                          ,case when a.assign_type = 1 then '1. Single Assign'
                                when a.assign_type in (2,4) then '2. Multi Assign'
                                when a.assign_type = 3 then '3. Well-Stack Assign'
                                when a.assign_type = 5 then '4. Free Pick'
                                when a.assign_type = 6 then '5. Manual'
                                when a.assign_type in (7,8) then '6. New Stack Assign'
                                when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 then 'Group assign'
                                else null end as assign_type      
                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 then 1 else 0 end as  is_group_order 
                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 1 else 0 end as  is_stack_order 
                          ,ogi.group_code
                          ,dot.group_id
                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 then group_order.total_order_in_group 
                                when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 2 
                                else 1 end as  total_order_in_group  
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
                          ,date_diff('second',from_unixtime(dot.submitted_time- 60*60),fa.last_incharge_timestamp) as submit_to_incharged
                          ,date_diff('second',fa.last_incharge_timestamp,fa.last_picked_timestamp) as incharged_to_pickup
                          ,date_diff('second',from_unixtime(dot.submitted_time- 60*60),from_unixtime(dot.real_drop_time - 60*60)) as submit_to_delivered
                          ,case when dot.real_drop_time > 0 then 1 else 0 end as is_valid

                          ,case when dot.is_asap = 0 then fa.first_auto_assign_timestamp else from_unixtime(dot.submitted_time- 60*60) end as inflow_timestamp
                          ,case when dot.is_asap = 0 then extract(hour from fa.first_auto_assign_timestamp) else extract(hour from from_unixtime(dot.submitted_time- 60*60)) end as inflow_hour
                          ,case when dot.is_asap = 0 then date(fa.first_auto_assign_timestamp) else date(from_unixtime(dot.submitted_time- 60*60)) end as inflow_date
                          
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
                          ,case when cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_hub_inshift
                          ,eta.max_eta
                        --   ,from_unixtime(go.confirm_timestamp) confirm_timestamp
                        --   ,from_unixtime(go.pick_timestamp) pick_timestamp

                    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

                    left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

                    -- left join (select * from shopeefood.foody_mart__fact_gross_order_join_detail where grass_region = 'VN') go on go.id = dot.ref_order_id and dot.ref_order_category = 0
                    
                    left join 
                                (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                            
                                    from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                                    where status in (3,4) -- shipper incharge
                            
                                    UNION
                                
                                    SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                            
                                    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                    where status in (3,4) -- shipper incharge
                                )a on a.order_id = dot.ref_order_id and dot.ref_order_category = a.order_type
                                
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
                                    ,ogm.total_order_in_group
                            
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
                               LEFT JOIN
                               (select 
                                                group_id
                                            --    ,ref_order_category 
                                            --    ,ref_order_code
                                            --    ,mapping_status
                                               ,count(distinct ref_order_code) as total_order_in_group
                               
                               
                               from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day)
                               group by 1 
                               )  ogm on ogm.group_id =  (case when a.order_type = 200 then a.order_id else 0 end)    
                                    
                                -- auto accept 
                                
                               LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end
                            
                            where 1=1
                            and a_filter.order_id is null -- take last incharge
                            -- and a.order_id = 9490679
                            and a.order_type = 200
                            
                            GROUP BY 1,2,3,4,5,6,7,8,9
                            
                            )group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category 
                            
                    WHERE 1=1
                    and ogm_filter.create_time is null
                    and dot.order_status = 400
                    and dot.ref_order_category = 0
                    and dot.pick_city_id <> 238
                    and dot.grass_schema = 'foody_partner_db'
                    and a_filter.order_id is null
                    and (case when dot.is_asap = 0 then date(fa.first_auto_assign_timestamp) else date(from_unixtime(dot.submitted_time- 60*60)) end) between current_date - interval '30' day and current_date - interval '1' day
)

-- select * from base where submit_to_delivered < 0

select 
        inflow_date
       ,inflow_hour 
       ,city_name
       ,assign_type
       ,IF(ref_order_category = 0, 'order-delivery','order-spxi') as order_source 
       ,IF(is_hub_inshift = 1,'Hub inshift','non hub') as delivery_type 
    --    ,order_code  
    --    ,submit_to_delivered
       ,count(distinct order_code) as total_del
       ,count(distinct case when is_asap = 1 and is_valid = 1 then order_code else null end) as total_del_asap 
       ,sum(case when is_asap = 1 and is_valid = 1  then submit_to_incharged else null end) as lt_incharged_asap
       ,sum(case when is_asap = 1 and is_valid = 1  then incharged_to_pickup else null end) as lt_pickup_asap
       ,sum(case when is_asap = 1 and is_valid = 1  then submit_to_delivered else null end) as lt_e2e_asap 

from base 


where inflow_date >= date'2022-08-16' - interval '3' day
and city_name in ('HCM City','Ha Noi City')
and assign_type is not null 
group by 1,2,3,4,5,6

