with raw as 
(select 
         a.group_id
        ,b.group_code
        ,a.ref_order_category
        ,mapping_status
        ,date(from_unixtime(a.create_time - 3600)) as created_date
        ,case when coalesce(group_order.order_type,0) = 200 then 1 else 0 end as is_group
        ,count(distinct ref_order_code) as total_order_in_group
        ,count(distinct case when mapping_status = 22 then ref_order_code else null end) as total_denied_group


from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) a 

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) b on b.id = a.group_id and a.ref_order_category = b.ref_order_category
-- group order check
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
                            
                            )group_order on group_order.order_id = a.group_id  and  group_order.order_category = a.ref_order_category
where 1 = 1 

-- and mapping_status = 22

-- and ref_order_category = 0

-- and group_id = 210219
group by 1,2,3,4,5,6

) 
,denied as 
(
select 
        deny_date 
       ,sum(case when is_denied_group = 1 then total_deny else null end ) as total_deny_group 

from 
(select 
         date(from_unixtime(de.create_time - 3600)) as deny_date 
        ,case when reason_id in(116,117,118,61,62,63) then 1 else 0 end as is_denied_group
        -- ,city.name_en as city_name
        ,count(de.order_id) as total_deny 


from shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live de 

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.id = de.order_id

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

-- where reason_id in(116,117,118,61,62,63)
where date(from_unixtime(de.create_time - 3600)) between current_date - interval  '30' day and current_date - interval  '1' day 

group by 1,2

)
group by 1 
)


select 
         created_date
        -- ,ref_order_category
        ,b.total_deny_group
        ,count(distinct group_code) as total_group_create
        ,count(distinct case when mapping_status = 11 then group_code else null end) as total_group_success
        -- ,sum(total_order_in_group) as total_order_in_group



from raw a 

left join denied b on b.deny_date = a.created_date 

where created_date between current_date - interval '30' day and current_date - interval '1' day


group by 1,2
order by 1 desc 







