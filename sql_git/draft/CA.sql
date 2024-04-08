SELECT  base.date_ 
,base.year_week
,base.city_group
,base.city_name
,base.order_source
,base.order_group_type
,base.assign_type
,base.is_auto_accepted
,base.is_auto_accepted_continuous_assign
,case when base.assign_type = '1. Single Assign' then base.total_single_assign_turn
    else base.assign_type end as assign_type_lv2 
,count(distinct base.order_uid) as total_order_being_incharged
,count(distinct case when is_asap = 1 and ref_order_status = 7 then base.order_uid else null end) as total_asap_delivered
,sum(total_order) total_order
,sum(denied_turn_CA) total_denied_turn_CA
--,sum(ignore_turn_CA) total_ignore_turn_CA
,sum(denied_turn) total_denied_turn
,sum(ignore_turn) total_ignore_turn
,sum(lt_incharge) assign_time
,sum(case when is_asap = 1 and ref_order_status = 7 then lt_completion else null end) completion_time
from
(SELECT a.order_uid
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
,city.city_name
,case when a.city_id  = 217 then 'HCM'
    when a.city_id  = 218 then 'HN'
    when a.city_id  = 219 then 'DN'
 	when a.city_id  = 220 then 'Hai Phong'
    when a.city_id  = 221 then 'Can Tho'
 	when a.city_id  = 222 then 'Dong Nai'
    when a.city_id  = 223 then 'Vung Tau'
    when a.city_id  = 230 then 'Binh Duong'
    when a.city_id  = 273 then 'Hue'
 	else 'OTH'
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
,date(from_unixtime(a.create_time - 60*60)) as date_
,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
      when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
        else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week
,a.status
,case when a.experiment_group in (3,4) then 1 ELSE 0 end as is_auto_accepted
,case when a.experiment_group in (7,8) then 1 ELSE 0 end as is_auto_accepted_continuous_assign
-- ,sa.total_single_assign_turn
,case when sa.total_single_assign_turn = 0 or sa.total_single_assign_turn is null then '# 0' 
    when sa.total_single_assign_turn = 1 then '# SA 1'
    when sa.total_single_assign_turn = 2 then '# SA 2'
    when sa.total_single_assign_turn = 3 then '# SA 3'
    when sa.total_single_assign_turn > 3 then '# SA 3+'
    else null end as total_single_assign_turn
,case when a.order_type <> 200 then 1 else coalesce(order_rank.total_order_in_group_at_start,0) end as total_order 
,coalesce(do.total_denied_CA,0) as denied_turn_CA
,coalesce(do.total_ignore_CA,0) as ignore_turn_CA
,coalesce(do.total_denied,0) as denied_turn
,coalesce(do.total_ignore,0) as ignore_turn
,date_diff('second',fa.first_auto_assign_timestamp,fa.last_incharge_timestamp)*1.0000/60 as lt_incharge
,date_diff('second',from_unixtime(dot.submitted_time - 3600),from_unixtime(dot.real_drop_time - 3600))*1.0000/60 as lt_completion
,dot.is_asap
,dot.ref_order_status
-- ,a_filter.order_id as f_


from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

        from foody.foody_partner_archive_db__order_assign_shipper_log_archive_tab
        where status in (3,4) -- shipper incharge

        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

        from foody.foody_partner_db__order_assign_shipper_log_tab
        where status in (3,4) -- shipper incharge
    )a
    
    -- take last incharge
    LEFT JOIN 
            (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
    
            from foody.foody_partner_archive_db__order_assign_shipper_log_archive_tab
            where status in (3,4) -- shipper incharge
    
            UNION
        
            SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
    
            from foody.foody_partner_db__order_assign_shipper_log_tab
            where status in (3,4) -- shipper incharge
        )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time
        
    -- auto accept 
    
        
    -- count # single assign for each order 
    LEFT JOIN 
            (SELECT a.order_uid
                ,count(case when assign_type = 1 then a.order_id else null end) as total_single_assign_turn
            
            from
                (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
        
                    from foody.foody_partner_archive_db__order_assign_shipper_log_archive_tab
        
                UNION
            
                SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
        
                    from foody.foody_partner_db__order_assign_shipper_log_tab
                )a
                
                GROUP By 1
            )sa on sa.order_uid = a.order_uid
            
    ---denied turn 
    LEFT JOIN 
            (SELECT a.order_uid
                ,count(case when a.status in (2,14,15) and a.experiment_group in (7,8) then a.order_id else null end) as total_denied_CA
                ,count(case when a.status in (8,9) and a.experiment_group in (7,8) then a.order_id else null end) as total_ignore_CA
                 ,count(case when a.status in (2,14,15) then a.order_id else null end) as total_denied
                ,count(case when a.status in (8,9) then a.order_id else null end) as total_ignore
            
            from
                (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,experiment_group
        
                    from foody.foody_partner_archive_db__order_assign_shipper_log_archive_tab
        
                UNION
            
                SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,experiment_group
        
                    from foody.foody_partner_db__order_assign_shipper_log_tab
                )a
                
                GROUP BY 1
            )do on do.order_uid = a.order_uid        
    -- location
    left join (SELECT city_id
                    ,city_name
                    
                    from shopeefood.foody_mart__fact_gross_order_join_detail
                    where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP)) 
                    and grass_region = 'VN'
                    GROUP BY city_id
                    ,city_name
                   )city on city.city_id = a.city_id
    
   LEFT JOIN foody.foody_partner_db__order_group_info_tab ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

LEFT JOIN
            (SELECT ogm.group_id
                   ,ogi.group_code
                   ,count (distinct ogm.ref_order_id) as total_order_in_group
                   ,count(distinct case when ogi.create_time = ogm.create_time then ogm.ref_order_id else null end) total_order_in_group_at_start
             FROM
                     (SELECT *
                     
                      FROM foody.foody_partner_db__order_group_mapping_tab
                      WHERE grass_schema = 'foody_partner_db'
                      
                     )ogm 
            
             LEFT JOIN 
                     (SELECT *
                      
                      FROM foody.foody_partner_db__order_group_info_tab
                      WHERE grass_schema = 'foody_partner_db'
                     )ogi on ogi.id = ogm.group_id 
             WHERE 1=1
             and ogm.group_id is not null
            
             GROUP BY 1,2
             )order_rank on order_rank.group_id = case when a.order_type = 200 then a.order_id else 0 end
             
---time performance
LEFT JOIN
            (
            SELECT order_id , 0 as order_type
            ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
            ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
            ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
            from foody.foody_order_db__order_status_log_tab
            where 1=1
            and grass_schema = 'foody_order_db'
            group by 1,2
            
            UNION
            
            SELECT ns.order_id, ns.order_type 
            ,min(from_unixtime(create_time - 60*60)) first_auto_assign_timestamp
            ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
            ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
            FROM
            ( SELECT order_id, order_type , create_time , update_time, status
            
            from foody.foody_partner_archive_db__order_assign_shipper_log_archive_tab
            where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
            and grass_schema = 'foody_partner_archive_db'
            UNION
            
            SELECT order_id, order_type, create_time , update_time, status
            
            from foody.foody_partner_db__order_assign_shipper_log_tab
            where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
            and schema = 'foody_partner_db'
            )ns
            GROUP BY 1,2
            )fa on a.order_id = fa.order_id and a.order_type = fa.order_type

LEFT JOIN shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on dot.ref_order_id = a.order_id and a.order_type = dot.ref_order_category

where 1=1
and a_filter.order_id is null -- take last incharge

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27



)base    

where base.date_ between date(now()) - interval '30' day and date(now()) - interval '1' day


GROUP By 1,2,3,4,5,6,7,8,9,10