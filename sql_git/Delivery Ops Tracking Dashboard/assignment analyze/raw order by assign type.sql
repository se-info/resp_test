-- DROP TABLE IF EXISTS dev_vnfdbi_opsndrivers.phong_assign_type_by_hour;
-- CREATE TABLE IF NOT EXISTS dev_vnfdbi_opsndrivers.phong_assign_type_by_hour AS 

with report_date_time as 
(
SELECT
     DATE(report_date) AS report_date
    ,sequence(cast(t.report_date as timestamp ) ,cast(t.report_date as timestamp) + interval '86400' second, interval '1799.99' second  ) dt_array 
    ,1 as mapping
FROM
(
(SELECT sequence(current_date - interval '7' day, current_date - interval '1' day) bar)
CROSS JOIN unnest (bar) as t(report_date)
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

order by 2,1 desc
)
,free_pick as 
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
-- ,case when sa.total_single_assign_turn = 0 or sa.total_single_assign_turn is null then '# 0' 
--     when sa.total_single_assign_turn = 1 then '# SA 1'
--     when sa.total_single_assign_turn = 2 then '# SA 2'
--     when sa.total_single_assign_turn = 3 then '# SA 3'
--     when sa.total_single_assign_turn > 3 then '# SA 3+'
--     else null end as total_single_assign_turn
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
    -- LEFT JOIN 
    --         (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
    
    --         from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
    --         where status in (3,4) -- shipper incharge
    
    --         UNION
        
    --         SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
    
    --         from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
    --         where status in (3,4) -- shipper incharge
    --     )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time
        
    -- auto accept 
    
        
    -- count # single assign for each order 
    -- LEFT JOIN 
    --         (SELECT a.order_uid
    --             ,count(case when assign_type = 1 then a.order_id else null end) as total_single_assign_turn
            
    --         from
    --             (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
        
    --                 from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        
    --             UNION
            
    --             SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
        
    --                 from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
    --             )a
                
    --             GROUP By 1
    --         )sa on sa.order_uid = a.order_uid
            
    -- location
    left join (SELECT city_id
                    ,city_name
                    
                    from shopeefood.foody_mart__fact_gross_order_join_detail
                    where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP)) 
                    
                    GROUP BY city_id
                    ,city_name
                   )city on city.city_id = a.city_id
    
        
where 1=1
-- and a_filter.order_id is null -- take last incharge
-- and a.order_id = 109630183

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

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
 SELECT shipper_uid
       ,order_id
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

LEFT JOIN (select * from check_maxhandle) c on b.shipper_uid = c.shipper_uid and ((b.second_incharge_timestamp > c.first_incharge_timestamp 
                                                                  and b.second_incharge_timestamp < c.first_delivered_timestamp)
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
,metrics as 
(SELECT 
         a.order_id
        ,a.shipper_uid as shipper_id
        ,case when city_id = 217 then 'HCM'
              when city_id = 218 then 'HN'
              when city_id = 219 then 'DN'
              else 'OTH' end as city_group   
        ,1 as mapping 
        ,case when a.order_type = 200 then 'Group' else 'Single' end as assign_type_lv1
        -- ,case when a.order_type = 0 then '1. Food/Market'
        --       else '2. SPXI' end as order_type  
        ,case when a.order_type <> 200 
                    then (case when a.order_type = 0 then '1. Food/Market'
                         else '2. SPXI' end) 
                    else (case when ogi.ref_order_category = 0 then '1. Food/Market'
                    else '2. SPXI' end) 
                end as order_category 
        ,case when a.assign_type = 1 then '1. Single Assign'
              when a.assign_type in (2,4) then '2. Multi Assign'
              when a.assign_type = 3 then '3. Well-Stack Assign'
              when a.assign_type = 5 then '4. Free Pick'
              when a.assign_type = 6 then '5. Manual'
              when a.assign_type in (7,8) then '6. New Stack Assign'
              else null end as assign_type_lv2
        ,from_unixtime(a.create_time - 60*60) as create_time
        ,from_unixtime(a.update_time - 60*60) as update_time
        ,date(from_unixtime(a.create_time - 60*60)) as date_
        ,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
              when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
              else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week
        ,coalesce(ogm.total_order_in_group,1) as total_order_in_group
        ,coalesce(cast(ogm.order_id as map(varchar ,varchar)),null) as order_id_in_group
from 
(
SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live

where status in (3,4,2,14,15)
and date(from_unixtime(create_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day 

UNION
    
SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live

where status in (3,4,2,14,15)
and date(from_unixtime(create_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day 
) a 
LEFT JOIN
(select 
         group_id
        -- ,ref_order_category 
        -- ,ref_order_code
        -- ,mapping_status 
        ,map_agg(ref_order_code,ref_order_category) as order_id 
        ,count(distinct ref_order_code) as total_order_in_group
                               
                               
from (  select * 
        from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day
     )
        group by 1 
     )  ogm on ogm.group_id =  (case when a.order_type = 200 then a.order_id else 0 end)                                    
                                
LEFT JOIN ( select * 
            from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da 
            where date(dt) = current_date - interval '1' day
          ) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end
       

order by create_time desc      
)
,final as 
(select 
        a.*
       ,dt.start_time
       ,dt.end_time 
       ,case when coalesce(a.create_time,a.update_time) between dt.start_time and dt.end_time then 1 else 0 end as is_valid   
       ,case when b.second_order_id is not null and b.first_order_id is not null then 1 else 0 end as is_fp_max_handle_2nd_order 
       ,case when c.first_order_id is not null then 1 else 0 end as is_fp_max_handle_1st_order

from metrics a 

LEFT JOIN all_freepick b on a.order_id = b.second_order_id and a.shipper_id = b.shipper_id

LEFT JOIN (SELECT distinct first_order_id,shipper_id FROM all_freepick) c on a.order_id = c.first_order_id and a.shipper_id = c.shipper_id

LEFT JOIN final_dt dt on dt.mapping = a.mapping
)

select 
       date_ 
      ,start_time
      ,end_time
      ,city_group
      ,order_category
      ,sum(case when assign_type_lv1 = 'Single' and assign_type_lv2 = '1. Single Assign' and is_fp_maxhandle = 0 then total_assign_turn else null end ) as total_assign_turn_single_assign
      ,sum(case when assign_type_lv1 = 'Single' and assign_type_lv2 = '6. New Stack Assign'then total_assign_turn else null end ) as total_assign_turn_stack_assign
      ,sum(case when assign_type_lv1 = 'Single' and is_fp_maxhandle = 1 then total_assign_turn else null end ) as total_assign_turn_fp_maxhandle
      ,sum(case when assign_type_lv1 = 'Group' and assign_type_lv2 = '1. Single Assign' then total_assign_turn else null end ) as total_assign_turn_group_single
      ,sum(case when assign_type_lv1 = 'Group' and assign_type_lv2 = '4. Free Pick' then total_assign_turn else null end ) as total_assign_turn_group_freepick
      ,sum(case when assign_type_lv1 = 'Single' and assign_type_lv2 = '4. Free Pick' and is_fp_maxhandle = 0 then total_assign_turn else null end ) as total_assign_turn_single_freepick
      ,sum(case when assign_type_lv1 = 'Single' and assign_type_lv2 = '5. Manual' and is_fp_maxhandle = 0 then total_assign_turn else null end ) as total_assign_turn_single_manual

from 
(select 
         date(f.start_time) as date_
        ,f.start_time
        ,f.end_time
        ,city_group
        ,order_category
        ,assign_type_lv1
        ,assign_type_lv2
        ,least(1,is_fp_max_handle_2nd_order + is_fp_max_handle_1st_order) is_fp_maxhandle
        ,total_order_in_group
        ,count(order_id) as total_assign_turn
        ,count(distinct f.order_id) as total_order_being_incharged
        ,count(distinct shipper_id) as total_unique_driver_assigned


from final f 


where is_valid = 1

group by 1,2,3,4,5,6,7,8,9
)

group by 1,2,3,4,5

