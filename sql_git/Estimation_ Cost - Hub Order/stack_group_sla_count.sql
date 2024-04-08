-- assignment condition 
WITH incharge_tab as
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        where status in (3,4) -- shipper incharge

        UNION

        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
        where status in (3,4) -- shipper incharge
)
,last_incharge as
(select 
    order_uid
    ,order_id
    ,max_by(city_id,create_time) as city_id
    ,max_by(assign_type,create_time) as assign_type
    ,max_by(update_time,create_time) as update_time
    ,max_by(status,create_time) as status
    ,max_by(create_time,create_time) as create_time
    ,max_by(order_type,create_time) as order_type
    ,max_by(experiment_group,create_time) as experiment_group
from incharge_tab

group by 1,2
)
,group_order as
(select 
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
        ,hour(from_unixtime(a.create_time - 60*60)) as created_hour
        ,from_unixtime(a.create_time - 60*60) as create_time
        ,from_unixtime(a.update_time - 60*60) as update_time
        ,date(from_unixtime(a.create_time - 60*60)) as date_
        ,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week
        -- ,case when a.experiment_group in (3,4) then 1 ELSE 0 end as is_auto_accepted
        -- ,case when a.experiment_group in (7,8) then 1 ELSE 0 end as is_auto_accepted_continuous_assign        
        ,total_order_in_group                
from last_incharge a
LEFT JOIN (select * 
            from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da 
            where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end
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
where a.order_type = 200
)
,assignment AS
(SELECT
    COALESCE(assign.date_, deny.deny_date) AS report_date
    , COALESCE(assign.shipper_id, deny.shipper_id) AS shipper_id
    , COALESCE(assign.cnt_total_assign_order,0) + COALESCE(deny.cnt_deny_acceptable,0) AS cnt_total_assign_order
    , COALESCE(assign.cnt_total_incharge,0) AS cnt_total_incharge
    , COALESCE(assign.cnt_ignore_total_all,0) AS cnt_ignore_total_all
    , COALESCE(assign.cnt_group_ignore_total,0) AS cnt_group_ignore_total
    , COALESCE(assign.cnt_stack_ignore_total,0) AS cnt_stack_ignore_total    
    , COALESCE(assign.cnt_ignore_delivery,0) AS cnt_ignore_delivery
    , COALESCE(assign.cnt_ignore_spxi,0) AS cnt_ignore_spxi
    , COALESCE(deny.cnt_deny_non_acceptable,0) AS cnt_deny_non_acceptable
FROM
        (SELECT
            date_
            , shipper_id
            , COUNT(DISTINCT order_uid) AS cnt_total_assign_order
            , COUNT(DISTINCT IF(status IN (3,4), order_uid, NULL)) AS cnt_total_incharge
            , COUNT(DISTINCT IF(status IN (8,9,17,18) /*and assign_type != '6. New Stack Assign'*/, order_uid, NULL)) AS cnt_ignore_total_all
            , COUNT(DISTINCT IF(status IN (8,9,17,18) and assign_type != '6. New Stack Assign', order_uid, NULL)) AS cnt_group_ignore_total
            , COUNT(DISTINCT IF(status IN (8,9,17,18) and order_type != '5. GROUP', order_uid, NULL)) AS cnt_stack_ignore_total
            , COUNT(DISTINCT IF(status IN (8,9,17,18) and order_type != '1. Food/Market' , order_uid, NULL)) AS cnt_ignore_spxi
            , COUNT(DISTINCT IF(status IN (8,9,17,18) and order_type = '1. Food/Market' , order_uid, NULL)) AS cnt_ignore_delivery

        FROM
            (SELECT
                a.shipper_id
                , a.order_uid
                , a.order_id
                , CASE
                    WHEN a.order_type = 0 THEN '1. Food/Market'
                    WHEN a.order_type in (4,5) THEN '2. NS'
                    WHEN a.order_type = 6 THEN '3. NSS'
                    WHEN a.order_type = 7 THEN '4. NS Same Day'
                    WHEN a.order_type = 200 THEN '5. GROUP'
                    ELSE 'Others' END AS order_type
                , a.order_type AS order_code
                ,CASE
                    WHEN a.assign_type = 1 THEN '1. Single Assign'
                    WHEN a.assign_type in (2,4) THEN '2. Multi Assign'
                    WHEN a.assign_type = 3 THEN '3. Well-Stack Assign'
                    WHEN a.assign_type = 5 THEN '4. Free Pick'
                    WHEN a.assign_type = 6 THEN '5. Manual'
                    WHEN a.assign_type in (7,8) THEN '6. New Stack Assign'
                ELSE NULL END AS assign_type
                , DATE(FROM_UNIXTIME(a.create_time - 3600)) AS date_
                , FROM_UNIXTIME(a.create_time - 3600) AS create_timestamp
                , HOUR(FROM_UNIXTIME(a.create_time - 3600)) AS create_hour
                , MINUTE(FROM_UNIXTIME(a.create_time - 3600)) AS create_minute
                , a.status
                , IF(a.experiment_group IN (3,4,7,8), 1, 0) AS is_auto_accepted
            FROM
                (SELECT
                    CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                    , order_id, city_id, assign_type, update_time, create_time, status, order_type
                    , experiment_group, shipper_uid AS shipper_id

                FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                WHERE status IN (3,4,8,9) -- shipper incharge + ignore
                AND DATE(FROM_UNIXTIME(create_time - 3600)) between date'2022-11-01' and date'2022-11-30'

                UNION ALL

                SELECT
                    CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                    , order_id, city_id, assign_type, update_time, create_time, status, order_type
                    , experiment_group, shipper_uid AS shipper_id

                FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                
                WHERE status IN (3,4,8,9) -- shipper incharge + ignore
                AND DATE(FROM_UNIXTIME(create_time - 3600)) between date'2022-11-01' and date'2022-11-30'
                ) a
            LEFT JOIN (select * 
            from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da 
            where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

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
            )
        GROUP BY 1,2
        ) assign

    FULL JOIN

        (SELECT
            deny_date
            , shipper_id
            , COUNT(ref_order_code) AS cnt_deny_total
            , COUNT(IF(deny_type <> 'Driver_Fault', ref_order_code, NULL)) AS cnt_deny_acceptable
            , COUNT(IF((deny_type = 'Driver_Fault' or deny_reason = 'Did not accept order belongs type "Auto accept"'), ref_order_code, NULL)) AS cnt_deny_non_acceptable
        FROM
            (SELECT
                dod.uid AS shipper_id
                , DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_date
                , FROM_UNIXTIME(dod.create_time - 3600) AS deny_timestamp
                , HOUR(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_hour
                , MINUTE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_minute
                , dot.ref_order_id
                , dot.ref_order_code
                , dot.ref_order_category
                , rea.content_en as deny_reason
                , CASE
                    WHEN dot.ref_order_category = 0 THEN 'Food/Market'
                    WHEN dot.ref_order_category = 4 THEN 'NS Instant'
                    WHEN dot.ref_order_category = 5 THEN 'NS Food Mex'
                    WHEN dot.ref_order_category = 6 THEN 'NS Shopee'
                    WHEN dot.ref_order_category = 7 THEN 'NS Same Day'
                    WHEN dot.ref_order_category = 8 THEN 'NS Multi Drop'
                ELSE NULL END AS order_source
                , CASE
                    WHEN dod.deny_type = 0 THEN 'NA'
                    WHEN dod.deny_type = 1 THEN 'Driver_Fault'
                    WHEN dod.deny_type = 10 THEN 'Order_Fault'
                    WHEN dod.deny_type = 11 THEN 'Order_Pending'
                    WHEN dod.deny_type = 20 THEN 'System_Fault'
                END AS deny_type
                , reason_text

            FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod
            LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dod.order_id = dot.id

            left  join shopeefood.foody_internal_db__deny_reason_template_tab__reg_daily_s0_live rea on rea.id = dod.reason_id

            WHERE DATE(FROM_UNIXTIME(dod.create_time - 3600)) between date'2022-11-01' and date'2022-11-30'

            ) dod
        group by 1,2
        ) deny on assign.date_ = deny.deny_date AND assign.shipper_id = deny.shipper_id
)   

-- Complete order 
,rate as 
(   
SELECT 
        date_ts  
       ,shipper_id
       ,count(distinct ref_order_id) as total_delivered
       ,count(distinct case when is_stack_group_order = 1 then ref_order_id else null end) as total_group_delivered
       ,count(distinct case when is_stack_group_order = 2 then ref_order_id else null end) as total_stack_delivered
       ,count(distinct case when shipper_rate = 1 then ref_order_id else null end) as star_1
       ,count(distinct case when shipper_rate = 2 then ref_order_id else null end) as star_2
       ,count(distinct case when shipper_rate = 3 then ref_order_id else null end) as star_3
       ,count(distinct case when shipper_rate = 4 then ref_order_id else null end) as star_4
       ,count(distinct case when shipper_rate = 5 then ref_order_id else null end) as star_5
       ,count(distinct case when shipper_rate = 0 then ref_order_id else null end) as no_rate


from         
(
select  dot.ref_order_id
       ,date(from_unixtime(real_drop_time - 3600)) as date_ts
       ,coalesce(sr.shipper_rate,0) as shipper_rate
       ,dot.uid as shipper_id
       ,case 
            when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time = ogi.create_time then 1  -- 1: group
            when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 2 -- 2: stack
            when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time != ogi.create_time then 2 -- 2: stack
            else 0 end as is_stack_group_order       

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

LEFT JOIN group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                        and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                        and ogm_filter.create_time >  ogm.create_time
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id

left join 
(
    
    SELECT 
        order_id
        ,shipper_uid as shipper_id
        ,case when cfo.shipper_rate = 0 then null
        when cfo.shipper_rate = 1 or cfo.shipper_rate = 101 then 1
        when cfo.shipper_rate = 2 or cfo.shipper_rate = 102 then 2
        when cfo.shipper_rate = 3 or cfo.shipper_rate = 103 then 3
        when cfo.shipper_rate = 104 then 4
        when cfo.shipper_rate = 105 then 5
        else null end as shipper_rate
        ,from_unixtime(cfo.create_time - 60*60) as create_ts

FROM shopeefood.foody_user_activity_db__customer_feedback_order_tab__reg_daily_s0_live cfo


) sr on sr.order_id = dot.ref_order_id and dot.ref_order_category = 0 and sr.shipper_id = dot.uid 

where dot.order_status = 400
and ogm_filter.create_time is null

)

where date_ts between date'2022-11-01' and date'2022-11-30'
group by 1,2
)

-- quit order 
,quit as 
(
select shipper_id 
       ,date_ts 
       ,count(case when risk_bearer = 'Driver' then id else null end) as total_quit_driver_fault 


from 
(select  
        oct.id 
       ,oct.shipper_uid as shipper_id  
       ,CASE WHEN CAST(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') AS integer) = 1 THEN 'Now' 
             ELSE 'Driver' END AS risk_bearer
       ,date(quit.quit_timestamp) as date_ts             
    --    ,COALESCE(CAST(json_extract(bo.note_content, '$.default') AS varchar), bo.extra_note) quit_reason      


from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct 

-- LEFT JOIN (select * from shopeefood.foody_mart__fact_order_note
--                     where grass_region = 'VN' ) bo 
--                     ON bo.order_id = oct.id AND bo.note_type_id = 3
--                     AND COALESCE(CAST(json_extract(bo.note_content, '$.default') AS varchar), CAST(json_extract(bo.note_content, '$.en') AS varchar), bo.extra_note) <> ''

LEFT JOIN (
            SELECT
              osl.order_id
            , osl.create_uid
            , max(from_unixtime(osl.create_time - 3600)) quit_timestamp
            FROM
              shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live osl
            WHERE 1 = 1 
            AND status = 9
            GROUP BY 1, 2
         )  quit ON quit.order_id = oct.id
)

where date_ts between date'2022-11-01' and date'2022-11-30'

group by 1,2
)


,base_driver as 
(select 
        bonus.uid
       ,hub.shipper_name
       ,hub.city_name 
       ,date(from_unixtime(bonus.report_date - 3600)) as report_date  
       ,bonus.completed_rate*1.00/100 as sla --current
       ,bonus.bonus_value*1.00/100 as bonus_value
       ,case when hub.shipper_type_id = 12 then 'Hub'
             when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
             when bonus.tier in (3,8,13) then 'T3'
             when bonus.tier in (4,9,14) then 'T4'
             when bonus.tier in (5,10,15) then 'T5'
             else null end as tier 


from shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

left join shopeefood.foody_mart__profile_shipper_master hub on hub.shipper_id = bonus.uid and try_cast(hub.grass_date as date) = date(from_unixtime(bonus.report_date - 3600))


)

,raw_est as 
(select 
       base.report_date 
      ,base.uid as shipper_id 
      ,base.shipper_name 
      ,base.city_name
      ,base.tier 
      ,base.sla as system_sla 
      ,base.bonus_value as daily_bonus
      ,sa.cnt_total_assign_order
      ,sa.cnt_total_incharge 
      ,coalesce(total_quit_driver_fault,0) as i
      ,sa.cnt_ignore_total_all as h
      ,sa.cnt_ignore_spxi as h_1 
      ,sa.cnt_ignore_delivery as h_2
      ,sa.cnt_group_ignore_total as g_1 
      ,sa.cnt_stack_ignore_total as s_1  
      ,sa.cnt_deny_non_acceptable as g 
      ,no_rate as f
      ,star_1 as e 
      ,star_2 as d 
      ,star_3 as c 
      ,star_4 as b
      ,star_5 as a 
      ,total_delivered
      ,total_group_delivered
      ,total_stack_delivered



from base_driver base 


left join assignment sa on sa.shipper_id = base.uid and base.report_date = sa.report_date


left join rate rt on rt.shipper_id = base.uid and rt.date_ts = base.report_date

left join quit on quit.shipper_id = base.uid and quit.date_ts = base.report_date

where base.report_date between date'2022-11-01' and date'2022-11-30'
-- and base.uid = 16762663
)
,final_metrics as 
(select  
         report_date
       , shipper_id 
       , shipper_name          
       , city_name
       , tier
       , system_sla
       , total_delivered
       , total_stack_delivered
       , total_group_delivered        
            ---
       , coalesce(sum(daily_bonus),0) as bonus_value
       , coalesce(sum(g),0) as denied_auto_accept
       , coalesce(sum(cnt_total_assign_order),0) as cnt_total_assign_order
       , coalesce(sum(h),0) as ignore_all 
       , coalesce(sum(g_1),0) as ignore_cnt_group
       , coalesce(sum(s_1),0) as ignore_cnt_stack 
            ---
       ,(sum(total_delivered)*1.00/cast((sum(g+h+i)*1.00 + sum(total_delivered)*1.00) as DOUBLE )) as estimate_sla_count_ignore_total_all  
       ,(sum(total_delivered)*1.00/cast((sum(g+g_1+i)*1.00 + sum(total_delivered)*1.00) as DOUBLE )) as estimate_sla_count_ignore_cnt_group
       ,(sum(total_delivered)*1.00/cast((sum(g+s_1+i)*1.00 + sum(total_delivered)*1.00) as DOUBLE )) as estimate_sla_count_ignore_cnt_stack
    --    ,(sum(total_delivered)*1.00/cast((sum(g+(h*0.5)+i)*1.00 + sum(total_delivered)*1.00) as DOUBLE )) as estimate_sla_count_ignore_total_adj_weight

from raw_est

where 1 = 1 
-- and h > 0 

group by 1,2,3,4,5,6,7,8,9
having (sum(total_delivered)>0)
)
select  
       report_date
      ,city_name
      ,tier   
      ,case when system_sla >= 90 and estimate_sla_count_ignore_total_all < 90 then 1 else 0 end as is_impacted 
      ,count(distinct shipper_id) as total_drivers  
      ,sum(total_delivered) as total_delivered 
      ,sum(total_stack_delivered) as total_stack_delivered 
      ,sum(total_group_delivered) as total_group_delivered 

from final_metrics 

where report_date between date'2022-11-01' and date'2022-11-30'
and tier != 'Hub'

group by 1,2,3,4