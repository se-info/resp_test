drop table if exists dev_vnfdbi_opsndrivers.phong_raw_assignment_test;
create table if not exists dev_vnfdbi_opsndrivers.phong_raw_assignment_test as 
WITH combine_raw AS
(SELECT
         DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS created_date
        ,dod.order_id 
        ,dod.uid AS shipper_id
        ,dot.ref_order_id 
        ,dot.ref_order_code AS order_code
        ,dot.ref_order_category AS order_category
        ,FROM_UNIXTIME(dod.create_time - 3600) AS created_timestamp
        -- ,rea.content_en as deny_reason
        ,'Denied' AS metrics 
        , CASE
                WHEN dod.deny_type = 0 THEN 'NA'
                WHEN dod.deny_type = 1 THEN 'Driver_Fault'
                WHEN dod.deny_type = 10 THEN 'Order_Fault'
                WHEN dod.deny_type = 11 THEN 'Order_Pending'
                WHEN dod.deny_type = 20 THEN 'System_Fault'
                END AS type_category
        ,CASE WHEN rea.content_en = 'Did not accept order belongs type "Auto accept"' THEN 1 ELSE 0 END AS is_denied_aa 
        ,0 AS status                
        ,rea.content_en AS deny_reason
        ,dod.reason_id

FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE date(dt) = current_date - interval '1' day) dot 
    on dod.order_id = dot.id

LEFT JOIN shopeefood.foody_internal_db__deny_reason_template_tab__reg_daily_s0_live rea 
    on rea.id = dod.reason_id

WHERE DATE(FROM_UNIXTIME(dod.create_time - 3600)) BETWEEN current_date - interval '180' day and current_date - interval '1' day

UNION ALL

SELECT 
        DATE(FROM_UNIXTIME(sa.create_time - 3600)) AS created_date
       ,sa.order_id
       ,sa.shipper_uid AS shipper_id
       ,COALESCE(ogm.ref_order_id,dot.ref_order_id) AS ref_order_id 
       ,COALESCE(ogm.ref_order_code,dot.ref_order_code) AS order_code
       ,COALESCE(ogi.ref_order_category,sa.order_type) AS order_category
       ,FROM_UNIXTIME(sa.create_time - 3600) AS created_timestamp
       ,CASE WHEN sa.status in (8,9,17,18,13) THEN 'Ignore' else 'Assign' end as metrics
       ,CASE 
            WHEN sa.assign_type = 1 then '1. Single Assign'
            WHEN sa.assign_type in (2,4) then '2. Multi Assign'
            WHEN sa.assign_type = 3 then '3. Well-Stack Assign'
            WHEN sa.assign_type = 5 then '4. Free Pick'
            WHEN sa.assign_type = 6 then '5. Manual'
            WHEN sa.assign_type in (7,8) then '6. New Stack Assign'
            ELSE NULL END AS type_category
       ,0 AS is_denied_aa
       ,status
       ,null as deny_reason
       ,null as reason_id                      
    --    ,CASE 
    --         WHEN sa.order_type = 200 then 'Group'
    --         ELSE 'Single' END AS order_type
    --    ,dot.order_status                      

FROM 
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live


        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
) sa 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogi
    on ogi.id = (CASE WHEN sa.order_type = 200 THEN sa.order_id ELSE 0 END)

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogm 
    on ogm.group_id = ogi.id
    and ogm.ref_order_category = ogi.ref_order_category
    and ogm.create_time <= sa.create_time

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_id ELSE sa.order_id END) 
    and dot.ref_order_category = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_category ELSE sa.order_type END)


WHERE 1 = 1
AND DATE(FROM_UNIXTIME(sa.create_time - 3600)) BETWEEN current_date - interval '180' day AND current_date - interval '1' day

)
SELECT * FROM combine_raw order by 7 DESC
