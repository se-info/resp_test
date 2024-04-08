WITH assignment AS 
(SELECT 
        sa.order_id
       ,COALESCE(ogi.group_code,'0') AS group_code 
       ,COALESCE(ogm.ref_order_id,dot.ref_order_id) AS ref_order_id 
       ,COALESCE(ogm.ref_order_code,dot.ref_order_code) AS order_code
       ,COALESCE(ogi.ref_order_category,sa.order_type) AS order_category
       ,sa.status
       ,sa.shipper_uid AS driver_id
       ,FROM_UNIXTIME(sa.create_time - 3600) AS create_time
       ,CASE 
            WHEN sa.assign_type = 1 then '1. Single Assign'
            WHEN sa.assign_type in (2,4) then '2. Multi Assign'
            WHEN sa.assign_type = 3 then '3. Well-Stack Assign'
            WHEN sa.assign_type = 5 then '4. Free Pick'
            WHEN sa.assign_type = 6 then '5. Manual'
            WHEN sa.assign_type in (7,8) then '6. New Stack Assign'
            ELSE NULL END AS assign_type
       ,CASE 
            WHEN sa.order_type = 200 then 'Group'
            ELSE 'Single' END AS order_type

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
AND DATE(FROM_UNIXTIME(sa.create_time - 3600)) BETWEEN current_date - interval '90' day AND current_date - interval '1' day
AND sa.status in (3,4)
) 
,raw as 
(SELECT 
         dot.uid AS shipper_id 
        ,dot.ref_order_code
        ,dot.ref_order_id
        ,dot.group_id
        ,CASE 
            WHEN dot.group_id > 0 AND sa.order_type != 'Group' THEN 'stack'
            WHEN dot.group_id > 0 AND sa.order_type  = 'Group' THEN 'group'
            ELSE 'single' END AS final_assign_type
        ,COALESCE(oct.restaurant_id,0) AS merchant_id 
        ,sa.assign_type
        ,sa.order_type
        ,dot.ref_order_category
        ,CASE WHEN dot.pick_city_id = 217 THEN 'HCM' WHEN dot.pick_city_id = 218 THEN 'HN' WHEN dot.pick_city_id = 219 THEN 'DN' ELSE 'OTH' END AS city_group
        ,DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date



FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot


LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_vn_order_completed_da WHERE date(dt) = current_date - interval '1' day ) oct 
    on oct.id = dot.ref_order_id
    and dot.ref_order_category = 0 

LEFT JOIN assignment sa 
    on sa.ref_order_id = dot.ref_order_id
    and sa.order_category = dot.ref_order_category

LEFT JOIN assignment sa_filter
    on  sa.ref_order_id = sa_filter.ref_order_id          
    and sa.order_category = sa_filter.order_category 
    and sa.create_time < sa_filter.create_time
    
WHERE 1=1
AND sa_filter.order_id is null
AND dot.order_status = 400
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day
)
-- select * from raw where group_id = 42824129
,summary AS 
(SELECT 
         a.group_id
        ,final_assign_type
        ,CASE WHEN ref_order_category = 0 THEN 'delivery' else 'spxi' end as source
        ,report_date
        ,city_group
        ,COUNT(DISTINCT merchant_id) AS total_mex
        ,COUNT(DISTINCT ref_order_id) AS total_order_in_group    


FROM raw a 


GROUP BY 1,2,3,4,5
)
SELECT 
        report_date
       ,city_group 
       ,final_assign_type AS assign_type
       ,source 
       ,CASE 
            WHEN source = 'delivery' AND group_id > 0 THEN total_mex ELSE 0 END AS mex_segment_by_merchant_id
       ,COUNT(CASE WHEN group_id > 0 THEN group_id ELSE NULL END) AS cnt_group_id
       ,SUM(total_order_in_group) AS cnt_order_id

FROM summary 

GROUP BY 1,2,3,4,5