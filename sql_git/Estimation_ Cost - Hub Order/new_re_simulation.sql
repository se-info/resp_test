WITH group_cal AS 
(SELECT 
         group_id 
        ,group_code
        ,group_category
        ,MAX(final_stack_fee) AS current_group_fee
        ,MAX(final_stack_fee)/MAX(rank_order) AS group_fee_allocate_current 
        ,MAX(rank_order) AS total_order_in_group
        ,CEILING(MAX(re_stack) * POWER(10, 1)) / POWER(10, 1) AS system_re
        ,CEILING((SUM(single_distance)/CAST(MAX(group_distance) AS DOUBLE)) * POWER(10, 1)) / POWER(10, 1) AS expect_re
        -- ,ROUND(MAX(re_stack),2) AS system_re
        -- ,ROUND(SUM(single_distance)/CAST(MAX(group_distance) AS DOUBLE),2) AS expect_re
        ,SUM(single_distance)/CAST(MAX(group_distance)AS DOUBLE) AS expect_re_original
        ,MAX(re_stack) AS system_re_original
        ,MAX(group_distance) AS group_distance
        ,SUM(single_fee) AS sum_single_fee

FROM dev_vnfdbi_opsndrivers.group_order_info_raw 

GROUP BY 1,2,3
)  
,raw AS
(SELECT 
         raw.id
        ,raw.order_code
        ,dot.group_id
        ,raw.source
        ,CASE 
              WHEN dot.group_id > 0 AND order_assign_type != 'Group' THEN 2
              WHEN dot.group_id > 0 AND order_assign_type = 'Group' THEN 1 
              ELSE 0 END AS is_stack_group
        ,raw.assign_type
        ,raw.order_assign_type
        ,raw.order_type
        ,raw.distance
        ,raw.created_date

FROM dev_vnfdbi_opsndrivers.phong_raw_order_v2 raw

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day ) dot 
    on dot.ref_order_id = raw.id 
    and dot.ref_order_category = raw.order_type 




-- LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet
--     on dotet.order_id = dot.id

WHERE 1 = 1 
AND raw.order_status = 'Delivered'
AND raw.created_date between date'2023-05-29' and current_date - interval '1' day

)

,summary AS
(SELECT 
        raw.created_date
       ,raw.group_id
       ,raw.source
       ,CASE WHEN expect_re_original < 1.05 THEN 1 ELSE 0 END AS is_impacted
       ,gi.group_code 
       ,gi.system_re 
       ,gi.expect_re
       ,gi.group_distance
       ,gi.current_group_fee
       ,gi.total_order_in_group
       ,gi.sum_single_fee
       ,gi.system_re_original
       ,gi.expect_re_original
       ,CASE WHEN MAX_BY(is_stack_group,raw.id) = 2 THEN 'stack' WHEN MAX_BY(is_stack_group,raw.id) = 1 THEN 'group' ELSE 'single' END AS assign_order_type 
       ,SUM(raw.distance) AS single_distance
       ,COUNT(DISTINCT order_code) AS total_order_in_group_v2
       ,ARRAY_AGG(order_code) AS ext_info



FROM raw

LEFT JOIN group_cal gi 
     on gi.group_id = raw.group_id
     and gi.group_category = raw.order_type

WHERE raw.group_id > 0
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
SELECT * FROM
(SELECT 
             created_date
            ,source
            ,assign_order_type
            ,system_re
            ,expect_re
            ,total_order_in_group
            ,SUM(total_order_in_group) AS cnt_order
            ,COUNT(DISTINCT group_id) AS cnt_group
            ,SUM(group_distance) AS sum_group_distance
            ,SUM(single_distance) AS sum_single_distance
            ,SUM(current_group_fee) AS current_group_fee
            ,SUM(CASE WHEN is_impacted = 0 THEN current_group_fee ELSE NULL END) new_group_fee
            ,SUM(sum_single_fee) AS sum_single_fee
            ,SUM(system_re_original)/CAST(COUNT(DISTINCT group_id) AS DOUBLE) AS avg_system_re
            ,SUM(expect_re_original)/CAST(COUNT(DISTINCT group_id) AS DOUBLE) AS avg_expect_re


FROM summary

WHERE total_order_in_group = total_order_in_group_v2

GROUP BY 1,2,3,4,5,6)