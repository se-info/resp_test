WITH raw AS
(SELECT 
       group_id 
      ,group_code
      ,ref_order_category
    --   ,ARRAY_JOIN(ARRAY_AGG(delivery_id),'_') AS group_id_waybill
      ,MAX(report_date) AS report_date
      ,MAX(extra_fee) AS extra_fee_config
      ,CASE WHEN MAX(rank_order) > 1 THEN MAX(extra_fee) * 2 * (MAX(rank_order) - 1) ELSE 0 END AS extra_fee_old_logic
      ,CASE 
            WHEN MAX(rank_order) > 1 THEN 
            ROUND(GREATEST(
                MAX(min_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate)
            ))  ELSE MAX(final_stack_fee) END AS group_fee_old_logic
      ,CASE 
            WHEN MAX(rank_order) > 1 THEN 
                 ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  )) ELSE MAX(final_stack_fee) END AS group_fee_new_logic                  

      ,MAX(min_fee) AS min_group
      ,MAX(unit_fee) * MAX(group_distance)*MAX(surge_rate) AS total_group                  
      ,MAX(rank_order) AS total_order_in_group
      ,SUM(single_fee) AS sum_single_fee
      ,MAX_BY(driver_policy,ref_order_id) AS driver_policy
      ,ARRAY_AGG(DISTINCT ref_order_id) AS order_ext
      
      ,CARDINALITY(ARRAY_AGG(pick_latitude)) AS count_pick_lat
      ,CARDINALITY(ARRAY_AGG(DISTINCT pick_latitude)) AS count_pick_lat_unique
      ,CARDINALITY(ARRAY_AGG(pick_longitude)) AS count_pick_long
      ,CARDINALITY(ARRAY_AGG(DISTINCT pick_longitude)) AS count_pick_long_unique

      ,CARDINALITY(ARRAY_AGG(drop_latitude)) AS count_drop_lat
      ,CARDINALITY(ARRAY_AGG(DISTINCT drop_latitude)) AS count_drop_lat_unique
      ,CARDINALITY(ARRAY_AGG(drop_longitude)) AS count_drop_long
      ,CARDINALITY(ARRAY_AGG(DISTINCT drop_longitude)) AS count_drop_long_unique

FROM dev_vnfdbi_opsndrivers.group_order_info_raw
-- WHERE driver_policy != 2
GROUP BY 1,2,3
)
,final AS 
(SELECT
       report_date
      ,group_code
      ,driver_policy
      ,ref_order_category
      ,total_order_in_group
      ,group_fee_old_logic
      ,extra_fee_old_logic
      ,group_fee_new_logic
      ,CASE 
            WHEN count_pick_lat_unique = count_pick_long_unique AND count_pick_lat_unique = 1 THEN 'same_pick'
            WHEN count_pick_lat_unique = count_pick_long_unique AND count_pick_lat_unique > 1 THEN 'diff_pick'
            WHEN count_pick_lat_unique != count_pick_long_unique THEN 'diff_pick' END AS pick_route
      ,CASE 
            WHEN count_drop_lat_unique = count_drop_long_unique AND count_drop_lat_unique = 1 THEN 'same_drop'
            WHEN count_drop_lat_unique = count_drop_long_unique AND count_drop_lat_unique > 1 THEN 'diff_drop' 
            WHEN count_drop_lat_unique != count_drop_long_unique THEN 'diff_drop' END AS drop_route            
      ,CASE 
            WHEN total_order_in_group > 1 THEN
                  (CASE 
                        WHEN count_pick_lat_unique = count_pick_long_unique THEN IF(ref_order_category = 0, 1000 , 500  ) * (count_pick_lat_unique - 1)
                        WHEN count_pick_lat_unique != count_pick_long_unique THEN IF(ref_order_category = 0, 1000 , 500  ) *(total_order_in_group -1)
                        END) +
                  (CASE 
                        WHEN count_drop_lat_unique = count_drop_long_unique THEN IF(ref_order_category = 0, 1000 , 500  ) * (count_drop_lat_unique - 1)
                        WHEN count_drop_lat_unique != count_drop_long_unique THEN IF(ref_order_category = 0, 1000 , 500  ) * (total_order_in_group -1)
                        END) ELSE 0 END AS extra_fee_new_logic 
                       
FROM raw
WHERE 1 = 1 
AND report_date between date'2023-05-19' and current_date - interval '1' day
-- AND ref_order_category = 6
)
SELECT 
         f.report_date
        ,f.ref_order_category
        ,CASE WHEN driver_policy = 2 THEN 'Hub' ELSE 'Non Hub' END AS driver_type
        -- ,total_order_in_group
        ,pick_route
        ,drop_route
        ,COUNT(DISTINCT f.group_code) AS cnt_group
        ,SUM(f.total_order_in_group) AS cnt_order 
        ,SUM(group_fee_old_logic)/CAST(ex.exchange_rate AS DOUBLE) AS group_fee_old_logic
        ,SUM(group_fee_new_logic)/CAST(ex.exchange_rate AS DOUBLE) AS group_fee_new_logic
        ,SUM(extra_fee_old_logic)/CAST(ex.exchange_rate AS DOUBLE) AS extra_fee_old_logic 
        ,SUM(extra_fee_new_logic)/CAST(ex.exchange_rate AS DOUBLE) AS extra_fee_new_logic
        ,CASE WHEN driver_policy = 2 THEN 0 ELSE SUM(group_fee_old_logic-group_fee_new_logic)/CAST(ex.exchange_rate AS DOUBLE) END AS saving_min_check
        ,CASE WHEN driver_policy = 2 THEN 0 ELSE SUM(extra_fee_old_logic-extra_fee_new_logic)/CAST(ex.exchange_rate AS DOUBLE) END AS saving_extra_check
        ,ex.exchange_rate   

FROM final f 

LEFT JOIN mp_order.dim_exchange_rate__reg_s0_live ex 
    on ex.grass_date = f.report_date
    and ex.currency = 'VND'

WHERE total_order_in_group > 1

GROUP BY 1,2,3,4,5,ex.exchange_rate,driver_policy
;
