WITH check_shift AS 
(SELECT 
      uid AS shipper_id
     ,shipper_name
     ,city_name  
     ,hub_type_original
     ,COUNT(DISTINCT (slot_id,date_)) AS num_of_shift   
     ,SUM(total_order) AS total_order
     ,SUM(kpi) AS total_kpi 
     ,MAX(CASE WHEN total_order > 0 THEN date_ ELSE NULL END) AS last_delivered_date

from dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab

WHERE date_ BETWEEN DATE'2023-09-01' AND DATE'2023-09-30'
AND (total_order > 0 OR total_income > 0) 
GROUP BY 1,2,3,4    
)

SELECT 
        shipper_id,
        shipper_name,
        city_name,
        num_of_shift,
        total_kpi,
        total_order,
        500000 as bonus,
        500000 as total_adj,
        'HUB_MODEL_BONUS_ADJ_Thuong tai xe tieu bieu '||'- '||date_format(last_delivered_date,'%d/%m/%Y') AS note_
FROM 
(SELECT 
        *
        ,ROW_NUMBER()OVER(PARTITION BY shipper_id ORDER BY num_of_shift DESC) AS rank



FROM check_shift
)  
WHERE rank = 1 
AND hub_type_original = '3 hour shift'
-- AND shipper_id IN ()
ORDER BY 5 DESC