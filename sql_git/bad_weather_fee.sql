
SELECT   
         CAST(JSON_EXTRACT(service_split,'$.collect_amount') AS BIGINT) AS amount
        ,JSON_EXTRACT(service_split,'$.code') AS code_
        ,COUNT(DISTINCT id) AS total_order 
FROM 
(SELECT id,CAST(JSON_EXTRACT(extra_data,'$.service_fees') AS ARRAY<JSON>) AS services,t.service_split

FROM shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live
CROSS JOIN UNNEST (CAST(JSON_EXTRACT(extra_data,'$.service_fees') AS ARRAY<JSON>)) AS t(service_split)
WHERE 1 = 1 
AND city_id = 218
AND DATE(FROM_UNIXTIME(final_delivered_time - 3600)) = current_date 
)

WHERE CAST(JSON_EXTRACT(service_split,'$.code') AS VARCHAR) LIKE  '%bad_weather_fee%'
GROUP BY 1,2
ORDER BY 1 DESC 