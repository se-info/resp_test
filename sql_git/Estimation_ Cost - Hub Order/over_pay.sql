with raw as 
(select              
         DATE(FROM_UNIXTIME(ogm.create_time - 3600)) AS report_date
        ,ogm.group_id
        ,ogi.group_code
        ,city.name_en as city_name         
        ,ogm.ref_order_category
        ,ogi.ship_fee/cast(100 as double) as group_fee
        ,SUM(CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE)) AS extra_fee
        ,SUM(CAST(json_extract(ogm.extra_data,'$.ship_fee_info.driver_ship_fee') AS BIGINT)) as single_fee
        ,((ogi.ship_fee/cast(100 as double)) - (CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) *CARDINALITY(array_agg(ogm.ref_order_id)))) - SUM(CAST(json_extract(ogm.extra_data,'$.ship_fee_info.driver_ship_fee') AS BIGINT)) as gap                                     
        ,array_agg(ogm.ref_order_id) as order_success_ext
        ,CARDINALITY(array_agg(ogm.ref_order_id)) as total_order_success
        ,array_agg(di.name_en) as district_ext
                                                                                                                                                                                        
    from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm 

    left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi 
        on ogi.id = ogm.group_id
    
    left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
        on dot.ref_order_id = ogm.ref_order_id
        and dot.ref_order_category = ogm.ref_order_category        
    
    left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86
    
    left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = dot.pick_district_id        
    
    WHERE DATE(FROM_UNIXTIME(ogm.create_time - 3600)) BETWEEN current_date - interval '1' day AND current_date
    AND dot.order_status in (400,401,402,403,404,405,406,407) 
    GROUP BY 1,2,3,4,5,6,ogi.ship_fee,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE)
)
SELECT 
        report_date
       ,city_name 
       ,COUNT(DISTINCT group_id) AS total_order_group 
       ,SUM(total_order_success) AS total_order_in_group 
       ,COUNT(DISTINCT CASE WHEN gap > 0 then group_id ELSE NULL END) AS total_group_over_pay
       ,SUM(CASE WHEN gap > 0 then total_order_success ELSE NULL END) AS total_order_in_group_over_pay
       ,COUNT(DISTINCT CASE WHEN gap > 0 then group_id ELSE NULL END)/CAST(COUNT(DISTINCT group_id) AS DOUBLE) AS pct_over_pay
       ,SUM(CASE WHEN gap > 0 THEN gap ELSE 0 END) AS vnd_over_pay        

GROUP BY 1,2

UNION

SELECT 
        report_date
       ,'All' AS city_name 
       ,COUNT(DISTINCT group_id) AS total_order_group 
       ,SUM(total_order_success) AS total_order_in_group 
       ,COUNT(DISTINCT CASE WHEN gap > 0 then group_id ELSE NULL END) AS total_group_over_pay
       ,SUM(CASE WHEN gap > 0 then total_order_success ELSE NULL END) AS total_order_in_group_over_pay
       ,COUNT(DISTINCT CASE WHEN gap > 0 then group_id ELSE NULL END)/CAST(COUNT(DISTINCT group_id) AS DOUBLE) AS pct_over_pay
       ,SUM(CASE WHEN gap > 0 THEN gap ELSE 0 END) AS vnd_over_pay  

FROM raw        
GROUP BY 1,2