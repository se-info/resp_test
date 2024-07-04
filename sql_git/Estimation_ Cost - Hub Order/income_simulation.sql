WITH group_allocate AS 
(SELECT
        ogi.group_code
       ,ogi.id
       ,ogi.ref_order_category 
       ,(ogi.ship_fee/CAST(100 AS DOUBLE)) AS original_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_a') AS DOUBLE) AS rate_a 
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_b') AS DOUBLE) AS rate_b
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.unit_fee') AS DOUBLE) AS unit_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.surge_rate') AS DOUBLE) AS surge_rate
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee') AS DOUBLE) AS min_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) AS extra_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.re') AS DOUBLE) AS re_stack       
       ,(ogi.ship_fee/CAST(100 AS DOUBLE))/COUNT(DISTINCT ogm.ref_order_code) AS allocate_fee 
       ,COUNT(DISTINCT ogm.ref_order_code) AS total_order

        
FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE date(dt) = current_date - interval '1' day) ogi     

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da WHERE date(dt) = current_date - interval '1' day) ogm
    on ogm.group_id = ogi.id
    and ogi.ref_order_category = ogm.ref_order_category
    and ogm.mapping_status = 11

WHERE 1 = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,ogi.ship_fee
)
,metrics AS
(SELECT 
         dot.ref_order_id
        ,dot.ref_order_code 
        ,dot.group_id
        ,city.name_en AS city_name_full
        ,CASE 
              WHEN dot.pick_city_id = 217 then 'HCM'
              WHEN dot.pick_city_id = 218 then 'HN'
              WHEN dot.pick_city_id = 219 then 'DN'
              ELSE 'OTH' END AS city_name  
        ,dot.delivery_distance/CAST(1000 AS DOUBLE) AS distance
        ,dot.delivery_cost/CAST(100 AS DOUBLE) AS delivery_cost
        ,COALESCE(CAST(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') AS DOUBLE),
                  CAST(json_extract(dotet.order_data,'$.shopee.shipping_fee') AS DOUBLE)
                 ) as dotet_total_shipping_fee
        ,CAST(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') AS DOUBLE) as unit_fee
        ,CAST(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') AS DOUBLE) as min_fee
        ,CAST(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') AS DOUBLE) as surge_rate
        ,CAST(json_extract(dotet.order_data,'$.shipper_policy.type') AS DOUBLE) as driver_payment_policy
        ,CASE WHEN dot.group_id > 0 then ga.allocate_fee ELSE 0 END AS group_fee
        ,ga.original_fee AS group_fee_original
        ,dotet.order_data
        ,DATE(FROM_UNIXTIME(dot.submitted_time - 3600)) AS created_date
        ,dot.uid AS shipper_id
        ,dot.order_status

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da 
           where date(dt) = current_date - interval '1' day) dotet
    on dot.id = dotet.order_id

LEFT JOIN group_allocate ga 
    on ga.id = dot.group_id
    and ga.ref_order_category = dot.ref_order_category

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
    on city.id = dot.pick_city_id
    and city.country_id = 86

WHERE 1 =1 
AND DATE(FROM_UNIXTIME(dot.submitted_time - 3600)) between current_date - interval '22' day and current_date - interval '1' day
AND dot.pick_city_id not in (217,218)
AND dot.order_status in (400,401,402,405,406,407)
)
,final_metrics AS 
(SELECT 
      m.* 
     ,GREATEST((case 
                     when city_name in ('HCM','HN') and min_fee <= 13500 then 12500 
                     when city_name in ('DN','OTH') and min_fee <= 13500 then 12000
                     else min_fee end),
        (case 
            when city_name in ('HCM','HN') and unit_fee >= 3750 then 3750 
            when city_name in ('DN','OTH') and unit_fee >= 3750 then 3750
            else unit_fee end)
        *distance*surge_rate) AS shipping_fee_opt1

     ,GREATEST((case 
                     when city_name in ('HCM','HN') and min_fee <= 13500 then 12500 
                     when city_name in ('DN','OTH') and min_fee <= 13500 then 11650
                     else min_fee end),
        (case 
            when city_name in ('HCM','HN') and unit_fee >= 3750 then 3750 
            when city_name in ('DN','OTH') and unit_fee >= 3750 then 3750
            else unit_fee end)
        *distance*surge_rate) as shipping_fee_opt2


FROM metrics m)
SELECT  
        created_date
       ,shipper_id
       ,city_name_full
       ,city_name
       ,COUNT(DISTINCT ref_order_code) AS total_delivered
       ,SUM(
            CASE WHEN group_id = 0 then delivery_cost
                 WHEN group_id > 0 then group_fee
                 ELSE 0 END) AS current_shipping_fee 
       ,SUM(
            CASE WHEN group_id = 0 then shipping_fee_opt1
                 WHEN group_id > 0 then group_fee
                 ELSE 0 END) AS opt1_shipping_fee 
       ,SUM(
            CASE WHEN group_id = 0 then shipping_fee_opt2
                 WHEN group_id > 0 then group_fee
                 ELSE 0 END) AS opt2_shipping_fee                  


FROM final_metrics


GROUP BY 1,2,3,4