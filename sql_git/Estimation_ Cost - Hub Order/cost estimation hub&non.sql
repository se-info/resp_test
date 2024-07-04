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
    and ogm.mapping_status in (11,23,26)


WHERE 1 = 1
                        
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,ogi.ship_fee
) 
,raw AS
(SELECT 
       dot.uid AS shipper_id
      ,dot.group_id  
      ,dot.ref_order_code
      ,dot.ref_order_id
      ,CASE
           WHEN dot.ref_order_category = 0 THEN '1. Delivery'
           ELSE '2. SPXI' END AS source
      ,city.name_en AS city_name
      ,dot.delivery_distance/CAST(1000 AS DOUBLE) AS distance
      ,dot.delivery_cost/CAST(100 AS DOUBLE) AS delivery_cost
      ,CASE
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 1 THEN '5 hour shift'
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 2 THEN '8 hour shift'
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 3 THEN '10 hour shift'
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 4 THEN '3 hour shift'
             ELSE 'Non Hub' end as hub_type_v1
      ,hi.hub_type_x_start_time AS hub_type_v2                        
      ,fa.create_time AS inflow_timestamp 
      ,DATE(fa.create_time) AS inflow_date
      ,DATE(FROM_UNIXTIME(dot.real_drop_time)) AS report_date
      ,ROW_NUMBER()OVER(PARTITION BY dot.uid,DATE(fa.create_time),hi.hub_type_x_start_time ORDER BY fa.create_time ASC) AS rank_order
      ,CASE WHEN cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_hub
      ,CAST(json_extract(doet.order_data,'$.delivery.shipping_fee.total') as double) as dotet_total_shipping_fee
      ,CAST(json_extract(doet.order_data,'$.shopee.shipping_fee_info.return_fee') as double) as return_fee
      ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
      ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
      ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate
      ,hi.kpi AS is_qualified_kpi
      ,CASE WHEN dot.group_id > 0 then ga.allocate_fee ELSE 0 END AS group_fee

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet 
     on dot.id = doet.order_id

LEFT JOIN (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid,order_id, order_type , from_unixtime(create_time - 3600) as create_time , update_time, status,shipper_uid
          FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
          WHERE status in (3,4)
     UNION
          SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid,order_id, order_type , from_unixtime(create_time - 3600) as create_time , update_time, status,shipper_uid
          FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
          WHERE status in (3,4) 
          )fa 
    on dot.ref_order_id = fa.order_id 
    and dot.ref_order_category = fa.order_type
    and dot.uid = fa.shipper_uid

         
LEFT JOIN (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid,order_id, order_type , from_unixtime(create_time - 3600) as create_time , update_time, status,shipper_uid
          FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
          WHERE status in (3,4)
     UNION
          SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid,order_id, order_type , from_unixtime(create_time - 3600) as create_time , update_time, status,shipper_uid
          FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
          WHERE status in (3,4)
          )a_filter 
     on fa.order_uid = a_filter.order_uid 
     and fa.create_time < a_filter.create_time

LEFT JOIN dev_vnfdbi_opsndrivers.phong_hub_driver_metrics hi 
     on hi.uid = dot.uid 
     and hi.date_ = DATE(fa.create_time)
     and fa.create_time between hi.start_shift_time and hi.end_shift_time
     and hi.total_order > 0

           
LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
     on city.id = dot.pick_city_id 
     and city.country_id = 86

LEFT JOIN group_allocate ga 
    on ga.id = dot.group_id
    and ga.ref_order_category = dot.ref_order_category

WHERE 1 = 1 
AND a_filter.order_id is null
                                    
AND dot.order_status in (400,401,402,405,406,407)
AND dot.ref_order_category = 0
                                                                                
AND DATE(FROM_UNIXTIME(dot.real_drop_time)) between date'2023-03-01' and date'2023-03-31'
                         
)
,final_metrics AS
(SELECT 
        raw.report_date
       ,raw.inflow_timestamp
       ,raw.rank_order
       ,raw.shipper_id
       ,raw.ref_order_code
       ,raw.group_id
       ,raw.source
       ,raw.is_qualified_kpi
       ,raw.hub_type_v2
       ,raw.city_name
       ,raw.distance
       ,raw.is_hub
       ,raw.group_fee

       ,CASE WHEN is_hub = 1 then 13500
             WHEN is_hub = 0 then GREATEST(13500,unit_fee*distance*surge_rate)
             END AS current_ship_shared

       ,CASE WHEN is_hub = 1 then 13500
             WHEN is_hub = 0 AND city_name NOT IN ('HCM City','Ha Noi City') THEN GREATEST(12000,(3750*distance*surge_rate))
             WHEN is_hub = 0 AND city_name IN ('HCM City','Ha Noi City') THEN GREATEST(13500,3750*distance*surge_rate)
             END AS opt1                                     

       ,CASE WHEN is_hub = 1 AND city_name != 'Hai Phong City' THEN 12500
             WHEN is_hub = 1 AND city_name = 'Hai Phong City' THEN 12000
             WHEN is_hub = 0 AND city_name NOT IN ('HCM City','Ha Noi City') THEN GREATEST(12000,(3750*distance*surge_rate))
             WHEN is_hub = 0 AND city_name IN ('HCM City','Ha Noi City') THEN GREATEST(12500,(3750*distance*surge_rate)) 
             END AS opt2
         
        ,CASE 
            WHEN raw.distance <= 2 then '1. 0 - 2km'
            WHEN raw.distance < 4 then '2. 2 - 4km'
            WHEN raw.distance >= 4 then '5. >= 4km'
            END AS distance_range
       ,CASE 
             WHEN hub_type_v1 = '10 hour shift' and rank_order > 30 then 6000
             
             WHEN hub_type_v1 = '8 hour shift' and rank_order between 26 and 30 then 4000
             WHEN hub_type_v1 = '8 hour shift' and rank_order > 30 then 6000

             WHEN hub_type_v1 = '5 hour shift' and rank_order between 14 and 24 then 4000
             WHEN hub_type_v1 = '5 hour shift' and rank_order > 24 then 6000             

             WHEN hub_type_v1 = '3 hour shift' and rank_order between 7 and 14 then 2000
             WHEN hub_type_v1 = '3 hour shift' and rank_order > 14 then 3000                             
             
             ELSE 0 END AS current_bonus
       ,1000 bonus_opt2_opt3

from raw
)
,summary AS 
(SELECT 
       report_date 
      ,shipper_id
      ,city_name AS city_name_full
      ,CASE
            WHEN city_name in ('HCM City','Ha Noi City','Da Nang City') then city_name
            ELSE 'Other' END AS city_name      
      ,is_hub
      ,COALESCE(hub_type_v2,'Non Hub') AS driver_type
      ,COUNT(DISTINCT ref_order_code) AS total_order
      ,COUNT(DISTINCT CASE WHEN is_hub = 1 THEN ref_order_code ELSE NULL END) AS total_order_hub
      ,SUM(CASE 
               WHEN is_hub = 1 THEN current_ship_shared
               WHEN is_hub = 0 AND group_id = 0 then current_ship_shared
               WHEN is_hub = 0 AND group_id > 0 then group_fee ELSE 0 END) AS current_base
      ,SUM(CASE WHEN is_hub = 1 then opt1 ELSE 0 END) AS opt1_hub
      ,SUM(CASE WHEN is_hub = 1 then opt2 ELSE 0 END) AS opt2_hub

      ,SUM(CASE
               WHEN is_hub = 0 AND group_id = 0 then opt1
               WHEN is_hub = 0 AND group_id > 0 then group_fee ELSE 0 END) AS opt1_non_hub
      ,SUM(CASE
               WHEN is_hub = 0 AND group_id = 0 then opt2
               WHEN is_hub = 0 AND group_id > 0 then group_fee ELSE 0 END) AS opt2_non_hub
                     


FROM final_metrics

GROUP BY 1,2,3,4,5,6
)
SELECT
        YEAR(s.report_date)*100 + MONTH(s.report_date) AS year_month
       ,s.city_name
       ,COUNT(DISTINCT CASE WHEN s.driver_type = 'Non Hub' THEN shipper_id 
                            WHEN s.driver_type != 'Non Hub' AND total_order_hub <= 0 THEN shipper_id END) AS non_hub_active
       ,COUNT(DISTINCT CASE WHEN s.driver_type != 'Non Hub' AND total_order_hub > 0 THEN shipper_id END) AS hub_active 
       ,SUM(total_order) AS ado 
       ,COALESCE(SUM(total_order_hub),0) AS ado_hub
       ,SUM(CASE WHEN is_hub = 1 THEN current_base ELSE 0 END)/CAST(fx.exchange_rate AS DOUBLE) AS shipping_current_hub
       ,SUM(CASE WHEN is_hub = 0 THEN current_base ELSE 0 END)/CAST(fx.exchange_rate AS DOUBLE) AS shipping_current_non_hub
       ,SUM(CASE WHEN is_hub = 1 THEN opt1_hub ELSE 0 END)/CAST(fx.exchange_rate AS DOUBLE) AS shipping_hub_opt1
       ,SUM(CASE WHEN is_hub = 1 THEN opt2_hub ELSE 0 END)/CAST(fx.exchange_rate AS DOUBLE) AS shipping_hub_opt2
       ,SUM(CASE WHEN opt1_non_hub > 0 THEN opt1_non_hub ELSE 0 END)/CAST(fx.exchange_rate AS DOUBLE) AS shipping_non_hub_opt1
       ,SUM(CASE WHEN opt2_non_hub > 0 THEN opt2_non_hub ELSE 0 END)/CAST(fx.exchange_rate AS DOUBLE) AS shipping_non_hub_opt2



FROM summary s 

LEFT JOIN mp_order.dim_exchange_rate__reg_s0_live fx 
    on fx.grass_date = s.report_date
    and fx.currency = 'VND'

GROUP BY 1,2,fx.exchange_rate