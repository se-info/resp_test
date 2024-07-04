SELECT 
         dot.ref_order_category
        ,dot.ref_order_code
        ,DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
        ,FROM_UNIXTIME(dot.real_drop_time - 3600) AS delivered_timestamp
        ,FROM_UNIXTIME(dot.submitted_time - 3600) AS created_timestamp
        ,dot.uid AS shipper_id 
        ,sm.shipper_name
        ,sm.city_name
        ,dot.ref_order_id
        ,'non-hub' AS type
        ,(dot.delivery_cost/CAST(100 AS DOUBLE)) AS shipping_fee_system
        ,13500 AS expected_fee
        ,'ADJUSTMENT_SHIPPING FEE_PT16_Food_'||CAST(dot.ref_order_id AS VARCHAR)||'_'||DATE_FORMAT(DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)),'%d/%m/%Y') AS noted
        ,dot.pick_city_id
        ,doet.driver_policy

FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE date(dt) = current_date - interval '1' day)dot 

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = dot.uid 
    and try_cast(sm.grass_date as date) = DATE(FROM_UNIXTIME(real_drop_time - 3600))

LEFT JOIN (select 
                  order_id 
                  ,CAST(json_extract_scalar(order_data,'$.shipper_policy.type') AS BIGINT) AS driver_policy
            from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da 
            where date(dt) = current_date - interval '1' day) doet 
    on doet.order_id = dot.id 


WHERE 1 = 1 
AND DATE(FROM_UNIXTIME(real_drop_time - 3600)) between current_date - interval '1' day and current_date - interval '1' day
AND pick_city_id in (217,218)
AND (delivery_cost/CAST(100 AS DOUBLE)) < 13500
AND doet.driver_policy != 2 