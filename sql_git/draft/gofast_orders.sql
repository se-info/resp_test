SELECT 
    all.created_date
    , all.city_name 
    , all.city_group
    , CAST(COUNT(DISTINCT CASE WHEN all.order_status = 'Delivered' THEN all.uid ELSE NULL END) AS DOUBLE) AS total_delivered_orders
    , CAST(COUNT(DISTINCT CASE WHEN all.order_status = 'Delivered' AND mode.peak_mode_name = 'Peak 3 Mode' AND all.source = 'order_delivery' THEN all.uid ELSE NULL END) AS DOUBLE) AS total_delivered_peak3_orders
    , CAST(COUNT(DISTINCT all.uid) AS DOUBLE) AS total_orders


FROM
    (-- ********** order_delivery: Food/Market
    SELECT 
        oct.id
        , CONCAT('order_delivery_',CAST(oct.id AS VARCHAR)) AS uid
        , 'order_delivery' AS source
        , oct.shipper_uid AS shipper_id
        , FROM_UNIXTIME(oct.submit_time - 60*60) AS created_timestamp
        , DATE(FROM_UNIXTIME(oct.submit_time - 60*60)) AS created_date
        , CASE 
            WHEN oct.status = 7 Then 'Delivered'
            WHEN oct.status = 8 THEN 'Cancelled'
            WHEN oct.status = 9 THEN 'Quit' END AS order_status
        , CASE WHEN oct.foody_service_id = 1 THEN 'Food'
            -- WHEN oct.foody_service_id = 3 THEN 'Laundy'
            -- WHEN oct.foody_service_id = 4 THEN 'Products'
            -- WHEN oct.foody_service_id = 5 THEN 'Fresh'
            -- WHEN oct.foody_service_id = 6 THEN 'Flowers'
            -- WHEN oct.foody_service_id = 7 THEN 'Medicine'
            -- WHEN oct.foody_service_id = 12 THEN 'Pets'
            -- WHEN oct.foody_service_id = 13 THEN 'Liquor'
            -- WHEN oct.foody_service_id = 15 THEN 'Salon'
            ELSE 'Market' END AS foody_service
        , oct.city_id
        , oct.district_id
        , CASE WHEN oct.city_id = 238 THEN 'Dien Bien' ELSE city.name_en END AS city_name
        , CASE 
            WHEN oct.city_id = 217 THEN 'HCM'
            WHEN oct.city_id = 218 THEN 'HN'
            WHEN oct.city_id = 219 THEN 'DN'
            WHEN oct.city_id = 220 THEN 'Hai Phong'
            WHEN oct.city_id = 221 THEN 'Can Tho'
            WHEN oct.city_id = 222 THEN 'Dong Nai'
            WHEN oct.city_id = 223 THEN 'Vung Tau'
            WHEN oct.city_id = 230 THEN 'Binh Duong'
            WHEN oct.city_id = 273 THEN 'Hue'
        ELSE 'OTH' END AS city_group
    
    FROM shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
      
        -- location
        LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city ON city.id = oct.city_id AND city.country_id = 86
        
    --    left join (SELECT district_id
      --              ,district_name
                    
        --            from shopeefood.foody_mart__fact_gross_order_join_detail
          --          where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP))
                    
            --        GROUP BY district_id
              --              ,district_name
                --    )district on district.district_id = oct.district_id
    
    UNION ALL
    
    --************** Now Ship/NSS
    SELECT 
        ns.id
        , ns.uid
        , CASE 
            WHEN ns.booking_type = 1 THEN 'Now Ship Moto'
            WHEN ns.booking_type = 2 THEN 'Now Ship'
            WHEN ns.booking_type = 3 THEN 'Now Ship'
            WHEN ns.booking_type = 4 THEN 'Now Ship Shopee'
            ELSE NULL END AS source
        , ns.shipper_id
        , FROM_UNIXTIME(ns.create_time - 60*60) AS created_timestamp
        , DATE(from_unixtime(ns.create_time - 60*60)) AS created_date
        , CASE 
            WHEN ns.status = 11 THEN 'Delivered'
            WHEN ns.status in (6,9,12) THEN 'Cancelled'
            ELSE 'Others' END AS order_status
        , CASE WHEN ns.booking_type = 1 THEN 'Now Ship Moto'
            WHEN ns.booking_type = 2 THEN 'Now Ship'
            WHEN ns.booking_type = 3 THEN 'Now Ship'
            WHEN ns.booking_type = 4 THEN 'Now Ship Shopee'
            ELSE NULL END AS foody_service
        , ns.city_id
        , ns.district_id
        , CASE WHEN ns.city_id = 238 THEN 'Dien Bien' ELSE city.name_en END AS city_name
        , CASE 
            WHEN ns.city_id = 217 THEN 'HCM'
            WHEN ns.city_id = 218 THEN 'HN'
            WHEN ns.city_id = 219 THEN 'DN'
            WHEN ns.city_id = 220 THEN 'Hai Phong'
            WHEN ns.city_id = 221 THEN 'Can Tho'
            WHEN ns.city_id = 222 THEN 'Dong Nai'
            WHEN ns.city_id = 223 THEN 'Vung Tau'
            WHEN ns.city_id = 230 THEN 'Binh Duong'
            WHEN ns.city_id = 273 THEN 'Hue'
        ELSE 'OTH' END AS city_group
        
        FROM
            (SELECT id,concat('now_ship_',cast(id AS VARCHAR)) AS uid, booking_type,shipper_id, distance,create_time, status, payment_method,'now_ship' AS original_source,city_id,cast(json_extract(extra_data,'$.pick_address_info.district_id') AS DOUBLE) AS district_id
            
            FROM shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live
            
            UNION ALL
            
            SELECT id,concat('now_ship_shopee_',cast(id AS VARCHAR)) AS uid, 4 AS booking_type, shipper_id,distance,create_time,status,1 AS payment_method,'now_ship_shopee' AS original_source,city_id,cast(json_extract(extra_data,'$.sender_info.district_id') AS DOUBLE) AS district_id
            
            FROM shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live
            
            ) ns
            
            -- location
            LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city ON city.id = ns.city_id AND city.country_id = 86
            
         --   left join (SELECT district_id
           --             ,district_name
                        
             --           from shopeefood.foody_mart__fact_gross_order_join_detail
               --         where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP))
                        
                 --       GROUP BY district_id
                   --             ,district_name
                     --   )district on district.district_id = ns.district_id
    
    ) all
    
    LEFT JOIN
        (SELECT 
            pm.city_id
            , pm.district_id
            , pm.mode_id
            , from_unixtime(pm.start_time - 60*60) AS start_time
            , from_unixtime(pm.start_time + pm.running_time - 60*60) AS end_time
            , pm.available_driver
            , pm.assigning_order
            , pm.driver_availability
            , pm_name.name AS peak_mode_name
            
            FROM shopeefood.foody_delivery_admin_db__peak_mode_export_activity_tab__reg_daily_s0_live pm 
            LEFT JOIN shopeefood.foody_delivery_admin_db__peak_mode_tab__reg_daily_s0_live pm_name ON pm_name.id = pm.mode_id
            WHERE pm.mode_id in (7,8,9,10,11)
        ) mode ON mode.city_id = all.city_id AND mode.district_id = all.district_id AND all.created_timestamp >= mode.start_time AND all.created_timestamp < mode.end_time

WHERE all.created_date BETWEEN current_date - interval '90' day AND current_date - interval '1' day


GROUP BY 1,2,3