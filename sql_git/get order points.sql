SELECT   
                 oct.id
                ,oct.shipper_uid as shipper_id
                ,date(from_unixtime(oct.submit_time - 3600)) as created_date
                ,oct.submit_time
                ,case when oct.status = 7 then 'Delivered'
                    when oct.status = 8 then 'Cancelled'
                    when oct.status = 9 then 'Quit' end as order_status
                ,city.name_en AS city_name
                ,oct.foody_service_id
                ,oct.distance
                ,oct.restaurant_id
                ,dt.name_en as district_name
                ,poi.point

        from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
        left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86
        left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live dt on dt.id = oct.district_id 
        left join shopeefood.foody_partner_db__order_point_log_tab__reg_daily_s0_live poi on poi.order_id = oct.id and poi.order_type = 0
        WHERE 1=1

        and date(from_unixtime(oct.submit_time - 3600)) = current_date - interval '1' day 
        and oct.city_id <> 238  
        and oct.restaurant_id in 
            (1037991
            ,1114973
            ,47294
            ,47293
            ,972021
            ,120212
            ,1114974
            ,196972
            ,91266
            ,297026
            ,133342
            ,647143
            ,871083
            ,300450
            ,247390
            ,645116
            ,1127746
            ,198605
            ,632238
            ,977745
            ,704702
            ,908398
            ,950380
            ,972974
            ,1132427
            ,1141726)
        and poi.point < 20    
