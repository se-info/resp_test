select create_date 
      , orgin_diff_type_gg 
      , origin_diff_gg
      , origin_distance_group
      , google_distance_group
      , count(distinct order_id) cnt_order 
      , sum(geo_diff_distance) geo_diff_distance
      , sum(amt_diff_from_geo_simulate) sum_amt_diff_from_geo_simulate

from 
(
select create_date 
     , case when google_distance != origin_distance then 1 else 0 end as is_diff_from_google 
     , case when geo_distance != origin_distance then 1 else 0 end as is_diff_from_geo 
     , case when google_distance > origin_distance then '1. google higher' 
            when google_distance < origin_distance then '2. google lower' 
            else '3. same' end as google_diff_type 
     , case when geo_distance > origin_distance then '1. geo higher' 
            when geo_distance < origin_distance then '2. geo lower' 
            else '3. same' end as geo_diff_type  
     , case when geo_distance > google_distance then '1. geo higher' 
            when geo_distance < google_distance then '2. geo lower' 
            else '3. same' end as geo_diff_type_gg  
     , case when origin_distance > google_distance then '1. origin higher' 
            when origin_distance < google_distance then '2. origin lower' 
            else '3. same' end as orgin_diff_type_gg  
     , case when abs(google_distance - origin_distance) = 0 then '0. same'
            when abs(google_distance - origin_distance) <= 1 then '1. <= 1km'
            when abs(google_distance - origin_distance) <= 3 then '2. 1-3km'
            when abs(google_distance - origin_distance) <= 5 then '3. 3-5km'
            when abs(google_distance - origin_distance) <= 10 then '4. 5-10km'
            else '5. 10km++' end as google_diff_group 
     , case when abs(geo_distance - origin_distance) = 0 then '0. same'
            when abs(geo_distance - origin_distance) <= 0.1 then '1. 0 - 100'
            when abs(geo_distance - origin_distance) <= 0.5 then '2. 100 - 500'
            when abs(geo_distance - origin_distance) <= 1 then '3. 500 - 1000'
            when abs(geo_distance - origin_distance) <= 5 then '4. 1000 - 2000'
            when abs(geo_distance - origin_distance) <= 10 then '5. 2000 - 5000'
            when abs(geo_distance - origin_distance) <= 10 then '6. 5000 - 10000'
            when abs(geo_distance - origin_distance) > 10 then '7. ++ 10000'
            end as geo_diff_origin
     , case when abs(geo_distance - google_distance) = 0 then '0. same'
            when abs(geo_distance - google_distance) <= 0.1 then '1. 0 - 100'
            when abs(geo_distance - google_distance) <= 0.5 then '2. 100 - 500'
            when abs(geo_distance - google_distance) <= 1 then '3. 500 - 1000'
            when abs(geo_distance - google_distance) <= 5 then '4. 1000 - 2000'
            when abs(geo_distance - google_distance) <= 10 then '5. 2000 - 5000'
            when abs(geo_distance - google_distance) <= 10 then '6. 5000 - 10000'
            when abs(geo_distance - google_distance) > 10 then '7. ++ 10000'
            end as geo_diff_gg
     , case when abs(origin_distance - google_distance) = 0 then '0. same'
            when abs(origin_distance - google_distance) <= 0.1 then '1. 0 - 100'
            when abs(origin_distance - google_distance) <= 0.5 then '2. 100 - 500'
            when abs(origin_distance - google_distance) <= 1 then '3. 500 - 1000'
            when abs(origin_distance - google_distance) <= 5 then '4. 1000 - 2000'
            when abs(origin_distance - google_distance) <= 10 then '5. 2000 - 5000'
            when abs(origin_distance - google_distance) <= 10 then '6. 5000 - 10000'
            when abs(origin_distance - google_distance) > 10 then '7. ++ 10000'
            end as origin_diff_gg
     , case when origin_distance between 0 and 3 then '1. 0-3km'
            when origin_distance between 3 and 4 then '2. 3-4km'
            when origin_distance between 4 and 5 then '3. 4-5km'
            when origin_distance > 5 then '4. 5km++'
            end as origin_distance_group 
     , case when google_distance between 0 and 3 then '1. 0-3km'
            when google_distance between 3 and 4 then '2. 3-4km'
            when google_distance between 4 and 5 then '3. 4-5km'
            when google_distance > 5 then '4. 5km++'
            end as google_distance_group  
     , case when geo_distance between 0 and 3 then '1. 0-3km'
            when geo_distance between 3 and 4 then '2. 3-4km'
            when geo_distance between 4 and 5 then '3. 4-5km'
            when geo_distance > 5 then '4. 5km++'
            end as geo_distance_group                   
     , order_id 
     , (final_simulated_buyer_shipping_fee_google_distance - final_simulated_buyer_shipping_fee_original)*1.00000/b.exchange_rate as amt_diff_from_google_simulate 
     , (final_simulated_buyer_shipping_fee_google_distance - buyer_shipping_fee_after_flatship)*1.00000/b.exchange_rate as amt_diff_from_google_actual 
     , (final_simulated_buyer_shipping_fee_geo_distance - final_simulated_buyer_shipping_fee_original)*1.00000/b.exchange_rate as amt_diff_from_geo_simulate      
     , abs(google_distance - origin_distance) google_diff_distance 
     , abs(geo_distance - origin_distance) geo_diff_distance      

from two_wheel_shadow_test_buyer_shipping_fee a 
left join 
(
    SELECT distinct
        grass_date, exchange_rate
    FROM mp_order.dim_exchange_rate__reg_s0_live 
    WHERE currency='VND' 
    -- AND grass_date >= date('2020-12-28')
    and grass_date >= date'2024-07-23'
)b on a.create_date = b.grass_date
where a.is_missing_from_data = 0
)
group by 1,2,3,4,5


