SELECT if(order_type = 6, 'On Shopee', 'Off Shopee') AS service
     , city_name                                             AS pick_city_name
     , drop_city_name
     , district_name                                         AS pick_district_name
     , drop_district_name
     , created_date
     , case
           when is_group_order = 1 then 'Grouped'
           when is_stacked = 1 then 'Stacked'
           else 'Single' end                               as assign_type
     , case
           when is_group_order = 1 then total_order_in_group_original
           when is_stacked = 1 then total_order_in_group
           else 1 end as total_order_in_group
     , case
        when distance <= 1 then '1. 0-1km'
        when distance <= 2 then '2. 1-2km'
        when distance <= 3 then '3. 2-3km'
        when distance <= 4 then '4. 3-4km'
        when distance <= 5 then '5. 4-5km'
     else '6. 5+km' end as distance
     , count(distinct uid)                                   as delivered_orders
     , sum(if(is_valid_lt_e2e = 1, lt_e2e, 0))                  lt_e2e
     , sum(if(is_valid_lt_incharge = 1, lt_incharge, 0))        lt_incharge
     , sum(if(is_valid_lt_pickup = 1, lt_pickup, 0))            lt_pickup
     , sum(if(is_valid_lt_deliver = 1, lt_deliver, 0))          lt_deliver
FROM foody_bi_anlys.snp_foody_nowship_performance_tab
WHERE created_date BETWEEN DATE'2021-08-12'
  AND DATE'2021-08-23'
  AND order_status = 'Delivered'
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9
