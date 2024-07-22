with order_info as 
(SELECT 
        dot.id as ref_order_id
       ,dot.shipper_id 
       ,dot.city_name 
       ,dot.order_code
       ,dot.order_type
       ,dot.group_id
       ,ogi.group_code
       ,ogi.ref_order_category AS group_category
       ,dot.delivery_id
       ,dot.last_incharge_timestamp
       ,DATE(delivered_timestamp) as report_date
       ,dot.driver_policy
       ,ogi.rate_a 
       ,ogi.rate_b
       ,coalesce(ogi.unit_fee,ogi.unit_fee_v2) as unit_fee
       ,ogi.surge_rate
       ,ogi.min_fee
       ,ogi.extra_fee
       ,ogi.re_stack       
       ,dotet.total_shipping_fee as single_fee
       ,dot.distance AS single_distance   
       ,ogi.distance/CAST(100000 AS DOUBLE) AS group_distance 
       ,ogi.ship_fee/CAST(100 AS DOUBLE) AS final_stack_fee
       ,dotet.unit_fee_single
       ,dotet.min_fee_single
       ,dotet.surge_rate_single
       ,case 
       when dot.group_id > 0 and order_assign_type = 'Group' then 'group'
       when dot.group_id > 0 and order_assign_type != 'Group' then 'stack'
       else 'single' end as is_stack_group
       ,case 
       when dot.group_id > 0 then ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) 
       else 0 end as rank_order
       ,CAST(dot.pick_latitude AS DECIMAL(7,4)) AS pick_latitude
       ,CAST(dot.pick_longitude AS DECIMAL(7,4)) AS pick_longitude
       ,CAST(dot.drop_latitude AS DECIMAL(7,4)) AS drop_latitude
       ,CAST(dot.drop_longitude AS DECIMAL(7,4)) AS drop_longitude

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab dot         

LEFT JOIN 
(SELECT 
        group_id
        ,order_id
        ,ref_order_category
        ,cast(json_extract(dotet.extra_data,'$.ship_fee_info.per_km') as double) as unit_fee_single
        ,cast(json_extract(dotet.extra_data,'$.ship_fee_info.min_fee') as double) as min_fee_single
        ,cast(json_extract(dotet.extra_data,'$.ship_fee_info.surge_rate') as double) as surge_rate_single
        ,CAST(JSON_EXTRACT(dotet.extra_data,'$.ship_fee_info.driver_ship_fee') AS DOUBLE) AS total_shipping_fee   


from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da dotet
where date(dt) = current_date - interval '1' day

) dotet on dotet.order_id = dot.delivery_id and dotet.ref_order_category = dot.order_type and dotet.group_id = dot.group_id
LEFT JOIN (
SELECT 
        id,
        ref_order_category,
        group_code,
        distance,
        ship_fee,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_a') AS DOUBLE) AS rate_a ,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_b') AS DOUBLE) AS rate_b,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.unit_fee') AS DOUBLE) AS unit_fee,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.per_km') AS DOUBLE) AS unit_fee_v2,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.surge_rate') AS DOUBLE) AS surge_rate,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee') AS DOUBLE) AS min_fee,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) AS extra_fee,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.re') AS DOUBLE) AS re_stack     

from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da ogi 
WHERE DATE(dt) = current_date - interval '1' day
AND date(from_unixtime(create_time - 3600)) >= current_date - interval '30' day

) ogi 
    on ogi.id = dot.group_id 
    and ogi.ref_order_category = dot.order_type



where 1 = 1 
and grass_date >= current_date - interval '30' day
and dot.order_status in ('Delivered','Quit','Returned')
and dot.group_id > 0  
and dot.order_type != 0
)
,f as 
(select  
        group_id,
        group_code,
        order_type,
        max(report_date) as report_date,
        case 
        when MAX(group_distance) <= 1 then '1. 0-1'
        when MAX(group_distance) <= 2 then '2. 1 - 2'
        when MAX(group_distance) <= 3 then '3. 2 - 3'
        when MAX(group_distance) <= 4 then '4. 3 - 4'
        when MAX(group_distance) <= 5 then '5. 4 - 5'
        when MAX(group_distance) <= 6 then '6. 5 - 6'
        when MAX(group_distance) <= 7 then '7. 6 - 7'
        when MAX(group_distance) <= 8 then '8. 7 - 8'
        when MAX(group_distance) <= 9 then '9. 8 - 9'
        when MAX(group_distance) <= 10 then '10. 9 - 10'
        when MAX(group_distance) <= 11 then '11. 10 - 11'
        when MAX(group_distance) <= 12 then '12. 11 - 12'
        when MAX(group_distance) <= 13 then '13. 12 - 13'
        when MAX(group_distance) <= 14 then '14. 13 - 14'
        when MAX(group_distance) <= 15 then '15. 14 - 15'
        when MAX(group_distance) > 15 then '16. 15++'
        end as group_distance_range,
        MAX_BY(is_stack_group,last_incharge_timestamp) AS is_stack_group,
        ROUND(CASE WHEN MAX(rank_order) >= 2 THEN 
        MAX(final_stack_fee) - 
        (CASE WHEN MAX(rank_order) = 2 THEN 
         ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  ))
              WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.5),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))   
              ELSE MAX(final_stack_fee) END) END) AS extra_fee,
        MAX(final_stack_fee) AS group_fee_current,
        SUM(single_fee) AS sum_single_fee,
        MAX(group_distance) * MAX(unit_fee) * MAX(surge_rate) AS pay_by_distance,
        MAX(rank_order) as cnt_order,
        CARDINALITY(array_agg(DISTINCT pick_latitude)) AS count_pick_lat_unique,
        CARDINALITY(array_agg(DISTINCT pick_longitude)) AS count_pick_long_unique,

        CARDINALITY(array_agg(DISTINCT drop_latitude)) AS count_drop_lat_unique,
        CARDINALITY(array_agg(DISTINCT drop_longitude)) AS count_drop_long_unique,
       (CASE 
            WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) = CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category = 0, 1000 , 500  ) * (CARDINALITY(array_agg(DISTINCT pick_latitude)) - 1)
            WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) != CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category = 0, 1000 , 500  ) *(GREATEST(CARDINALITY(array_agg(DISTINCT pick_latitude)),CARDINALITY(array_agg(DISTINCT pick_longitude))) -1)
            END) +
        (CASE 
            WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) = CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category = 0, 1000 , 500  ) * (CARDINALITY(array_agg(DISTINCT drop_latitude)) - 1)
            WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) != CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category = 0, 1000 , 500  ) * (GREATEST(CARDINALITY(array_agg(DISTINCT drop_latitude)),CARDINALITY(array_agg(DISTINCT drop_longitude))) -1)
            END) AS new_extra_fee,
        MAX(min_fee) as min_group


from order_info

where group_id > 0 
and driver_policy != 2
group by 1,2,3,group_category
having MAX(rank_order) > 1
)
select
        group_distance_range,
        is_stack_group,
        cnt_order as total_order_in_group,
        IF(
           (coalesce(group_fee_current,0) - coalesce(greatest(extra_fee,0),0)) = COALESCE(min_group,0),
           'pay by min group','pay by distance'                                      
        ) as fee_segment,
        count(distinct group_id) as cnt_group,
        sum(cnt_order) as cnt_order,
        coalesce(sum(group_fee_current),0) - coalesce(sum(greatest(new_extra_fee,0)),0) as  "total group fee excluded extra",
        coalesce(sum(greatest(new_extra_fee,0)),0) as  "total extra fee",
        coalesce(sum(sum_single_fee),0) as "total single fee",
        count(distinct report_date) as "total days"


from f 

where 1 = 1 
group by 1,2,3,4


