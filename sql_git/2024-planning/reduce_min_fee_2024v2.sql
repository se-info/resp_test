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
       ,case 
       when delivered_timestamp is not null then DATE(delivered_timestamp)
       else created_date end as report_date
       ,dotet.driver_policy
       ,ogi.rate_a 
       ,ogi.rate_b
       ,ogi.unit_fee
       ,ogi.surge_rate
       ,ogi.min_fee
       ,ogi.extra_fee
       ,ogi.re_stack       
       ,dotet.total_shipping_fee AS single_fee
       ,13500 as hub_current
       ,case 
        when dot.distance <= 3 then 12500
        when dot.distance <= 3.5 then 13500
        when dot.distance >= 3.5 then ((ceiling(dot.distance) - 3.5)/0.5) *1000 + 13500 
        end as hub_opt1,
        case 
        when dot.distance <= 3 then 12000
        when dot.distance <= 3.5 then 13500
        when dot.distance >= 3.5 then ((ceiling(dot.distance) - 3.5)/0.5) *1000 + 13500 
        end as hub_opt2,
        case 
        when dot.distance <= then 12000
        when dot.distance <= 3.5 then 13500
        when dot.distance >= 3.5 then ((ceiling(dot.distance) - 3.5)/0.5) *1000 + 13500 
        end as hub_opt3,
        12500 as hub_opt4,
        (hi.shift_end_time - hi.shift_start_time)/3600 as shift_hour,
        cast(json_extract(hi.extra_data,'$.is_apply_fixed_amount') as varchar) as apply_extra_fee,
        hub.slot_id
        
       ,dotet.total_shipping_fee as non_hub_current
       ,GREATEST(12500,dotet.unit_fee_single*dotet.surge_rate_single*dot.distance) as non_hub_opt1 
       ,GREATEST(12000,dotet.unit_fee_single*dotet.surge_rate_single*dot.distance) as non_hub_opt2
       ,GREATEST(IF(dot.distance <=2,12000,13500),dotet.unit_fee_single*dotet.surge_rate_single*dot.distance) as non_hub_opt3_4
       
       ,dot.distance AS single_distance   
       ,ogi.distance/CAST(100000 AS DOUBLE) AS group_distance 
       ,ogi.ship_fee/CAST(100 AS DOUBLE) AS final_stack_fee
        ,dotet.unit_fee_single
        ,dotet.min_fee_single
        ,dotet.surge_rate_single
       ,CAST(dot.pick_latitude AS DECIMAL(7,4)) AS pick_latitude
       ,CAST(dot.pick_longitude AS DECIMAL(7,4)) AS pick_longitude
       ,CAST(dot.drop_latitude AS DECIMAL(7,4)) AS drop_latitude
       ,CAST(dot.drop_longitude AS DECIMAL(7,4)) AS drop_longitude
        ,GREATEST(
                dotet.min_fee_single,
                ROUND(dotet.unit_fee_single * (dot.distance) *
                      dotet.surge_rate_single
                     )
        ) AS single_cal
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) = 1 THEN dotet.total_shipping_fee
            ELSE 0
            END AS fee_1_cal
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) = 2 THEN dotet.total_shipping_fee
            ELSE 0
            END AS fee_2_cal             
        ,GREATEST(
                12500,
                ROUND(dotet.unit_fee_single * (dot.distance) *
                      dotet.surge_rate_single
                     )
        ) AS single_opt1
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) = 1 
                THEN GREATEST(12500,ROUND(dotet.unit_fee_single * (dot.distance) * dotet.surge_rate_single))
            ELSE 0
            END AS fee_1_opt1
        ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) = 2 
                THEN GREATEST(12500,ROUND(dotet.unit_fee_single * (dot.distance) * dotet.surge_rate_single))
            ELSE 0
            END AS fee_2_opt1

-- opt2     
        ,GREATEST(
                12000,
                ROUND(dotet.unit_fee_single * (dot.distance) *
                      dotet.surge_rate_single
                     )
        ) AS single_opt2
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) = 1 
                THEN GREATEST(12000,ROUND(dotet.unit_fee_single * (dot.distance) * dotet.surge_rate_single))
            ELSE 0
            END AS fee_1_opt2
        ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) = 2 
                THEN GREATEST(12000,ROUND(dotet.unit_fee_single * (dot.distance) * dotet.surge_rate_single))
            ELSE 0
            END AS fee_2_opt2

-- opt3,4     
        ,GREATEST(
                IF(dot.distance <=2,12000,13500)
                ,
                ROUND(dotet.unit_fee_single * (dot.distance) *
                      dotet.surge_rate_single
                     )
        ) AS single_opt3_4
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) = 1 
                THEN GREATEST(IF(dot.distance <=2,12000,13500),ROUND(dotet.unit_fee_single * (dot.distance) * dotet.surge_rate_single))
            ELSE 0
            END AS fee_1_opt3_4
        ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) = 2 
                THEN GREATEST(IF(dot.distance <=2,12000,13500),ROUND(dotet.unit_fee_single * (dot.distance) * dotet.surge_rate_single))
            ELSE 0
            END AS fee_2_opt3_4
       ,case 
       when dot.group_id > 0 then ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.picked_timestamp ASC) 
       else 0 end as rank_order

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab dot         


left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live hub 
    on hub.ref_order_id = dot.id
    and hub.ref_order_category = dot.order_type

left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hi 
    on hi.id = hub.autopay_report_id

LEFT JOIN (SELECT 
                  order_id
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee_single
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee_single
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate_single
        ,CAST(JSON_EXTRACT(dotet.order_data,'$.shipper_policy.type') AS DOUBLE) AS driver_policy   
        ,CAST(JSON_EXTRACT(dotet.order_data,'$.delivery.shipping_fee.total') AS DOUBLE) AS total_shipping_fee   
        -- ,order_data
    from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da dotet
    where date(dt) = current_date - interval '1' day
          ) dotet on dotet.order_id = dot.delivery_id  

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
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.surge_rate') AS DOUBLE) AS surge_rate,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee') AS DOUBLE) AS min_fee,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) AS extra_fee,
        CAST(JSON_EXTRACT(ogi.extra_data,'$.re') AS DOUBLE) AS re_stack     

from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da ogi 
WHERE DATE(dt) = current_date - interval '1' day
) ogi 
    on ogi.id = dot.group_id 
    and ogi.ref_order_category = dot.order_type


where 1 = 1 
and (case 
       when delivered_timestamp is not null then DATE(delivered_timestamp)
       else created_date end) between date'2023-10-01' and date'2023-10-31'
and dot.order_status in ('Delivered','Quit','Returned')
and dot.shipper_id > 0 
and dot.order_type = 0
)
,group_info as 
(select  
        group_id,
        order_type,
        ROUND(CASE WHEN MAX(rank_order) >= 2 THEN 
        MAX(final_stack_fee) - (CASE WHEN MAX(rank_order) = 2 THEN 
         ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  ))
              WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))   
              ELSE MAX(final_stack_fee) END) END) AS extra_fee,
        
        ROUND(CASE WHEN MAX(rank_order) >= 2 THEN 
        MAX(final_stack_fee) - (CASE WHEN MAX(rank_order) = 2 THEN 
         ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  ))
              WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))   
              ELSE MAX(final_stack_fee) END) END)/MAX(rank_order) AS extra_fee_allocate,

         MAX(final_stack_fee)/CAST(MAX(rank_order) AS DOUBLE) AS group_fee_current,
        ROUND(CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt1) + (MAX(fee_2_opt1)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt1) + (MAX(fee_1_opt1)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt1),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt1)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt1),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END)/MAX(rank_order) AS group_fee_opt1,

        ROUND(CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt2) + (MAX(fee_2_opt2)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt2) + (MAX(fee_1_opt2)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt2),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt2)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt2),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END)/MAX(rank_order) AS group_fee_opt2,

        ROUND(CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt3_4) + (MAX(fee_2_opt3_4)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt3_4) + (MAX(fee_1_opt3_4)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt3_4),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt3_4)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt3_4),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END)/MAX(rank_order) AS group_fee_opt3_4

from order_info

where group_id > 0 
and driver_policy != 2
group by 1,2,group_category
)
,metrics as 
(select 
        raw.report_date,
        raw.shipper_id,
        raw.is_hub,
        raw.cities,
        raw.shift_hour,
        raw.slot_id,
        coalesce(do.extra_ship,0) as extra_ship,
        count(distinct ref_order_id) as total_order,
        sum(current_fee) as current_fee,
        sum(fee_opt1) as fee_opt1,
        sum(fee_opt2) as fee_opt2,
        sum(fee_opt3) as fee_opt3,
        sum(fee_opt4) as fee_opt4     
from
(select 
        oi.report_date,
        oi.ref_order_id,
        oi.group_id,
        oi.shipper_id,
        oi.slot_id,
        shift_hour,
        case 
        when oi.city_name in ('HCM City','Ha Noi City','Da Nang City') then oi.city_name
        else 'Others' end as cities,
        oi.group_code,
        oi.order_code,
        case 
        when oi.slot_id > 0 then 1 else 0 end as is_hub,
        case 
        when oi.slot_id > 0 then oi.hub_current
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_current
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_current end as current_fee,

        case 
        when oi.slot_id > 0 then oi.hub_opt1
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_opt1
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_opt1 end as fee_opt1,

        case 
        when oi.slot_id > 0 then oi.hub_opt2
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_opt2
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_opt2 end as fee_opt2,

        case 
        when oi.slot_id > 0 then oi.hub_opt3
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_opt3_4
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_opt3_4 end as fee_opt3,

        case 
        when oi.slot_id > 0 then oi.hub_opt4
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_opt3_4
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_opt3_4 end as fee_opt4

from order_info oi
left join group_info ogi 
        on ogi.group_id = oi.group_id 
        and oi.order_type = ogi.order_type
) raw 

left join driver_ops_hub_driver_performance_tab do 
    on do.uid = raw.shipper_id
    and do.date_ = raw.report_date
    and do.total_order > 0 
    and do.slot_id = raw.slot_id
group by 1,2,3,4,5,6,7
)
select 
        date_trunc('month',report_date) as month_,
        is_hub,
        coalesce(cities,'VN') as cities,
        sum(current_fee) as current_fee,
        sum(fee_opt1) as fee_opt1, 
        sum(fee_opt2) as fee_opt2, 
        sum(fee_opt3) as fee_opt3, 
        sum(fee_opt4) as fee_opt4, 

        sum(extra_ship) as current_extra,
        sum(new_extra_opt1) as new_extra_opt1, 
        sum(new_extra_opt1) as new_extra_opt1, 
        sum(new_extra_opt2) as new_extra_opt2, 
        sum(new_extra_opt3) as new_extra_opt3, 
        sum(new_extra_opt4) as new_extra_opt4,

        sum(total_order) as total_order 
from 
(select  
        *,
        case 
        when extra_ship > 0 and shift_hour = 10 then (13500*30)-fee_opt1
        when extra_ship > 0 and shift_hour = 8 then (13500*25)-fee_opt1
        else 0 end as new_extra_opt1,
        case 
        when extra_ship > 0 and shift_hour = 10 then (13500*30)-fee_opt2
        when extra_ship > 0 and shift_hour = 8 then (13500*25)-fee_opt2
        else 0 end as new_extra_opt2,
        case 
        when extra_ship > 0 and shift_hour = 10 then (13500*30)-fee_opt3
        when extra_ship > 0 and shift_hour = 8 then (13500*25)-fee_opt3
        else 0 end as new_extra_opt3,
        case 
        when extra_ship > 0 and shift_hour = 10 then (13500*30)-fee_opt4
        when extra_ship > 0 and shift_hour = 8 then (13500*25)-fee_opt4
        else 0 end as new_extra_opt4
from metrics )

group by 1,2, grouping sets (cities,())








