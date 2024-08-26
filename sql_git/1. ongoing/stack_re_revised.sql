with ogi as 
(select 
    id as group_id
    , group_code
    , ref_order_category
    , if(ref_order_category=0,'Delivery','SPXI') as source
    , distance * 1.00 / 100000 as group_distance
    , ship_fee * 1.00 / 100 as group_fee
    , uid as shipper_uid
    , group_status
    , create_time
    , cast(json_extract(extra_data, '$.re') as double) AS re 
    , cast(json_extract(extra_data, '$.pick_city_id') as int) AS city_id
    , from_unixtime(create_time - 3600) as created_ts

from shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi 

where date(from_unixtime(create_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
)
select 
        city_name,
        case 
        when re <=1 then '1. <= 1'
        when re <=1.01 then '2. <= 1.01'
        when re <=1.02 then '3. <= 1.02'
        when re <=1.03 then '4. <= 1.03'
        when re <=1.04 then '5. <= 1.04'
        when re <=1.05 then '6. <= 1.05'
        when re <=1.06 then '7. <= 1.06'
        when re <=1.07 then '8. <= 1.07'
        when re <=1.08 then '9. <= 1.08'
        when re <=1.09 then '10. <= 1.09'
        when re <=1.1 then '11. <= 1.1'
        when re <=1.2 then '12. <= 1.2'
        when re <=1.3 then '13. <= 1.3'
        when re <=1.4 then '14. <= 1.4'
        when re <=1.5 then '15. <= 1.5'
        when re > 1.5 then '16. ++1.5'
        end as re_assignment_range,
        case 
        when re_cal <=1 then '1. <= 1'
        when re_cal <=1.01 then '2. <= 1.01'
        when re_cal <=1.02 then '3. <= 1.02'
        when re_cal <=1.03 then '4. <= 1.03'
        when re_cal <=1.04 then '5. <= 1.04'
        when re_cal <=1.05 then '6. <= 1.05'
        when re_cal <=1.06 then '7. <= 1.06'
        when re_cal <=1.07 then '8. <= 1.07'
        when re_cal <=1.08 then '9. <= 1.08'
        when re_cal <=1.09 then '10. <= 1.09'
        when re_cal <=1.1 then '11. <= 1.1'
        when re_cal <=1.2 then '12. <= 1.2'
        when re_cal <=1.3 then '13. <= 1.3'
        when re_cal <=1.4 then '14. <= 1.4'
        when re_cal <=1.5 then '15. <= 1.5' 
        when re_cal > 1.5 then '16. ++1.5'
        end as re_cal_range,
        sum(re)*1.0000/count(distinct group_id) as assignment_re,
        sum(re_cal)*1.0000/count(distinct group_id) as avg_re_cal,
        sum(cnt_order_in_group)/count(distinct report_date)*1.00 as cnt_order,
        count(distinct group_id)/count(distinct report_date)*1.00 as cnt_group


from
(select 
        date(delivered_timestamp) as report_date,
        raw.group_id,
        case 
        when raw.city_id in (217,218,219) then raw.city_name
        else 'Other' end as city_name,
        ogi.re,
        ogi.group_distance,
        count(distinct raw.id) as cnt_order_in_group,
        sum(raw.driver_distance)*1.0000/ogi.group_distance as re_cal,
        array_agg(raw.driver_distance) as distance_agg,
        array_agg(raw.order_code) as code_agg,
        sum(raw.driver_distance) as sum_driver_distance

from (select 
        raw.*,
        if(raw.order_type != 0,1,coalesce(is_foody_delivery,0)) as filter_delivery,
        date_diff('second',created_timestamp,delivered_timestamp)/60.0000 as ata,
        IF(order_status = 'Delivered',1,0) as is_del,
        CASE 
        WHEN raw.source = 'now_ship_shopee' 
             THEN 
             (CASE 
             WHEN raw.order_status = 'Assigning Timeout' THEN 1 ELSE 0 END)
             WHEN raw.source in ('now_ship_user','now_ship_merchant') THEN 
                (CASE WHEN raw.last_incharge_timestamp is null and sa.assigning_count > 0 
                     and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') 
                     THEN 1 ELSE 0 END)
            WHEN raw.source in ('now_ship_same_day') THEN 
                (CASE WHEN raw.last_incharge_timestamp is null 
                and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') THEN 1 ELSE 0 END)
            WHEN raw.source in ('order_food','order_fresh','order_market') THEN 
                (CASE WHEN raw.cancel_reason = 'No driver' THEN 1 ELSE 0 END )
        ELSE 0 END AS is_no_driver


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 
left join (select id,is_foody_delivery 
           from shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
           where date(dt) = current_date - interval '1' day) oct 
                on raw.id = oct.id
LEFT JOIN 
    (SELECT 
             ref_order_id
            ,order_category
            ,COUNT(ref_order_id) AS assigning_count

     FROM driver_ops_order_assign_log_tab
     WHERE status in (3,4,2,14,15,8,9,17,18) 
     GROUP BY 1,2
     ) sa 
     on sa.ref_order_id = raw.id 
     and sa.order_category = raw.order_type

) raw 

left join ogi on ogi.group_id = raw.group_id and ogi.ref_order_category = raw.order_type

where raw.order_type = 0
and raw.group_id > 0 
and raw.filter_delivery = 1
and date(delivered_timestamp) between date'2024-08-18' and date'2024-08-20'
and raw.is_del = 1 
group by 1,2,3,4,5,ogi.group_distance
)
where cnt_order_in_group >= 1
and group_distance > 0 
group by 1,2,3
