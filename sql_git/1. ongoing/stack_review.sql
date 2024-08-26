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
        grass_date,
        -- HOUR(raw.created_timestamp) as "hour",
        case 
        when raw.city_id in (217,218,219) then raw.city_name
        else 'Other' end as cities,
        -- case 
        -- when raw.group_id > 0 and raw.order_assign_type != 'Group' then 'stack'
        -- when raw.group_id > 0 and raw.order_assign_type = 'Group' then 'group'
        -- else 'single' end as assign_type,
        -- -- case 
        -- -- when ogi.re <= 1.1 then '1. <= 1.1'
        -- -- when ogi.re <= 1.3 then '2. <= 1.3'
        -- -- when ogi.re <= 1.5 then '3. <= 1.5'
        -- -- when ogi.re <= 2 then '4. <= 2'
        -- -- when ogi.re > 2 then '5. ++2' 
        -- -- else 'single' end as re_range,
        count(distinct case when is_del = 1 then raw.id else null end) as cnt_order,
        count(distinct case when is_del = 1 then raw.group_id else null end) as cnt_group,
        sum(case when is_del = 1 then driver_distance else null end)/count(distinct case when is_del = 1 then raw.id else null end) as avg_single_distance,
        sum(case when is_del = 1 and raw.group_id > 0 then ogi.group_distance else null end)/count(distinct case when is_del = 1 and raw.group_id > 0 then raw.id else null end) as avg_group_distance,
        sum(case when is_del = 1 and raw.group_id > 0 then ogi.re else null end)/count(distinct case when is_del = 1 and raw.group_id > 0 then raw.id else null end) as avg_group_re,
        sum(case when is_asap = 1 then ata else null end)*1.0000/count(distinct case when is_asap = 1 then raw.id else null end) as ata,
        count(distinct case when raw.is_no_driver = 1 then raw.id else null end) as cnd,
        count(distinct raw.id) as gross_order

from (
select 
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
and raw.filter_delivery = 1
and raw.grass_date between date'2024-08-18' and date'2024-08-20'

group by 1,2

