WITH merchant_filter as  
(select
        sender_name as merchant_name,
        count(distinct order_code) as cnt_order

from driver_ops_raw_order_tab
where grass_date between current_date - interval '60' day and current_date - interval '1' day 
and order_type = 0
and order_status = 'Delivered'
group by 1 
having count(distinct order_code) >= 60
)
,raw AS 
(select 
        raw.*,
        hour(created_timestamp) as created_hour,
        raw.restaurant_id as merchant_id,
        raw.sender_name as merchant_name,
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
        ELSE 0 END AS is_no_driver,
        case 
        when coalesce(driver_distance,distance) <= 3 then '1. 0 - 3'
        when coalesce(driver_distance,distance) <= 5 then '2. 3 - 5'
        when coalesce(driver_distance,distance) <= 7 then '3. 5 - 7'
        when coalesce(driver_distance,distance) <= 9 then '4. 7 - 9'
        when coalesce(driver_distance,distance) > 9  then '5. ++9 ' end as distance_range,
        di.name_en as district_name,
        if(raw.order_status = 'Delivered',1,0) as is_del,
        coalesce(IF(ms.prepare_time_actual is not null,ms.prepare_time_actual,date_diff('second',osl.confirmed_time,osl.picked_time)),0)*1.0000/60 as prepare_time,
        if(ogi.cnt_merchant <=1,1,0) as same_merchant


from (select raw.*,if(raw.order_type != 0,1,coalesce(is_foody_delivery,0)) as filter_delivery,restaurant_id
from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 
left join (select id,is_foody_delivery,restaurant_id
           from shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
           where date(dt) = current_date - interval '1' day) oct 
                on raw.id = oct.id
) raw 

left join shopeefood.foody_order_db__order_completed_merchant_search_tab__reg_daily_s0_live ms 
    on ms.id = raw.id

left join 
(select 
        group_id,
        count(distinct sender_name) as cnt_merchant,
        count(distinct order_code) as cnt_order_in_group

from driver_ops_raw_order_tab 
where order_status in ('Delivered','Quit','Returned')
group by 1 
) ogi on ogi.group_id = raw.group_id

left join 
(select 
        order_id,
        cast(max(case when status = 13 then from_unixtime(create_time) else null end) as timestamp) - interval '1' hour as confirmed_time,
        cast(max(case when status = 6 then from_unixtime(create_time) else null end) as timestamp) - interval '1' hour as picked_time

from shopeefood.foody_order_db__order_status_log_tab_di
where grass_date >= cast(current_date - interval '60' day as varchar)
group by 1 
) osl on osl.order_id = raw.id

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di 
    on di.id = raw.district_id 
    and di.province_id = raw.city_id

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
where filter_delivery = 1 
and order_type =0
and grass_date >= date'2024-09-23'
)
select 
        city_name,
        raw.merchant_name,
        try(count(distinct case when is_del = 1 then order_code else null end) )as cnt_net_order,
        try(count(distinct case when is_no_driver = 1 then order_code else null end) )as cnd_order,
        try(coalesce(avg(case when is_del = 1 and prepare_time > 0 then prepare_time else null end),0) )as avg_preptime,
        try(coalesce(approx_percentile(case when is_del = 1 and prepare_time > 0 then prepare_time else null end,0.75),0) )as "prep_time pct 75th",
        try(coalesce(approx_percentile(case when is_del = 1 and prepare_time > 0 then prepare_time else null end,0.80),0) )as "prep_time pct 80th",
        try(coalesce(approx_percentile(case when is_del = 1 and prepare_time > 0 then prepare_time else null end,0.85),0) )as "prep_time pct 85th",
        try(coalesce(approx_percentile(case when is_del = 1 and prepare_time > 0 then prepare_time else null end,0.90),0) )as "prep_time pct 90th",
        try(count(distinct case when is_del = 1 and group_id > 0 then order_code else null end)*1.00/count(distinct case when is_del = 1 then order_code else null end)) as stack_rate,
        try(count(distinct case when is_del = 1 and group_id > 0 and same_merchant = 1 then order_code else null end)
                *1.00/count(distinct case when is_del = 1 then order_code else null end) )as stack_rate_same_mex,
        try(count(distinct case when is_del = 1 and group_id > 0 and same_merchant != 1 then order_code else null end)
                *1.00/count(distinct case when is_del = 1 then order_code else null end) )as stack_rate_diff_mex,
        try(count(distinct case when is_del = 1 and group_id > 0 and created_hour between 10 and 12 then order_code else null end)
                *1.00/count(distinct case when is_del = 1 and created_hour between 10 and 12 then order_code else null end) )as "stack 10h to 13h",

        try(count(distinct case when is_del = 1 and group_id > 0 and created_hour between 18 and 21 then order_code else null end)
                *1.00/count(distinct case when is_del = 1 and created_hour between 18 and 21 then order_code else null end) )as "stack 18h to 21h"


from raw 

inner join merchant_filter mf on mf.merchant_name = raw.merchant_name

where grass_date between current_date - interval '30' day and current_date - interval '1' day
and is_del = 1 
group by 1,2