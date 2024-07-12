with group_info as 
(select 
        id,
        group_code,
        max(stacked_per_order_current) as stacked_per_order_current,
        max(stacked_per_order_15k) as stacked_per_order_15k,
        max(stacked_per_order_16k) as stacked_per_order_16k
        
from
(select 
        a.id,
        a.group_code,
        a.distance*1.00/100000 as group_distance,
        cast(json_extract(a.extra_data,'$.re') as double) as re_system,
        r.cnt_order*1.00 as cnt_order,
        3750*(a.distance*1.00/100000)*1 as group_shipping_fee,
        500 as extra_fee,
        13500*r.cnt_order*1.00 AS sum_single,
        15000*r.cnt_order*1.00 AS sum_single_15k,
        16000*r.cnt_order*1.00 AS sum_single_16k,
        least(
            13500*r.cnt_order*1.00
        ,(greatest(3750*(a.distance*1.00/100000)*1,((13500*r.cnt_order)*0.7)) + (1000*(r.no_pickup - 1) + 1000*(no_dropoff - 1) ) *1.00)
        )
            /r.cnt_order*1.00 as stacked_per_order_current,
        least(
            15000*r.cnt_order*1.00
        ,(greatest(3750*(a.distance*1.00/100000)*1,((15000*r.cnt_order)*0.7)) + (1000*(r.no_pickup - 1) + 1000*(no_dropoff - 1) ) *1.00)
        )
            /r.cnt_order*1.00 as stacked_per_order_15k,
        least(
            16000*r.cnt_order*1.00
        ,(greatest(3750*(a.distance*1.00/100000)*1,((16000*r.cnt_order)*0.7)) + (1000*(r.no_pickup - 1) + 1000*(no_dropoff - 1) ) *1.00)
        )
            /r.cnt_order*1.00 as stacked_per_order_16k
    

from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da a 

left join 
(select 
        group_id,
        order_type,
        count(distinct order_code) as cnt_order,
        count(sender_name) as no_pickup,
        count(receiver_name) as no_dropoff

from driver_ops_raw_order_tab
where order_status IN ('Delivered','Quit','Returned')
group by 1,2 
) r ON r.group_id = a.id and r.order_type = a.ref_order_category 

-- cross join unnest (map_entries(cast(json_extract(a.extra_data,'$.fee_details.fee_config.sub_orders') as map<int,json>)))  as b(order_id,alue)
cross join unnest ((cast(json_extract(a.extra_data,'$.fee_details.fee_config.sub_orders') as map<int,json>)))  as b(order_id,info)

where date(dt) = current_date - interval '1' day

)
group by 1,2
)
,hub_order as 
(select 
        ho.uid as shipper_id,
        ho.slot_id,
        ho.autopay_report_id,
        ho.ref_order_id,
        ho.ref_order_category,
        r.group_id,
        gi.group_code,
        case when coalesce(oct.risk_bearer_id,0) != 2 then 1 else 0 end as is_hub_order,
        date(from_unixtime(ho.autopay_date_ts-3600)) as autopay_date,
        13500 as original_base_fee,
        case 
        when r.group_id > 0 then coalesce(gi.stacked_per_order_current,13500)
        else 13500 end as current_fee,
        case 
        when r.group_id > 0 then coalesce(gi.stacked_per_order_15k,15000)
        else 15000 end as opt1_15k,
        case 
        when r.group_id > 0 then coalesce(gi.stacked_per_order_16k,16000)
        else 16000 end as opt2_16k,
        r.city_name,
        case 
        when r.group_id > 0 then 1 else 0 end as is_group,
        r.delivery_id

from shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_daily_s0_live ho

left join driver_ops_raw_order_tab r on ho.ref_order_id = r.id and ho.ref_order_category = r.order_type

left join group_info gi on gi.id = r.group_id

left join 
(select id,cast(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') as int) as risk_bearer_id 
from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
where date(from_unixtime(submit_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
) oct
    on ho.ref_order_id = oct.id and ho.ref_order_category = 0
where 1 = 1 
and date(from_unixtime(ho.autopay_date_ts-3600)) between date'2024-06-24' and date'2024-06-30'  
and hour(r.delivered_timestamp) = 11 
and r.city_name = 'Ha Noi City'
and r.source = 'order_food'
)
select 
        autopay_date,
        sum(original_base_fee) AS total_cost_current,
        sum(current_fee) AS cost_optimzed_13k5,
        sum(opt1_15k) AS cost_optimzed_15k,
        sum(opt2_16k) AS cost_optimzed_16k,
        count(distinct ref_order_id) as total_order

from hub_order

group by 1 
