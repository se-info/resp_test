with group_info as 
(select 
        id,
        group_code,
        max(cnt_order) as total_order_in_group,
        max(group_distance) as group_distance
        
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
        least(
            13500*r.cnt_order*1.00
        ,(greatest(3750*(a.distance*1.00/100000)*1,((13500*r.cnt_order)*0.7)) + (1000*(r.no_pickup - 1) + 1000*(no_dropoff - 1) ) *1.00)
        )
            /r.cnt_order*1.00 as stacked_per_order_opt1
    

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
and date(from_unixtime(create_time - 3600)) >= date_trunc('month',current_date) - interval '4' month
)
group by 1,2
)
,f as 
(select 
        ho.uid as shipper_id,
        ho.slot_id,
        ho.autopay_report_id,
        ho.ref_order_id,
        ho.ref_order_category,
        r.group_id,
        coalesce(gi.total_order_in_group,1) as total_order_in_group,
        case 
        when r.group_id > 0 then coalesce(gi.group_distance,0)  
        else r.distance
        end as distance_range,
        case when coalesce(oct.risk_bearer_id,0) != 2 then 1 else 0 end as is_hub_order,
        date(from_unixtime(ho.autopay_date_ts-3600)) as autopay_date,
        case 
        when r.group_id > 0 then 1 else 0 end as is_group,
        date(r.delivered_timestamp) as report_date,
        case 
        when r.pick_hub_id = r.drop_hub_id then '1. same pick drop'
        else '2. diff pick drop' end as route_type,
        r.city_name

from shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_daily_s0_live ho

left join driver_ops_raw_order_tab r on ho.ref_order_id = r.id and ho.ref_order_category = r.order_type

left join group_info gi on gi.id = r.group_id

left join 
(select id,cast(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') as int) as risk_bearer_id 
from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
where date(from_unixtime(submit_time - 3600)) >= date_trunc('month',current_date) - interval '4' month
) oct
    on ho.ref_order_id = oct.id and ho.ref_order_category = 0
where 1 = 1 
and date(from_unixtime(ho.autopay_date_ts-3600)) between date'2024-04-17' and date'2024-07-16'
)
select 
        report_date,
        city_name,
        distance_range,
        is_out,
        sum("số lượng group") as "số lượng group",
        sum("số lượng đơn") as "số lượng đơn",
        sum("tổng khoảng cách của group") as "tổng khoảng cách của group",
        1.0000*sum("tổng khoảng cách của group")/sum("số lượng group") as "trung bình khoảng cách của 1 group"
from
(select 
        f.group_id,
        f.report_date,
        city_name,
        case 
        when distance_range <= 3 then '1. 0 - 3km'
        when distance_range <= 4 then '2. 3 - 4km'
        when distance_range <= 5 then '3. 4 - 5km'
        when distance_range > 5 then '4. ++5km'
        end as distance_range,
        count(distinct case when route_type = '2. diff pick drop' then 1 else null end) as is_out,
        count(distinct group_id) as "số lượng group",
        count(distinct ref_order_id) as "số lượng đơn",
        sum(distance_range) as "tổng khoảng cách của group",
        1.0000*sum(distance_range)/count(distinct group_id) as "trung bình khoảng cách của 1 group"



from f
where group_id > 0 
group by 1,2,3,4
)
group by 1,2,3,4