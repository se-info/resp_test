with group_info as 
(select 
        a.id,
        a.ship_fee as group_fee,
        a.group_code,
        case 
        when (a.distance*1.00/100000) between 0 and 1 then '1. 0-1km'
        when (a.distance*1.00/100000) between 1 and 2 then '2. 1-2km'
        when (a.distance*1.00/100000) between 2 and 3 then '3. 2-3km'
        when (a.distance*1.00/100000) between 3 and 4 then '4. 3-4km'
        when (a.distance*1.00/100000) between 4 and 5 then '5. 4-5km'
        when (a.distance*1.00/100000) between 5 and 6 then '6. 5-6km'
        when (a.distance*1.00/100000) between 6 and 7 then '7. 6-7km'
        when (a.distance*1.00/100000) between 7 and 8 then '8. 7-8km'   
        when (a.distance*1.00/100000) between 8 and 9 then '9. 8-9km'
        when (a.distance*1.00/100000) between 9 and 10 then '10. 9-10km'
        when (a.distance*1.00/100000) > 10 then '11. ++10km' end as group_distance_range,
        a.distance*1.00/100000 as group_distance,
        cast(json_extract(a.extra_data,'$.re') as double) as re_system,
                                                                                     
        cast(json_extract_scalar(b.info,'$.order_id') as bigint) as order_id,
        cast(json_extract(a.extra_data,'$.ship_fee_info.unit_fee') as double) as group_unit_fee,
        cast(json_extract(a.extra_data,'$.ship_fee_info.surge_rate') as double) as group_surge_rate,
        doet.unit_fee,
        doet.min_fee,
        doet.surge_rate,
        doet.total_fee,
        case 
        when row_number()over(partition by a.id order by cast(json_extract_scalar(b.info,'$.order_id') as bigint)) = 1 
        then doet.total_fee
        else 0 end as single_fee_1,   
        case 
        when row_number()over(partition by a.id order by cast(json_extract_scalar(b.info,'$.order_id') as bigint)) = 2 
        then doet.total_fee
        else 0 end as single_fee_2,
        greatest(13500,(a.distance*1.0/100000) * cast(json_extract(a.extra_data,'$.ship_fee_info.unit_fee') as double)
                *cast(json_extract(a.extra_data,'$.ship_fee_info.surge_rate') as double)) as group_shipping_fee,
        date(from_unixtime(a.create_time - 3600)) as group_created
 
from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da a 

cross join unnest ((cast(json_extract(a.extra_data,'$.route') as array<json>)))  as b(info)

left join (select 
                order_id,
                cast(json_extract(order_data,'$.delivery.shipping_fee.unit_fee') as double) as unit_fee,
                cast(json_extract(order_data,'$.delivery.shipping_fee.min_fee') as double) as min_fee,
                cast(json_extract(order_data,'$.delivery.shipping_fee.rate') as double) as surge_rate,
                cast(json_extract(order_data,'$.delivery.shipping_fee.total') as double) as total_fee
        from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da 
        where date(dt) = current_date - interval '1' day 
        ) doet 
    on doet.order_id = cast(json_extract_scalar(b.info,'$.order_id') as bigint)

where date(a.dt) = current_date - interval '1' day
and cast(json_extract(b.info,'$.is_pick') as boolean) = true
and a.ref_order_category = 0 
-- and date(from_unixtime(a.create_time - 3600)) between date'2024-03-01' and date'2024-03-31'
)
,f as 
(select
        ro.*,
        max_by(gi.group_code,gi.order_id) as group_code,
        max_by(gi.group_distance_range,gi.order_id) as group_distance_range ,
        greatest
        (least(
            (sum(gi.total_fee)), -- sum_single
            greatest(sum(gi.total_fee)*0.65,max(gi.group_shipping_fee)) + count(gi.order_id)*500  -- min_group_shipping & total_shipping
            )
            ,
            max(gi.total_fee)
        )*1.00 as option1,
        greatest
        (least(
            (sum(gi.total_fee)), -- sum_single
            greatest(sum(gi.total_fee)*0,max(gi.group_shipping_fee)) + count(gi.order_id)*2000  -- min_group_shipping & total_shipping
            )
            ,
            max(gi.total_fee)
        )*1.00 as option2,
        greatest
        (least(
            (sum(gi.total_fee)), -- sum_single
            greatest(sum(gi.total_fee)*0.6,max(gi.group_shipping_fee)) + count(gi.order_id)*1000  -- min_group_shipping & total_shipping
            )
            ,
            max(gi.total_fee)
        )*1.00 as option3,
        sum(gi.total_fee) as p4,
        (sum(gi.total_fee)*0.6) + count(gi.order_id)*1000 as p3,
        max(gi.group_shipping_fee) + count(gi.order_id)*1000 as p2,
        13500 + count(gi.order_id)*1000 as p1,
        sum(gi.total_fee) as sum_single_fee,
        max(gi.group_fee*1.00/100) as current_group_fee,
        case
        when greatest
        (least(
            (sum(gi.total_fee)), -- sum_single
            greatest(sum(gi.total_fee)*0.6,max(gi.group_shipping_fee)) + count(gi.order_id)*1000  -- min_group_shipping & total_shipping
            )
            ,
            max(gi.total_fee)
        )*1.00 = sum(gi.total_fee) then '4. P4'
        when greatest
        (least(
            (sum(gi.total_fee)), -- sum_single
            greatest(sum(gi.total_fee)*0.6,max(gi.group_shipping_fee)) + count(gi.order_id)*1000  -- min_group_shipping & total_shipping
            )
            ,
            max(gi.total_fee)
        )*1.00 = ((sum(gi.total_fee)*0.6) + count(gi.order_id)*1000) then '3. P3'
        when greatest
        (least(
            (sum(gi.total_fee)), -- sum_single
            greatest(sum(gi.total_fee)*0.6,max(gi.group_shipping_fee)) + count(gi.order_id)*1000  -- min_group_shipping & total_shipping
            )
            ,
            max(gi.total_fee)
        )*1.00 = (max(gi.group_shipping_fee) + count(gi.order_id)*1000) then '2. P2'
        when greatest
        (least(
            (sum(gi.total_fee)), -- sum_single
            greatest(sum(gi.total_fee)*0.6,max(gi.group_shipping_fee)) + count(gi.order_id)*1000  -- min_group_shipping & total_shipping
            )
            ,
            max(gi.total_fee)
        )*1.00 = (13500 + count(gi.order_id)*1000) then '1. P1'
        when greatest
        (least(
            (sum(gi.total_fee)), -- sum_single
            greatest(sum(gi.total_fee)*0.6,max(gi.group_shipping_fee)) + count(gi.order_id)*1000  -- min_group_shipping & total_shipping
            )
            ,
            max(gi.total_fee)
        )*1.00 = max(gi.total_fee) then '0. max_min_fee'
        end as option3_segment
from 
(select
        ro.city_name,
        ro.group_id as id,
        max_by(date(ro.delivered_timestamp),ro.id) as report_date,
        count(distinct ro.id) as total_order_in_group        



from driver_ops_raw_order_tab ro
where ro.order_status = 'Delivered'
and ro.group_id > 0 
and ro.order_type = 0 
and ro.driver_policy != 2 
group by 1,2) ro

left join group_info gi 
        on gi.id = ro.id


group by 1,2,3,4
)
-- select * from f where report_date between date'2024-03-01' and date'2024-03-31' and option3_segment is null   limit 100
select
        if(city_name in ('HCM City','Ha Noi City'),city_name,'OTH') cities,
        group_distance_range,
        if(total_order_in_group > 2,1,0) as is_group,        
        option3_segment,
        count(distinct id) as cnt_group,
        sum(total_order_in_group) as cnt_order,
        sum(option3) as opt3,
        sum(sum_single_fee) as sum_single_fee,
        sum(current_group_fee) as current_group_fee,
        count(distinct report_date) as days

from f 
where 1 = 1 
and total_order_in_group = 2 
and report_date between date'2024-03-01' and date'2024-03-31'   
group by 1,2,3,4 




