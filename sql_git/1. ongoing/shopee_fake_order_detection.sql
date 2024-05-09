with img as 
(select
        code,
        array_agg(distinct case when regexp_like(cast(image_urls as varchar),'vn') = true then 'https://down-bs-vn.img.susercontent.com/'||cast(image_urls as varchar)||'.webp'
                  else cast(image_urls as varchar) end) as image_urls_agg

FROM shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live raw
CROSS JOIN UNNEST (CAST(JSON_EXTRACT(raw.extra_data,'$.image_urls') AS ARRAY<JSON>)) AS y(image_urls)
group by 1
)
,group_info as 
(select         
        id as group_id,
        group_code,
        ship_fee/cast(100 as double) as group_fee,
        (ship_fee/cast(100 as double))/r2.total_order_in_group as group_fee_allocate
        -- (ship_fee/cast(100 as double))/(json_array_length(json_extract(extra_data,'$.distance_matrix.mapping'))/2) as group_fee_allocate,
        -- json_array_length(json_extract(extra_data,'$.distance_matrix.mapping'))/2 as length_group

from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da r 

left join 
(select group_id,count(distinct order_code) as total_order_in_group
from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab
where order_status in ('Delivered','Returned') 
group by 1 
)r2 on r2.group_id = r.id
where date(dt) = current_date - interval '1' day
)
,seller_info as 
(select 
        cast(json_extract(extra_data,'$.sender_info.username') as varchar) as user_name,
        min(date(from_unixtime(create_time - 3600))) as first_order_date

FROM shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live
group by 1 
)
,raw_order as 
(select 
        date(from_unixtime(raw.create_time - 3600)) as created,                
        raw.code,
        raw.cod_value/cast(100 as double) as cod_value,
        raw.item_value/cast(100 as double) as item_value,
        cast(json_extract(raw.extra_data,'$.sender_info.name') as varchar) as sender_name,
        cast(json_extract(raw.extra_data,'$.sender_info.address') as varchar) as sender_address,
        cast(json_extract(raw.extra_data,'$.recipient_info.name') as varchar) as recipient_name,
        cast(json_extract(raw.extra_data,'$.recipient_info.address') as varchar) as recipient_address,
        raw.distance/cast(1000 as decimal(20,2)) as distance,
        raw.shipper_id,
        dp.city_name as shipper_city_name,
        if(raw.cod_value>0,'cod','non_cod') as pay_type,
        r.city_name as order_city_name,
        r.order_status,
        r.group_id,
        img.image_urls_agg,
        if(r.group_id > 0,gi.group_fee,shipping_fee/cast(100 as decimal(20,2))) as driver_fee,
        r.last_incharge_timestamp,
        from_unixtime(raw.drop_real_time - 3600) as delivered_timestamp,
        date_diff('second',r.last_incharge_timestamp,from_unixtime(raw.drop_real_time - 3600)) as e2e,
        coalesce(dn.num_deny_fake_order,0) as num_deny_fake_order,
        date_diff('day',si.first_order_date,date(from_unixtime(raw.create_time - 3600))) as seniority_of_seller,

        -- shipping_fee/cast(100 as decimal(20,2)) as single_fee,
        sum(cast(json_extract(z.item_name,'$.quantity') as bigint)) as quantity,
        (sum(cast(json_extract(z.item_name,'$.weight') as bigint))*sum(cast(json_extract(z.item_name,'$.quantity') as bigint))
        )/cast(1000 as decimal(20,3)) as weight_kg,
        array_join(array_agg(distinct cast(json_extract(z.item_name,'$.name') as varchar)),',') as item_list,
        cardinality(array_agg(distinct cast(json_extract(z.item_name,'$.name') as varchar))) as num_of_item


FROM shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live raw

LEFT JOIN driver_ops_raw_order_tab r on r.order_code = raw.code and r.order_type = 6 

LEFT JOIN seller_info si on si.user_name = cast(json_extract(extra_data,'$.sender_info.username') as varchar)

LEFT JOIN img on img.code = raw.code

LEFT JOIN group_info gi on gi.group_id = r.group_id

LEFT JOIN driver_ops_driver_performance_tab dp on dp.shipper_id = raw.shipper_id and dp.report_date = date(from_unixtime(raw.create_time - 3600))

LEFT JOIN (select order_code,count(distinct (order_code,created_ts)) as num_deny_fake_order from driver_ops_deny_log_tab where reason_id = 127 group by 1) dn on dn.order_code = raw.code

CROSS JOIN UNNEST (CAST(JSON_EXTRACT(raw.extra_data,'$.items') AS ARRAY<JSON>)) AS z(item_name)

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
select * from 
(select  
        *,
        case 
        when order_status = 'Delivered' and num_deny_fake_order = 0 and quantity >= 10 and (cod_value <= 50000 or item_value <= 50000) and distance <= 3 then 1 
        else 0 end as is_hit_order_rule_hcm,
        case 
        when order_status = 'Delivered' and num_deny_fake_order = 0 and quantity >= 20 and distance >= 5 and distance/cast(e2e as decimal(10,2)) >= 100 then 1 
        else 0 end as is_hit_order_rule_hn,
        if(seniority_of_seller <= 180,1,0) as is_new_seller

from raw_order 

where created between current_date - interval '1' day and current_date - interval '1' day
)
where (is_hit_order_rule_hcm = 1 or is_hit_order_rule_hn = 1)
 

/*
HN

1. Về order level: 
- Status order: Delivered (Order chưa từng phát sinh bị deny Fake Order)
- Số lượng item: >= 20
- Distance: >= 5km  
- Thời gian kết thúc hành trình bất thường: V >= 100km/h
2. Về seller level: 
- Có Số lượng đơn bất thường (như muc 1): >5 đơn
- Thâm niên shop ngắn: <= 3 tháng

---
1. Về order level: 
- Status order: Delivered (Order chưa từng phát sinh bị deny Fake Order)
- Số lượng item: >=10 
- COD/Item value: <= 50k
- Distance: <= 3km 
2. Về seller level: 
- Có Số lượng đơn bất thường (như muc 1): >5 đơn
- Thâm niên shop ngắn: <= 3 tháng
*/
/*
Date, order_code, order_status, shipper_id, order_city_name, incharged_ts, cod_value, item_value, distance, payment_type, sender_name, sender_address, 
recipient_name, recipient_address, item_list, num_of_item, quantity, weight_kg, city_name, image_urls, shipper_id_at_final_status, group_id, group_code, driver_fee, total_order_in_group, 
*/