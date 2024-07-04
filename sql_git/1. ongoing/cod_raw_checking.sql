select 
        date(from_unixtime(raw.submitted_time - 3600)) as created,
        raw.order_code,
        raw.ref_order_code,
        raw.ref_order_id as order_id,
        raw.uid as shipper_id,
        case 
        when raw.ref_order_category = 0 then 'order_delivery'
        when raw.ref_order_category = 6 then 'order_shopee'
        when raw.ref_order_category not in (0,6) then 'order_ship'
        end as source,
        raw.delivery_bonus*1.00/100 as bonus_fee,
        coalesce(doet.return_fee,0) as return_fee,
        coalesce(doet.item_value,0) as item_value,
        coalesce(doet.cod_value,0) as cod_value,
        coalesce(doet.shipping_fee_discount,0) as shipping_fee_discount,
        doet.other_fees as other_fees,
        CASE 
        WHEN raw.ref_order_status = 1 THEN 'INIT'
        WHEN raw.ref_order_status = 2 THEN 'ASSIGNING'
        WHEN raw.ref_order_status = 3 THEN 'ASSIGNING TIMEOUT'
        WHEN raw.ref_order_status = 4 THEN 'ASSIGNED'
        WHEN raw.ref_order_status = 6 THEN 'SHOPEE CANCELED'
        WHEN raw.ref_order_status = 8 THEN 'PICKUP'
        WHEN raw.ref_order_status = 9 THEN 'DRIVER CANCELED'
        WHEN raw.ref_order_status = 11 THEN 'COMPLETED'
        WHEN raw.ref_order_status = 12 THEN 'SYSTEM CANCELED'
        WHEN raw.ref_order_status = 13 THEN 'SYSTEM ASSIGNED'
        WHEN raw.ref_order_status = 14 THEN 'RETURN SUCCESS'
        WHEN raw.ref_order_status = 15 THEN 'RETURN FAILED'
        WHEN raw.ref_order_status = 16 THEN 'LOST'
        WHEN raw.ref_order_status = 17 THEN 'PICKUP FAILED'
        WHEN raw.ref_order_status = 18 THEN 'DELIVERY PENDING'
        WHEN raw.ref_order_status = 19 THEN 'RETURN TO HUB'
        WHEN raw.ref_order_status = 20 THEN 'RETURNING TO HUB'
        WHEN raw.ref_order_status = 21 THEN 'DELIVERY RETRY'
        WHEN raw.ref_order_status = 22 THEN 'RECLAIMED' END AS ref_order_status,
        ro.sender_name


from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day ) raw 
left join 
(select 
        order_id,
        coalesce(cast(json_extract(order_data,'$.booking.return_fee') as bigint),
                 cast(json_extract(order_data,'$.shopee.shipping_fee_info.return_fee') as bigint)
                 )
                 as return_fee,
        coalesce(cast(json_extract(order_data,'$.booking.item_value') as bigint),
                 cast(json_extract(order_data,'$.shopee.item_value') as bigint)
                 ) as item_value,
        coalesce(cast(json_extract(order_data,'$.booking.bill_amount') as bigint),
                 cast(json_extract(order_data,'$.shopee.cod_value') as bigint)
                 ) as cod_value,
        coalesce(cast(json_extract(order_data,'$.booking.shipping_fee_discount') as bigint),
                 0) as shipping_fee_discount,
        coalesce(json_extract(order_data,'$.booking.other_fees'),null) as other_fees

        -- order_data
from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da 
where date(dt) = current_date - interval '1' day
    ) doet 
    on raw.id = doet.order_id

left join dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab ro 
    on ro.id = raw.ref_order_id
    and ro.order_type = raw.ref_order_category

where raw.ref_order_category NOT IN (0,6)
and date(from_unixtime(raw.submitted_time - 3600)) between date'2024-05-01' and date'2024-06-30'