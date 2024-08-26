with order_info as 
(select 
        date(dot.delivered_timestamp) as report_date,
        dot.id,
        dot.order_code,
        dot.group_id,
        dot.delivery_id,
        dot.city_name,
        dotet.total_shipping_fee as single_fee,
        dot.shipper_id,
        greatest(12000,if(order_type = 0,3750,3780)*1*dot.driver_distance) as recal_single_fee,
        greatest(11600,if(order_type = 0,3750,3780)*1*dot.driver_distance) as recal_single_fee_11k6,
        greatest(11000,if(order_type = 0,3750,3780)*1*dot.driver_distance) as recal_single_fee_11k,
        greatest(10500,if(order_type = 0,3750,3780)*1*dot.driver_distance) as recal_single_fee_10k5,
        greatest(10000,if(order_type = 0,3750,3780)*1*dot.driver_distance) as recal_single_fee_10k,
        order_status


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab  dot 

LEFT JOIN (SELECT 
                  order_id,
                  CAST(JSON_EXTRACT(dotet.order_data,'$.delivery.shipping_fee.total') AS DOUBLE) AS total_shipping_fee
        -- ,order_data
    from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da dotet
    where date(dt) = current_date - interval '1' day
          ) dotet on dotet.order_id = dot.delivery_id  
where 1 = 1 
and dot.order_type = 0
and dot.created_date >= date'2024-08-01'
and city_name in ('An Giang','Long An','Tien Giang','Hai Duong','Nam Dinh City','Phu Yen','Dong Thap','Kien Giang','Dak Lak','Thanh Hoa','Binh Dinh','Binh Thuan')
and dot.order_status in ('Delivered','Quit','Returned')
)
,group_info as 
(select 
        gi.id,
        gi.ship_fee as current_group_fee,
        gi.ship_cal as group_re_cal,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_single_current,greatest(oi.sum_single_current*gi.discount_rate,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_single_current,greatest(oi.sum_single_current*gi.discount_rate,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
              ) end as group_fee_current,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_recal_single_fee_11k6,greatest(oi.sum_recal_single_fee_11k6*gi.discount_rate,greatest(11600,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_recal_single_fee_11k6,greatest(oi.sum_recal_single_fee_11k6*gi.discount_rate,greatest(11600,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
              ) end as group_fee_11k6,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_recal_single_fee_11k,greatest(oi.sum_recal_single_fee_11k*gi.discount_rate,greatest(11000,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_recal_single_fee_11k,greatest(oi.sum_recal_single_fee_11k*gi.discount_rate,greatest(11000,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
              ) end as group_fee_11k,
 
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_recal_single_fee_10k5,greatest(oi.sum_recal_single_fee_10k5*gi.discount_rate,greatest(10500,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_recal_single_fee_10k5,greatest(oi.sum_recal_single_fee_10k5*gi.discount_rate,greatest(10500,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
              ) end as group_fee_10k5,

        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_recal_single_fee_10k,greatest(oi.sum_recal_single_fee_10k*gi.discount_rate,greatest(10000,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_recal_single_fee_10k,greatest(oi.sum_recal_single_fee_10k*gi.discount_rate,greatest(10000,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
              ) end as group_fee_10k,
        gi.cnt_order as total_order_in_group

--  Non Hub Stack Shipping Fee = Max( Min(Total Single Fee, Max(Discount Fee, Non Hub Stack Fee) + Non Hub Extra Fee, Orginal Larger Size Stack Fee), Original Smaller Size Stack Fee)

from driver_ops_group_table_temp gi 

left join 
(select 
        group_id,
        sum(recal_single_fee) as sum_single_current,
        sum(recal_single_fee_11k6) as sum_recal_single_fee_11k6,
        sum(recal_single_fee_11k) as sum_recal_single_fee_11k,
        sum(recal_single_fee_10k5) as sum_recal_single_fee_10k5,
        sum(recal_single_fee_10k) as sum_recal_single_fee_10k

from order_info
where group_id > 0 
group by 1 
) oi on oi.group_id = gi.id

where oi.group_id is not null 
)
select 
        report_date,
        city_name,
        count(distinct shipper_id) as a1, 
        count(distinct order_code) as total_order,
        count(distinct case when group_id > 0 then order_code else null end) as stack_group_order,
        sum(shipping_fee_shared) as total_shipping_fee_current,
        sum(shipping_fee_11k6) as total_shipping_fee_11k6,
        sum(shipping_fee_11k) as total_shipping_fee_11k,
        sum(shipping_fee_10k5) as total_shipping_fee_10k5,
        sum(shipping_fee_10k) as total_shipping_fee_10k

from
(select 
        oi.report_date,
        oi.order_code,
        oi.city_name,
        oi.group_id,
        oi.shipper_id,
        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then current_group_fee*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then current_group_fee*1.00/gi.total_order_in_group
        else single_fee end as shipping_fee_shared,
        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_11k6*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then current_group_fee*1.00/gi.total_order_in_group
        else recal_single_fee_11k6 end as shipping_fee_11k6,

        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_11k*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then current_group_fee*1.00/gi.total_order_in_group
        else recal_single_fee_11k end as shipping_fee_11k,

        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_10k5*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then current_group_fee*1.00/gi.total_order_in_group
        else recal_single_fee_10k5 end as shipping_fee_10k5,

        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_10k*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then current_group_fee*1.00/gi.total_order_in_group
        else recal_single_fee_10k end as shipping_fee_10k

from order_info oi 

left join group_info gi on gi.id = oi.group_id

where 1 = 1 
and report_date between current_date - interval '8' day and current_date - interval '2' day
and order_status = 'Delivered'
)


group by 1,2