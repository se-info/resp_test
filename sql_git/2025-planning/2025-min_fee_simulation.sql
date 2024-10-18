drop table if exists driver_ops_cal_tab;
create table if not exists driver_ops_cal_tab as
with config_tab as
(select 
    city_name
    ,cast(current_min_fee as double) as current_min_fee
    ,cast(adjust_min_fee_opt3 as double) as adjust_min_fee_opt3
from dev_vnfdbi_opsndrivers.driver_ops_min_fee_config_adhoc
)
,order_info as 
(select 
        date(dot.delivered_timestamp) as report_date,
        dot.id,
        dot.order_code,
        dot.group_id,
        dot.delivery_id,
        dot.city_name,
        dotet.total_shipping_fee as single_fee,
        dot.shipper_id,
        greatest(adjust_min_fee_opt3,if(order_type = 0,3650,3780)*1*dot.driver_distance) as adjust_min_fee_opt3,
        order_status


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab  dot 

LEFT JOIN (SELECT 
                  order_id,
                  CAST(JSON_EXTRACT(dotet.order_data,'$.delivery.shipping_fee.total') AS DOUBLE) AS total_shipping_fee
        -- ,order_data
    from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da dotet
    where date(dt) = current_date - interval '1' day
          ) dotet on dotet.order_id = dot.delivery_id  
left join config_tab cf
    on coalesce(dot.city_name, 'na') = cf.city_name
where 1 = 1 
and dot.order_type = 0
and dot.shipper_id > 0
and dot.created_date >= date'2024-10-01'
and dot.order_status in ('Delivered','Quit','Returned')

)
,group_info as 
(select 
        gi.id,
        gi.ship_fee as current_group_fee,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        coalesce(greatest
        (least(oi.sum_adjust_min_fee_opt3,greatest(oi.sum_adjust_min_fee_opt3*gi.discount_rate,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee),gi.ship_fee)
        when (larger_fee is null or smaller_fee is null) then
        coalesce(least(
                oi.sum_adjust_min_fee_opt3,greatest(oi.sum_adjust_min_fee_opt3*gi.discount_rate,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
              ),gi.ship_fee)
        else gi.ship_fee end as group_fee_current,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        coalesce(greatest
        (least(oi.sum_adjust_min_fee_opt3,greatest(oi.sum_adjust_min_fee_opt3*gi.discount_rate,greatest(11600,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),gi.ship_fee),
        smaller_fee)
        when (larger_fee is null or smaller_fee is null) then 
        coalesce(least(
                oi.sum_adjust_min_fee_opt3,greatest(oi.sum_adjust_min_fee_opt3*gi.discount_rate,greatest(11600,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*1)) + gi.extra_fee_cal
              ),gi.ship_fee)
        else gi.ship_fee end as group_fee_opt3,
        gi.cnt_order as total_order_in_group

--  Non Hub Stack Shipping Fee = Max( Min(Total Single Fee, Max(Discount Fee, Non Hub Stack Fee) + Non Hub Extra Fee, Orginal Larger Size Stack Fee), Original Smaller Size Stack Fee)

from driver_ops_group_table_temp gi 

left join 
(select 
        group_id,
        sum(single_fee) as sum_single_current,
        sum(adjust_min_fee_opt3) as sum_adjust_min_fee_opt3

from order_info
where group_id > 0 
group by 1 
) oi on oi.group_id = gi.id

where oi.group_id is not null 
)
select 
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
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_opt3*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then group_fee_current*1.00/gi.total_order_in_group
        else adjust_min_fee_opt3 end as shipping_fee_opt3

from order_info oi 

left join group_info gi on gi.id = oi.group_id

where 1 = 1 
and report_date between date'2024-10-01' and current_date - interval '1' day
;
with raw as 
(select 
        report_date,
        shipper_id,
        city_name,
        sum(shipping_fee_shared) as current_ship_shared,
        sum(shipping_fee_opt3) as shipping_fee_opt3

from driver_ops_cal_tab
where city_name not in ('HCM City','Ha Noi City')
group by 1,2,3)
select 
        date_trunc('month',report_date) as month_,
        city_name,
        sum(current_ship_shared)*1.0000/count(distinct (shipper_id,report_date)) as current_ship_shared,
        sum(shipping_fee_opt3)*1.0000/count(distinct (shipper_id,report_date)) as shipping_fee_opt3

from raw

group by 1,2