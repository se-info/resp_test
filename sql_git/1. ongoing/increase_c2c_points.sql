with raw as 
(select 
        a.id,
        a.order_code,
        a.group_id,
        a.order_type,
        if(dp.shipper_type=11,'non-hub','hub') as working_group,
        dp.city_name,
        dp.city_id,
        dp.shipper_tier,
        case when a.city_id = dp.city_id then p.point
        else 0 end as original_point,
        a.shipper_id,
        date(a.delivered_timestamp) as report_date,
        case 
        when a.order_type = 0 then doet.merchant_paid_amount
        when a.order_type = 6 then doet.cod_value
        else doet.item_value end as value_range

from (select raw.*,if(raw.order_type != 0,1,coalesce(is_foody_delivery,0)) as filter_delivery,merchant_paid_amount
from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 
left join (select id,is_foody_delivery,if(merchant_paid_status =1,merchant_paid_amount*1.00/100,0) as merchant_paid_amount
           from shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
           where date(dt) = current_date - interval '1' day
           ) oct 
                on raw.id = oct.id
) a  

left join driver_ops_driver_performance_tab dp 
        on dp.shipper_id = a.shipper_id
        and dp.report_date = date(a.delivered_timestamp)

left join shopeefood.foody_partner_db__order_point_log_tab__reg_daily_s0_live p 
        on p.order_id = a.id
        and p.order_type = a.order_type

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
        coalesce(json_extract(order_data,'$.booking.other_fees'),null) as other_fees,
        case 
        when cast(json_extract(order_data,'$.delivery.merchant_paid_status') as bigint) = 1 then
             cast(json_extract(order_data,'$.delivery.merchant_paid_amount') as bigint) 
             else 0 end as merchant_paid_amount

from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da 
where date(dt) = current_date - interval '1' day
    ) doet 
    on a.delivery_id = doet.order_id
where 1 = 1 
and a.shipper_id > 0 
and a.order_status in ('Delivered','Quit')
)
,f as 
(select 
        raw.report_date,
        raw.shipper_id,
        raw.working_group,
        raw.city_name,
        raw.city_id,
        raw.shipper_tier,
        raw.order_code,
        raw.original_point,
        case 
        when order_type = 6 then raw.original_point
        when value_range <= 99999 then 10
        when value_range <= 199999 then 14
        when value_range <= 999999 then 16
        when value_range >= 1000000 then 18 
        end as new_point,
        greatest(raw.original_point,
                 case 
                when order_type = 6 then raw.original_point
                when value_range <= 99999 and original_point > 0 then 10
                when value_range <= 199999 and original_point > 0 then 14
                when value_range <= 999999 and original_point > 0 then 16
                when value_range >= 1000000 and original_point > 0 then 18 
                end) as final_point

from raw 
)
,m as 
(select 
        f.*,
        cast(i.bonus as double) as current_bonus,
        cast(i2.bonus as double) as new_bonus

from
(select
        report_date,
        shipper_id,
        city_name,
        city_id,
        shipper_tier,
        count(distinct order_code) as total_order,
        sum(original_point) as current_point,
        sum(final_point) as new_point

from f 

where report_date between current_date - interval '7' day and current_date - interval '1' day 
and working_group != 'hub'
and city_id in (217,218)
group by 1,2,3,4,5
) f 

left join (select *,
                case 
                when current_driver_tier = 'T1' then 'Level 1'
                when current_driver_tier = 'T2' then 'Level 2'
                when current_driver_tier = 'T3' then 'Level 3'
                when current_driver_tier = 'T4' then 'Level 4'
                when current_driver_tier = 'T5' then 'Level 5' end as tier_mapping
         from dev_vnfdbi_opsndrivers.driver_ops_bonus_config_tab) i 
        on f.shipper_tier = i.tier_mapping
        and f.current_point between cast(i.from_ as bigint) and cast(i.to_ as bigint)

left join (select *,
                case 
                when current_driver_tier = 'T1' then 'Level 1'
                when current_driver_tier = 'T2' then 'Level 2'
                when current_driver_tier = 'T3' then 'Level 3'
                when current_driver_tier = 'T4' then 'Level 4'
                when current_driver_tier = 'T5' then 'Level 5' end as tier_mapping
         from dev_vnfdbi_opsndrivers.driver_ops_bonus_config_tab) i2 
        on f.shipper_tier = i2.tier_mapping
        and f.new_point between cast(i2.from_ as bigint) and cast(i2.to_ as bigint)
)
select 
        m.report_date,
        m.city_name,
        m.shipper_tier,
        sum(m.current_bonus) as current_bonus,
        sum(m.new_bonus) as new_bonus

from m 
where current_bonus > 0 
-- and (current_bonus > new_bonus)
-- limit 100
group by 1,2,3


