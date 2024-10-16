with raw as 
(select 
        r.order_code_agg,
        r.city_name, 
        a.group_id,
        a.is_hub,
        a.deal_order_type_txt,
        -- a.details,
        json_extract(a.details,'$.shippingfee_kafka.new_calculation_detail.calculation_detail.process') as stack_shipping_fee_process,
        json_extract(a.details,'$.shippingfee_kafka.new_calculation_detail.calculation_detail.parameters') as stack_shipping_fee_parameters,
        a.dt,
        a.details,
        from_unixtime(cast(_timestamp as bigint) - 3600) as created

from shopeefood_assignment.foodalgo_shippingfee_snapshot__vn_continuous_s0_live a 

left join 
(select 
        group_id,
        city_name,
        array_agg(order_code) as order_code_agg 
from driver_ops_raw_order_tab   
where order_status in ('Delivered','Quit','Returned')
group by 1,2 
) r on r.group_id = cast(a.group_id as bigint)  

where a.api = 'get_group_driver_shippingfee'
and cast(a.group_id as bigint) > 0  
and date(dt) = date'2024-07-18'
and r.group_id is not null
and cast(a."hour" as bigint) >= 17
and cast(json_extract(a.details,'$.shippingfee_kafka.extra_msg.grayscale_toggle') as bigint) = 1 
)
,f as 
(select 
        raw.dt,
        raw.group_id,
        ogi.group_code,
        raw.order_code_agg,
        json_array_length(json_extract(stack_shipping_fee_process,'$.adjust_stack_shippingfee_info')) as adjust_stack_shippingfee_info,
        cast(json_extract(stack_shipping_fee_process,'$.stack_fee') as bigint) as stack_fee,
        cast(json_extract(stack_shipping_fee_process,'$.final_stack_shippingfee') as bigint) as final_stack_shipping_fee,
        cast(json_extract(stack_shipping_fee_process,'$.discount_fee') as bigint) as discount_fee,
        cast(json_extract(stack_shipping_fee_process,'$.adjust_stack_shippingfee_info.input.ori_shippingfee') as bigint) as smaller_fee,
        cast(json_extract(stack_shipping_fee_process,'$.adjust_stack_shippingfee_info.input.cur_shippingfee') as bigint) as larger_fee,
        cast(json_extract(stack_shipping_fee_process,'$.round_stack_shippingfee') as bigint) as round_stack_shippingfee,
        cast(json_extract(stack_shipping_fee_process,'$.extra_fee') as bigint) as extra_fee,
        cast(json_extract(stack_shipping_fee_process,'$.total_single_shippingfee') as bigint) as total_single_shippingfee,
        cast(json_extract(stack_shipping_fee_parameters,'$.shippingfee_config.unit_fee') as bigint) as unit_fee,
        cast(json_extract(stack_shipping_fee_parameters,'$.shippingfee_config.surge_rate') as bigint) as surge_rate,
        cast(json_extract(stack_shipping_fee_parameters,'$.shippingfee_config.extra_pickdrop_fee') as bigint) as extra_pickdrop_fee,
        cast(json_extract(stack_shipping_fee_parameters,'$.shippingfee_config.discount_rate') as bigint) as discount_rate,
        cast(json_extract(stack_shipping_fee_parameters,'$.shippingfee_config.dropoff_cnt') as bigint) as dropoff_cnt,
        cast(json_extract(stack_shipping_fee_parameters,'$.shippingfee_config.pickup_cnt') as bigint) as pickup_cnt,
        cast(json_extract(stack_shipping_fee_parameters,'$.shippingfee_config.min_fee') as bigint) as min_fee,
        cast(json_extract(stack_shipping_fee_parameters,'$.distance') as bigint) as distance,
        ogi.group_fee,
        row_number()over(partition by group_id order by created desc) as rank_


from raw 

left join 
(select id,group_code,ship_fee/100.00 as  group_fee
from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da ogi 
WHERE DATE(dt) = current_date - interval '1' day
) ogi on ogi.id = cast(raw.group_id as bigint)

WHERE CARDINALITY(FILTER(order_code_agg,x -> x is not null)) >= 2
)
select * from f where group_id = '78600454'
-- select *,cast(adjust_stack_shippingfee_info as varchar),cast(adjust_stack_shippingfee_info as varchar) is null from f where group_code = 'D63508440074'
select *
from
(select
        group_code,
        order_code_agg,
        stack_fee as stack_exc_extra,
        final_stack_shipping_fee as final_stack_fee,
    --  Non Hub Stack Shipping Fee = Max( Min(Total Single Fee, Max(Discount Fee, Non Hub Stack Fee) + Non Hub Extra Fee, Orginal Larger Size Stack Fee), Original Smaller Size Stack Fee)
        case 
        when cast(adjust_stack_shippingfee_info as varchar) is not null then
            greatest(
                least(coalesce(total_single_shippingfee,0),
                    (greatest(coalesce(discount_fee,0),
                                coalesce(round_stack_shippingfee,0)) + 
                                (coalesce(extra_pickdrop_fee,0) * (coalesce(pickup_cnt,0) ) + coalesce(extra_pickdrop_fee,0) * (coalesce(dropoff_cnt,0) ) )
                                ), 
                                coalesce(larger_fee,0)),
                coalesce(smaller_fee,0)
                    ) 
        else 
                least(coalesce(total_single_shippingfee,0),
                    (greatest(coalesce(discount_fee,0),
                                coalesce(round_stack_shippingfee,0)) + 
                                (coalesce(extra_pickdrop_fee,0) * (coalesce(pickup_cnt,0) ) + coalesce(extra_pickdrop_fee,0) * (coalesce(dropoff_cnt,0) ) )
                                )
                    ) end as re_calculation,
        case 
        when cast(adjust_stack_shippingfee_info as varchar) is not null then
            greatest(
                least(coalesce(total_single_shippingfee,0),
                    (greatest(coalesce(discount_fee,0),
                                coalesce(round_stack_shippingfee,0)) 
                                ), 
                                coalesce(larger_fee,0)),
                coalesce(smaller_fee,0)
                    ) 
        else 
                least(coalesce(total_single_shippingfee,0),
                    (greatest(coalesce(discount_fee,0),
                                coalesce(round_stack_shippingfee,0)) 
                                )
                    ) end as re_calculation_exclude_extra
        -- least(coalesce(total_single_shippingfee,0),
        --             (greatest(coalesce(discount_fee,0),
        --                         coalesce(group_fee,0)) + coalesce(extra_fee,0))
        --             ) as ve2

from f
where rank_ = 1
)
where final_stack_fee != re_calculation
