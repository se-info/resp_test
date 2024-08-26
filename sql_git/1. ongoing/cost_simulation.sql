with driver_cost_base as 
(select 
    bf.*                                       
    ,(driver_cost_base + return_fee_share_basic) + (driver_cost_surge + return_fee_share_surge) as base_surge_vnd
    ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
    ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)  /exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
                                                                                                                   
WHERE grass_date != date'2023-06-06'
AND status = 7
AND source in ('Food')
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
        greatest(12000,if(order_type = 0,3750,3780)*1*dot.driver_distance) as recal_single_fee,
        order_status,
        dot.source


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
and city_name not in ('HCM City','Ha Noi City')
-- and city_name = 'Hai Phong City'
and dot.order_status in ('Delivered','Quit','Returned')
and dot.driver_policy != 2 
-- and source = 'order_food'
)
,group_info as 
(select 
        gi.id,
        gi.ship_fee as current_group_fee,
        gi.ship_cal as group_re_cal,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_single_current,greatest(oi.sum_single_current*gi.discount_rate,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_single_current,greatest(oi.sum_single_current*gi.discount_rate,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
              ) end as group_fee_current,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_single_current,greatest(oi.sum_single_current*0.6,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_single_current,greatest(oi.sum_single_current*0.6,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
              ) end as group_fee_opt1,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_single_current,greatest(oi.sum_single_current*0.58,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_single_current,greatest(oi.sum_single_current*0.58,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
              ) end as group_fee_opt2,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_single_current,greatest(oi.sum_single_current*0.55,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_single_current,greatest(oi.sum_single_current*0.55,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
              ) end as group_fee_opt3,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_single_current,greatest(oi.sum_single_current*0.53,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_single_current,greatest(oi.sum_single_current*0.53,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
              ) end as group_fee_opt4,
        case 
        when (larger_fee is not null or smaller_fee is not null) then 
        greatest
        (least(oi.sum_single_current,greatest(oi.sum_single_current*0.5,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
                ,coalesce(larger_fee,0)
              ),
        smaller_fee)
        else least(
                oi.sum_single_current,greatest(oi.sum_single_current*0.5,greatest(gi.min_fee,if(gi.ref_order_category=0,3750,3780)*gi.distance/100000*gi.surge_rate)) + gi.extra_fee_cal
              ) end as group_fee_opt5,
        gi.cnt_order as total_order_in_group

--  Non Hub Stack Shipping Fee = Max( Min(Total Single Fee, Max(Discount Fee, Non Hub Stack Fee) + Non Hub Extra Fee, Orginal Larger Size Stack Fee), Original Smaller Size Stack Fee)

from driver_ops_group_table_temp gi 

left join 
(select 
        group_id,
        sum(recal_single_fee) as sum_single_current

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
        sum(case when group_id > 0 then shipping_fee_shared else null end) as total_shipping_fee_current_stack,
        sum(case when group_id = 0 then shipping_fee_shared else null end) as total_shipping_fee_current_non_stack,

        sum(shipping_fee_shared_op1) as shipping_fee_shared_op1,
        sum(case when group_id > 0 then shipping_fee_shared_op1 else null end) as shipping_fee_shared_op1_stack,
        sum(case when group_id = 0 then shipping_fee_shared_op1 else null end) as shipping_fee_shared_op1_non_stack,

        sum(shipping_fee_shared_op2) as shipping_fee_shared_op2,
        sum(case when group_id > 0 then shipping_fee_shared_op2 else null end) as shipping_fee_shared_op2_stack,
        sum(case when group_id = 0 then shipping_fee_shared_op2 else null end) as shipping_fee_shared_op2_non_stack,

        sum(shipping_fee_shared_op3) as shipping_fee_shared_op3,
        sum(case when group_id > 0 then shipping_fee_shared_op3 else null end) as shipping_fee_shared_op3_stack,
        sum(case when group_id = 0 then shipping_fee_shared_op3 else null end) as shipping_fee_shared_op3_non_stack,

        sum(shipping_fee_shared_op4) as shipping_fee_shared_op4,
        sum(case when group_id > 0 then shipping_fee_shared_op4 else null end) as shipping_fee_shared_op4_stack,
        sum(case when group_id = 0 then shipping_fee_shared_op4 else null end) as shipping_fee_shared_op4_non_stack,

        sum(shipping_fee_shared_op5) as shipping_fee_shared_op5,
        sum(case when group_id > 0 then shipping_fee_shared_op5 else null end) as shipping_fee_shared_op5_stack,
        sum(case when group_id = 0 then shipping_fee_shared_op5 else null end) as shipping_fee_shared_op5_non_stack
from
(select 
        oi.report_date,
        oi.order_code,
        oi.city_name,
        oi.group_id,
        oi.shipper_id,
        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_current*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then single_fee
        else single_fee end as shipping_fee_shared,
        
        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_opt1*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then single_fee
        else single_fee end as shipping_fee_shared_op1,
        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_opt2*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then single_fee
        else single_fee end as shipping_fee_shared_op2,
        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_opt3*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then single_fee
        else single_fee end as shipping_fee_shared_op3,
        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_opt4*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then single_fee
        else single_fee end as shipping_fee_shared_op4,
        case 
        when oi.group_id > 0 and gi.total_order_in_group > 1 then group_fee_opt5*1.00/gi.total_order_in_group
        when oi.group_id > 0 and gi.total_order_in_group = 1 then single_fee
        else single_fee end as shipping_fee_shared_op5
        -- ,db.base_surge_vnd

from order_info oi 

left join group_info gi on gi.id = oi.group_id

left join driver_cost_base db on db.order_id = oi.id 

where 1 = 1 
and oi.report_date between date'2024-08-12' and current_date - interval '1' day
and oi.order_status = 'Delivered'
and oi.source = 'order_food'
)

group by 1,2