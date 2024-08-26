with driver_cost_base as 
(select 
    bf.*
    ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
    ,(case 
        when is_nan(bonus) = true then 0.00 

        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)  /exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee_usd

    -- ,from_unixtime(oct.final_delivered_time-3600) as delivered_timestamp
    -- ,hour(from_unixtime(oct.final_delivered_time-3600)) as delivered_hour
--     ,case when m.order_id is not null then 1 else 0 end as is_hub_surge_campaign
--     ,coalesce(m.diff,0) /exchange_rate as surge_fee_hub_cp
--     ,dotet.total_shipping_fee
--     ,dotet.unit_fee
--     ,dotet.min_fee
--     ,dotet.surge_rate
--     ,case 
--         when bf.city_name in ('HCM', 'HN') then 13500 
--         when bf.city_name in ('HP') then 12000
--         else  dotet.min_fee end as min_fee_normal

    ,case when bf.grass_date <= date '2024-07-31' then bf.is_stack_group_order else god.is_actual_stack_group_order end as is_stack_group_order_new
    ,god.group_shipping_fee
    ,god.single_shipping_fee
    ,god.single_distance
    ,god.total_single_distance
    ,god.cnt_order_in_group
    ,god.total_single_shipping_fee
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join dev_vnfdbi_opsndrivers.shopeefood_bi_group_order_detail_tab god
    on bf.group_id = god.group_id and bf.order_id = god.ref_order_id
where bf.grass_date = date '2024-08-19'
and source = 'Food'
and delivered_by = 'hub'
)

select
        delivered_by,
        if(distance <= 3,'1. <= 3km','2 > 3km') as distance_range                                                                                    
        ,case when group_shipping_fee < 13500*cnt_order_in_group then 1 else 0 end as is_impacted_orders
        ,sum(case when delivered_by = 'hub' then 13500.000/exchange_rate else total_shipping_fee/exchange_rate end) as driver_cost_before_stack
        ,sum((case when is_stack_group_order_new in (1,2) then 1.0000*group_shipping_fee * (single_shipping_fee*1.000 / total_single_shipping_fee) else 13500.000 end) / exchange_rate) as driver_cost_after_stack
        ,sum(dr_cost_base_usd + dr_cost_surge_usd) base_surge
        ,sum(dr_cost_bonus_usd) dr_cost_bonus_usd
        ,count(distinct order_id) as total_orders
        ,count(distinct case when single_distance <= 3 then order_id else null end) as total_orders_less_3km

from driver_cost_base
group by 1,2,3;