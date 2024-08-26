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
    ,god.total_single_shipping_fee
    ,god.single_shipping_fee
    ,god.single_distance
    ,god.total_single_distance
    ,god.cnt_order_in_group
    ,di.name_en as district_name
    ,if((r.hub_id > 0 or driver_policy = 2),1,0) as hub_order
    ,r.group_id as g2
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join dev_vnfdbi_opsndrivers.shopeefood_bi_group_order_detail_tab god
    on bf.group_id = god.group_id and bf.order_id = god.ref_order_id

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = bf.pick_district_id and di.province_id = bf.pick_city_id

left join driver_ops_raw_order_tab r on r.id = bf.order_id and r.order_type = bf.ref_order_category

where bf.grass_date = date'2024-08-19'
and bf.source = 'Food'
and bf.city_name_full in ('HCM City','Ha Noi City','Hai Phong City')
) 
select
        district_name,
        if(distance <= 3,'1. <=3km','2. ++3km') as distance_range,
        case when group_shipping_fee < 13500*cnt_order_in_group then 1 else 0 end as is_impacted_orders,
        delivered_by,
        count(order_id) as ado,
        count(distinct case when hub_order = 1 then order_id else null end)/count(distinct grass_date)*1.0000 as hub_order,
        count(distinct case when delivered_by = 'hub' then order_id else null end)/count(distinct grass_date)*1.0000 as hub_delivered,
        count(distinct case when delivered_by = 'hub' and is_stack_group_order_new in (1,2) then order_id else null end)/count(distinct grass_date)*1.0000 as hub_stack,
        sum(case when delivered_by = 'hub' and group_shipping_fee < 13500*cnt_order_in_group then 13500.000/exchange_rate else total_shipping_fee/exchange_rate end) as driver_cost_before_stack,
        sum((case when is_stack_group_order_new in (1,2) then 1.0000*group_shipping_fee * (single_shipping_fee*1.000 / total_single_shipping_fee) else 13500.000 end) / exchange_rate) as driver_cost_after_stack
 

from driver_cost_base
group by 1,2,3,4


