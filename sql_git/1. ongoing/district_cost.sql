with base as 
(select 
        bf.grass_date,
        bf.order_id,
        bf.partner_id,
        di.name_en as district_name,
        bf.delivered_by,
        case when bf.grass_date <= date '2024-07-31' then bf.is_stack_group_order else god.is_actual_stack_group_order end as is_stack_group_order_new,
        (driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd,
        (driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd,
        (case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)  /exchange_rate as dr_cost_bonus_usd,
        source

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 

-- left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_daily_s0_live ho 
--         on ho.ref_order_id = bf.order_id 
--         and ho.ref_order_category = bf.ref_order_category

-- left join driver_ops_hub_driver_performance_tab hub 
--         on hub.slot_id = ho.slot_id 
--         and bf.partner_id = hub.uid 
--         and bf.grass_date = hub.date_ 
--         and hub.total_order > 0 

left join dev_vnfdbi_opsndrivers.shopeefood_bi_group_order_detail_tab god
    on bf.group_id = god.group_id and bf.order_id = god.ref_order_id


left join 
(select id,name_en
from shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live) di on di.id = bf.pick_district_id and di.province_id = bf.pick_city_id

-- where delivered_by = 'hub'
where 1 = 1 
and delivered_by = 'hub' 
and (new_driver_tier_v2 is null or new_driver_tier_v2 = 'HUB_OTH' or new_driver_tier_v2 = 'HUB03' or new_driver_tier_v2 = 'HUB05')
and bf.grass_date >= date'2024-08-01'
and bf.grass_date <= date'2024-08-03'
and bf.source = 'Food'
and bf.pick_district_id in (1,2,5,7,8,10,12,13,14,15,16,17,18,19,20,21,22,23,24,27,28,29,67,467,690,693,945)
)
,summary as 
(select 
        grass_date,
        source,
        district_name,
        delivered_by,
        order_type,
        sum(dr_cost_base_usd + dr_cost_surge_usd) base_surge,
        sum(dr_cost_bonus_usd) dr_cost_bonus_usd,
        count(distinct order_id) as total_orders,
        count(distinct case when is_stack_group_order_new in (1,2) then order_id else null end) as stacked,
        cast(sum(dr_cost_base_usd + dr_cost_surge_usd + dr_cost_bonus_usd) as double)*(-1) as total_cost


from (select *,if(is_stack_group_order_new in (1,2),'stack','non-stack') as order_type 
from base )

group by 1,2,3,4,5
)
select
        -- grass_date,
        district_name,
        sum(total_orders)/count(distinct grass_date)*1.0000 as total_order,
        sum(case when delivered_by = 'hub' then total_orders else null end)/count(distinct grass_date)*1.0000 as hub_ado,
        -- sum(case when delivered_by != 'hub' then total_orders else null end)/count(distinct grass_date)*1.0000 as non_hub_ado,
        sum(stacked)/count(distinct grass_date)*1.0000as stacked,
        -- sum(case when delivered_by = 'hub' then stacked else null end)/count(distinct grass_date)*1.0000 as hub_stacked,
        -- sum(case when delivered_by != 'hub' then stacked else null end)/count(distinct grass_date)*1.0000 as non_hub_stacked,
        sum(case when source = 'Food' then total_cost else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_cpo,
        sum(case when source = 'Food' and delivered_by = 'hub' then total_cost else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' then total_orders else 0 end) as food_hub_cpo,
        sum(case when source = 'Food' and delivered_by != 'hub' then total_cost else 0 end) / sum(case when source = 'Food'  and delivered_by != 'hub' then total_orders else 0 end) as food_non_hub_cpo,
        sum(case when source = 'Food' and delivered_by = 'hub' and order_type = 'stack' then total_cost else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_hub_cpo,
        sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_cost else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_non_hub_cpo,
        sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_cost else 0 end) / sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_hub_cpo,
        sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_cost else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_non_hub_cpo

from summary 
group by 1