with driver_cost_raw AS (
    SELECT bf.*
        ,(CASE 
WHEN is_nan(bf.bonus) = true THEN 0.00 
WHEN bf.delivered_by = 'hub' THEN bonus_hub
WHEN bf.delivered_by != 'hub' THEN bonus_non_hub
ELSE null end)*1.000000  /bf.exchange_rate as bonus_usd_all
        , (bf.driver_cost_base + bf.return_fee_share_basic)*1.000000  /bf.exchange_rate as total_driver_cost_base_all
        , (bf.driver_cost_surge + bf.return_fee_share_surge)*1.000000  /bf.exchange_rate as total_driver_cost_surge_all
        ,(CASE 
WHEN is_nan(bf.bonus_v2) = true THEN 0.00 
WHEN bf.delivered_by = 'hub' THEN bf.bonus_hub_v2
WHEN bf.delivered_by != 'hub' THEN bf.bonus_non_hub_v2
ELSE null end)*1.000000  /bf.exchange_rate as bonus_usd_all_v2
        , (bf.driver_cost_base_v2 + bf.return_fee_share_basic)*1.000000  /bf.exchange_rate as total_driver_cost_base_all_v2
        , (bf.driver_cost_surge_v2 + bf.return_fee_share_surge)*1.000000  /bf.exchange_rate as total_driver_cost_surge_all_v2
        , CASE 
               WHEN distance <= 3 then '1. 0 - 3km'
               WHEN distance > 3 then '2. > 3km'
               END AS distance_range

FROM vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf

-- LEFT JOIN vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee bf_2 
--     on bf_2.order_id = bf.order_id 
--     and bf_2.grass_date = bf.grass_date 
--     and bf_2.source = bf.source 
WHERE grass_date >= date'2023-01-01'
and source in ('Food','Market')
)
select 
     year_week
    -- ,distance_range
    ,case 
        when delivered_by = 'hub' and new_driver_tier_v2 is not null then new_driver_tier_v2
        when delivered_by = 'hub' and (new_driver_tier_v2 is null or new_driver_tier_v2 = 'HUB_OTH') then 'HUB03'
        when delivered_by != 'hub' and lower(new_driver_tier_v2) like '%hub%' then 'T1'
        when delivered_by != 'hub' and new_driver_tier_v2 is null then 'part_time'
        else new_driver_tier_v2 end driver_tier
    ,sum(total_driver_cost_base_all + total_driver_cost_surge_all + bonus_usd_all)/count(distinct order_id) as cpo_v1
    ,sum(total_driver_cost_base_all_v2 + total_driver_cost_surge_all_v2 + bonus_usd_all_v2)/count(distinct order_id) as cpo_v2
    ,count(distinct order_id) as total_orders
    ,count(distinct case when is_stack_group_order = 2 then order_id else null end) as stack_order

from driver_cost_raw
group by 1,2
