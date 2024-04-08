with bill_fee_base as
(select
    *

    ,(driver_cost_base + return_fee_share_basic) as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge) as dr_cost_surge_usd
    ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)   as dr_cost_bonus_usd
    ,case when distance < 3.7 then '1. 0 -3.6km' when distance <=5 then '2. 3.7 - 5km' when distance > 5 then '3. ++5km' end as distance_range
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
where 1=1
and source in ('Food','Market')
and status = 7
and grass_date >= date'2023-10-02'
and grass_date <= date'2023-10-08'
)
-- SELECT delivered_by,COUNT(DISTINCT CASE WHEN is_stack_group_order IN(1,2) THEN order_id ELSE NULL END)*1.00/COUNT(DISTINCT order_id) FROM bill_fee_base GROUP BY 1
,order_stacking_and_hub_raw as
(select 
    date_trunc('month',grass_date) as report_month
    ,case when source in ('Food','Market') then 'Food' else 'Ship' end as source
    ,delivered_by
    ,city_name
    ,city_name_full
    ,case when is_stack_group_order in (1,2) then 'stack' else 'non-stack' end as order_type
    ,distance_range
    ,exchange_rate
    ,cast(count(distinct order_id) as double) as total_orders
    ,cast(sum(dr_cost_base_usd + dr_cost_surge_usd + dr_cost_bonus_usd) as double)*(-1) as total_cost
    ,cast(sum(dr_cost_base_usd + dr_cost_surge_usd ) as double)*(-1) as total_cost_exclude_bonus
    ,cast(sum(dr_cost_bonus_usd) as double)*(-1) AS dr_cost_bonus_usd
    ,sum(distance) AS distance
    ,sum(total_shipping_fee/exchange_rate) total_cost_before_stack

    ,sum(case 
        when delivered_by = 'non-hub' and is_stack_group_order in (1,2) then driver_cost_base_n_surge/exchange_rate        
        else total_shipping_fee/exchange_rate end) as total_cost_after_stack
    

    ,sum(case 
        when delivered_by = 'non-hub' and is_stack_group_order in (1,2) then driver_cost_base_n_surge/exchange_rate
        else total_shipping_fee/exchange_rate end)
    - 
    sum(total_shipping_fee/exchange_rate) as total_cost_saving
    ,COUNT(DISTINCT CASE WHEN group_id > 0 THEN group_id ELSE NULL END) AS cnt_group

from bill_fee_base
-- where city_name_full IN ('HCM City','Ha Noi City')
group by 1,2,3,4,5,6,7,8
)
select 
    report_month,
    distance_range,
    exchange_rate
    -- Food
    -- % ADO Food
    , sum(case when source = 'Food' then total_orders else 0 end) as ado_food
    , sum(case when source = 'Food' and delivered_by = 'hub' then total_orders else 0 end) as ado_food_hub
    , sum(case when source = 'Food' and delivered_by != 'hub' then total_orders else 0 end) as ado_food_non_hub
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end)  as food_stack_hub_dist
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_non_hub_dist
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_hub_dist
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_non_hub_dist

    -- CPO Food
    ,sum(case when source = 'Food' then total_cost else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_cpo
    ,sum(case when source = 'Food' then total_cost else 0 end)/7
    ,sum(case when source = 'Food' and delivered_by = 'hub' then total_cost else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' then total_orders else 0 end) as food_hub_cpo
    ,sum(case when source = 'Food' and delivered_by != 'hub' then total_cost else 0 end) / sum(case when source = 'Food'  and delivered_by != 'hub' then total_orders else 0 end) as food_non_hub_cpo
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type = 'stack' then total_cost else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_hub_cpo
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_cost else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_non_hub_cpo
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_cost else 0 end) / sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_hub_cpo
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_cost else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_non_hub_cpo

    --Base
    ,sum(case when source = 'Food' then total_cost_exclude_bonus else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_base
    ,sum(case when source = 'Food' and delivered_by = 'hub' then total_cost_exclude_bonus else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' then total_orders else 0 end) as food_hub_base
    ,sum(case when source = 'Food' and delivered_by != 'hub' then total_cost_exclude_bonus else 0 end) / sum(case when source = 'Food'  and delivered_by != 'hub' then total_orders else 0 end) as food_non_hub_base
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type = 'stack' then total_cost_exclude_bonus else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_hub_base
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_cost_exclude_bonus else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_non_hub_base
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_cost_exclude_bonus else 0 end) / sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_hub_base
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_cost_exclude_bonus else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_non_hub_base

    --Bonus
    ,sum(case when source = 'Food' then dr_cost_bonus_usd else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_bonus
    ,sum(case when source = 'Food' and delivered_by = 'hub' then dr_cost_bonus_usd else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' then total_orders else 0 end) as food_hub_bonus
    ,sum(case when source = 'Food' and delivered_by != 'hub' then dr_cost_bonus_usd else 0 end) / sum(case when source = 'Food'  and delivered_by != 'hub' then total_orders else 0 end) as food_non_hub_bonus
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type = 'stack' then dr_cost_bonus_usd else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_hub_bonus
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then dr_cost_bonus_usd else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_non_hub_bonus
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then dr_cost_bonus_usd else 0 end) / sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_hub_bonus
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then dr_cost_bonus_usd else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_non_hub_bonus

    ,sum(total_cost) as cost 
    ,sum(total_cost_exclude_bonus) as base 
    ,sum(dr_cost_bonus_usd) as bonus


from order_stacking_and_hub_raw

group by 1,2,3 


ORDER BY 1,2 DESC 
LIMIT 100


