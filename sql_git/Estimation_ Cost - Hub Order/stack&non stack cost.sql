with bill_fee_base as
(select
    *

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
where 1=1
and date_ between date_trunc('month', current_date - interval '1' day) - interval '1' month and current_date - interval '1' day
and source in ('Food','Market')
),order_stacking_and_hub_raw as
(select 
    date_trunc('month',grass_date) as report_month
    ,case when source in ('Food','Market') then 'Food' else 'Ship' end as source
    ,delivered_by
    ,case when is_stack_group_order in (1,2) then 'stack' else 'non-stack' end as order_type
    ,cast(count(distinct order_id) as double) as total_orders
    ,cast(sum(dr_cost_base_usd + dr_cost_surge_usd + dr_cost_bonus_usd) as double)*(-1) as total_cost

    ,sum(total_shipping_fee/exchange_rate) total_cost_before_stack

    ,sum(case 
        when delivered_by = 'non-hub' and is_stack_group_order in (1,2) then driver_cost_base_n_surge/exchange_rate        
        else total_shipping_fee/exchange_rate end) as total_cost_after_stack
    

    ,sum(case 
        when delivered_by = 'non-hub' and is_stack_group_order in (1,2) then driver_cost_base_n_surge/exchange_rate
        else total_shipping_fee/exchange_rate end)
    - 
    sum(total_shipping_fee/exchange_rate) as total_cost_saving

from bill_fee_base
group by 1,2,3,4
)
,order_stacking_and_hub_raw_flattern as
(select 
    report_month
    -- Food
    -- % ADO Food
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_stack_hub_dist
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_stack_non_hub_dist
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_non_stack_hub_dist
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_non_stack_non_hub_dist

    -- % ADO Ship
    ,sum(case when source = 'Ship' and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) / sum(case when source = 'Ship' then total_orders else 0 end) as ship_stack_hub_dist
    ,sum(case when source = 'Ship' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) / sum(case when source = 'Ship' then total_orders else 0 end) as ship_stack_non_hub_dist
    ,sum(case when source = 'Ship' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) / sum(case when source = 'Ship' then total_orders else 0 end) as ship_non_stack_hub_dist
    ,sum(case when source = 'Ship' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) / sum(case when source = 'Ship' then total_orders else 0 end) as ship_non_stack_non_hub_dist

    -- CPO Food
    ,sum(case when source = 'Food' then total_cost else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_cpo
    ,sum(case when source = 'Food' and delivered_by = 'hub' then total_cost else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' then total_orders else 0 end) as food_hub_cpo
    ,sum(case when source = 'Food' and delivered_by != 'hub' then total_cost else 0 end) / sum(case when source = 'Food'  and delivered_by != 'hub' then total_orders else 0 end) as food_non_hub_cpo
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type = 'stack' then total_cost else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_hub_cpo
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_cost else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_non_hub_cpo
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_cost else 0 end) / sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_hub_cpo
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_cost else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_non_hub_cpo

    -- CPO Ship
    ,sum(case when source = 'Ship' and delivered_by = 'hub' and order_type = 'stack' then total_cost else 0 end) / sum(case when source = 'Ship' and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) as ship_stack_hub_cpo
    ,sum(case when source = 'Ship' and delivered_by != 'hub' and order_type = 'stack' then total_cost else 0 end) / sum(case when source = 'Ship' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as ship_stack_non_hub_cpo
    ,sum(case when source = 'Ship' and delivered_by = 'hub' and order_type != 'stack' then total_cost else 0 end) / sum(case when source = 'Ship'  and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) as ship_non_stack_hub_cpo
    ,sum(case when source = 'Ship' and delivered_by != 'hub' and order_type != 'stack' then total_cost else 0 end) / sum(case when source = 'Ship' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as ship_non_stack_non_hub_cpo

    -- CPO Food saving
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type = 'stack' then total_cost_saving else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_hub_cpo_saving
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_cost_saving else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as food_stack_non_hub_cpo_saving
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_cost_saving else 0 end) / sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_hub_cpo_saving
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_cost_saving else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as food_non_stack_non_hub_cpo_saving

    -- CPO Ship
    ,sum(case when source = 'Ship' and delivered_by = 'hub' and order_type = 'stack' then total_cost_saving else 0 end) / sum(case when source = 'Ship' and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) as ship_stack_hub_cpo_saving
    ,sum(case when source = 'Ship' and delivered_by != 'hub' and order_type = 'stack' then total_cost_saving else 0 end) / sum(case when source = 'Ship' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as ship_stack_non_hub_cpo_saving
    ,sum(case when source = 'Ship' and delivered_by = 'hub' and order_type != 'stack' then total_cost_saving else 0 end) / sum(case when source = 'Ship'  and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) as ship_non_stack_hub_cpo_saving
    ,sum(case when source = 'Ship' and delivered_by != 'hub' and order_type != 'stack' then total_cost_saving else 0 end) / sum(case when source = 'Ship' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as ship_non_stack_non_hub_cpo_saving



from order_stacking_and_hub_raw
group by 1
)
select 
    *
from order_stacking_and_hub_raw_flattern
;