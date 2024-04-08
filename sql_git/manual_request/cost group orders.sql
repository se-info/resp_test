with group_base as
(select 
    order_id
    ,distance
    ,group_id
    ,is_stack_group_order
    ,order_in_groups
    ,total_shipping_fee_surge + total_shipping_fee_basic as base_surge_before_group
    ,driver_cost_base_n_surge as base_surge_after_group
    ,exchange_rate
    ,date_ 
    
    
from shopeefood_vn_bnp_bill_fee_and_bonus_order_level
where (date_ between date '2022-08-26' and date'2022-08-27'
       or
       date_ between date '2022-08-19' and date'2022-08-20')
and delivered_by = 'non-hub'
and is_stack_group_order in (0,1,2)
and source in ('Food','Market')
)
-- select 
--     group_id
--     ,sum(base_surge_before_group)
--     ,sum(base_surge_after_group)
-- from group_base
-- group by 1
-- having sum(base_surge_after_group) <sum(base_surge_before_group)
-- and count(distinct order_id) > 2

-- select 
--     *
-- from shopeefood_vn_bnp_bill_fee_and_bonus_order_level
-- where group_id = 29573854

select 
     date_ 
    ,case 
        when is_stack_group_order = 0 or (is_stack_group_order in (1,2) and coalesce(order_in_groups,1) = 1) then 'Single' 
        when is_stack_group_order = 1 then 'Group'
        when is_stack_group_order = 2 then 'Stack'
        end as order_assignment_type
    ,coalesce(order_in_groups,1) as order_in_group
    ,sum(base_surge_before_group)/exchange_rate base_surge_before_group_usd
    ,sum(base_surge_after_group)/exchange_rate base_surge_after_group_usd
    ,count(distinct order_id) as total_orders
    ,(sum(base_surge_after_group) - sum(base_surge_before_group))/exchange_rate / count(distinct order_id) as saving_cpo

from group_base
group by 1,2,3,exchange_rate

-- and group_id = 29558624
-- limit 100

-- select 
--     is_group_order
--     ,*
-- from shopeefood_vn_bnp_order_performance_dev
-- where group_id = 29558624
-- limit 10
