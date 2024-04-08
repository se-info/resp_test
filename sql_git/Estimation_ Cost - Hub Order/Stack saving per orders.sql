select
year(grass_date)*100 + week(grass_date) report_week
,city_name
,count(distinct order_id) net_order
,sum(total_shipping_fee) as before_stack_shipping_fee
,sum(driver_cost_base_n_surge) as after_stack_shipping_fee
,(sum(driver_cost_base_n_surge) - sum(total_shipping_fee))/count(distinct order_id) vnd_saving_per_order

from shopeefood_vn_bnp_bill_fee_and_bonus_order_level

where 1=1
and delivered_by = 'non-hub'
and source = 'Food'
and date_ between date '2022-10-10' and date '2022-11-06'
group by 1,2
