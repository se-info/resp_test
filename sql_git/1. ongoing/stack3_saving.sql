-- dev_vnfdbi_opsndrivers.shopeefood_bi_group_order_detail_tab
-- bf.group_id = god.group_id and bf.order_id = god.ref_order_id
select
year(bf.grass_date)*100 + week(bf.grass_date) report_week
-- ,bf.group_id
,case 
when bf.group_id is null then 'single'
when bf.group_id > 0 and god.is_stack_group_order = 2  and god.cnt_order_in_group > 2 then 'multi-stack'
when bf.group_id > 0 and god.is_stack_group_order = 2  and god.cnt_order_in_group <= 2 then 'normal-stack'
when bf.group_id > 0 and god.is_stack_group_order != 2  then 'group' end as order_assign_type
,count(distinct bf.order_id) net_order
,sum(bf.total_shipping_fee) as before_stack_shipping_fee
,sum(bf.driver_cost_base_n_surge) as after_stack_shipping_fee
,(sum(bf.driver_cost_base_n_surge) - sum(bf.total_shipping_fee))/count(distinct bf.order_id) vnd_saving_per_order

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 

left join dev_vnfdbi_opsndrivers.shopeefood_bi_group_order_detail_tab god 
        on bf.group_id = god.group_id and bf.order_id = god.ref_order_id

where 1=1
and delivered_by = 'non-hub'
and source = 'Food'
and date_ >= current_date - interval '7' day
group by 1,2