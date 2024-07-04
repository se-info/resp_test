
--- get order belong to stacked before and current is single
select 
    *
from dev_vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab raw
where date(create_timestamp) = date '2022-12-05'
and is_stack_group_order = 0 --final = single
and assign_type = 7
-- and o_order_list is not null
and ref_order_category != 0
;
--- get order belong to group before and current is single
-- note: only shopee order have group order before -> ref_order_category != 0
select 
distinct status
from dev_vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab raw
-- where date(create_timestamp) = date '2022-12-05'
where 1=1
and is_stack_group_order = 0 --final = single
and first_o_order_list is not null
-- and status in (2,14,15)
-- and o_order_list is not null
-- and ref_order_category = 0
