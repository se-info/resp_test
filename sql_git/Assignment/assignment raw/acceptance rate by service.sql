with base_assignment as
(
select
date(create_timestamp) created_date
        ,ref_order_category
        ,order_uid
        ,shipper_uid
        ,create_timestamp
        ,status
from dev_vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab raw
where status in (3,4,2,14,15,8,9,17,18) 
and date(create_timestamp) = date '2022-12-05'
)



select 
    created_date
    ,case 
when ref_order_category = 0 then 'Food'
when ref_order_category != 0 then 'Ship'
end as service
    ,count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end) as no_assign
    ,count(distinct case when status in (3,4) then (shipper_uid,order_uid,create_timestamp) else null end) as no_incharged
    ,count(distinct case when status in (8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end) as no_ignored
    ,count(distinct case when status in (2,14,15) then (shipper_uid,order_uid,create_timestamp) else null end) as no_deny
from base_assignment
group by 1,2
;
select 
    date(create_timestamp)
    ,is_stack_group_order
    ,case 
when ref_order_category = 0 then 'Food'
when ref_order_category != 0 then 'Ship'
end as service
    ,count(distinct ref_order_id) as total_orders
from dev_vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab raw
where order_status = 400
and date(create_timestamp) = date '2022-12-05'
and is_test = 0
group by 1,2,3
by type