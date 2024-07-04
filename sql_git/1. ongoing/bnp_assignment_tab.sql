with base_assignment as
(select
date(create_timestamp) created_date
        ,ref_order_category
        ,order_uid
        ,shipper_uid
        ,create_timestamp
        ,status
from vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab raw
where status in (3,4,2,14,15,8,9,17,18) 
and date(create_timestamp) between date'2024-04-01' and date'2024-05-31'
)
select 
    date_trunc('month',created_date)
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