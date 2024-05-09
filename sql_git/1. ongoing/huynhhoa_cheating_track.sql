with assign as 
(select 
        sa.ref_order_id,
        sa.order_category,
        sa.driver_id as assigned_shipper_id,
        sa.location as assigned_loc,
        sa.create_time as assigned_time,
        case 
        when sa.status in (3,4) then 'incharged'
        when sa.status in (2,14,15) then 'denied'
        when sa.status in (8,9,17,18) then 'ignored'
        end as assign_status

from driver_ops_order_assign_log_tab sa
where status in (3,4,2,14,15,8,9,17,18) 
)
,order_info as 
(select 
        raw.created_date,
        raw.created_timestamp,
        raw.id as order_id,
        raw.order_code,
        raw.shipper_id as last_shipper_incharged,
        raw.order_status,
        raw.cancel_by,
        raw.cancel_reason,
        0 as order_type,
        raw.sender_name,
        oct.sub_total


from driver_ops_raw_order_tab raw 

left join 
(select id,sub_total/cast(100 as double) as sub_total from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live) oct on oct.id = raw.id

)
select 
        a.*,
        oi.*,
        bt.balance as balance_of_assigned_shipper,
        bt.deposit as deposit_of_assigned_shipper,
        bt2.balance as balance_of_lastest_shipper,
        bt2.deposit as deposit_of_lastest_shipper

from assign a
left join order_info oi on oi.order_id = a.ref_order_id 

left join 
(select user_id,sum(balance)/cast(100 as double) as balance,sum(deposit)/cast(100 as double) as deposit 
from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
where 1 = 1 
group by 1 
) bt on bt.user_id = a.assigned_shipper_id

left join 
(select user_id,sum(balance)/cast(100 as double) as balance,sum(deposit)/cast(100 as double) as deposit 
from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
where 1 = 1 
group by 1 
) bt2 on bt2.user_id = oi.last_shipper_incharged

where 1 = 1 
-- and a.ref_order_id = 771248905
and a.order_category = 0 
and oi.sender_name like '%Bánh Mì Huynh Hoa - Bánh Mì Pate%'
and oi.created_date >= date'2024-04-14'
and oi.order_status = 'Cancelled'



/*
        bt.balance,
        bt.deposit,
left join 
(select user_id,sum(balance)/cast(100 as double) as balance,sum(deposit)/cast(100 as double) as deposit 
from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
where 1 = 1 
group by 1 
) bt on bt.user_id = raw.shipper_id
*/