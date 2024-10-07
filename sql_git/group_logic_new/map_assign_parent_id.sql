with group_tab as
(select
        ogm.group_id
        ,ogm.ref_order_id
        ,ogm.ref_order_category
        ,ogm.order_id
        ,ogi.uid as shipper_uid
    from dev_shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_uat ogi
    inner join dev_shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_uat ogm
        on ogi.id = ogm.group_id
    where mapping_status in (11,23,26, 13)
    -- and ogm.group_id = 19300
   
)
,assignment_base as
(select
    case when order_type = 200 then aot.order_id else coalesce(gt.group_id, aot.hold_group_id) end as parent_group_id
    ,aot.*
    ,adot.*
from dev_shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_uat aot
left join dev_shopeefood.foody_partner_db__assign_delivery_order_tab__reg_daily_s0_uat adot
    on aot.id = adot.assignment_id
left join group_tab gt
    on adot.delivery_order_id = gt.order_id and aot.shipper_uid = gt.shipper_uid and aot.status in (3,4,20) -- incharge
-- where order_id = 19296
)
select
    case when gt.order_id is null then 'faled/deny' else 'completed' end as sub_order_status
    ,case
        when status in (3,4) and gt.order_id is not null then 'completed'
        when status in (3,4) and gt.order_id is null then 'incharge then failed/deny'
        when status in (20) then 'incharge and ADMIN_REVERT_INCHARGED'
        when status in (5) then 'reverted incharge'
        when status in (16) then 'SHIPPER_INCHARGED_ERROR'
        when status in (8,9,17,18) then 'ignore'
        when status in (2,14,15) then 'Incharge then deny whole group'
    else 'unknown' end as final_status
    ,ab.delivery_order_id
    ,ab.*
from assignment_base ab
left join group_tab gt
    on ab.parent_group_id = gt.group_id and ab.delivery_order_id = gt.order_id
where parent_group_id = 19236
--- 19221
order by assignment_id
