select 
    ode.grass_date
    ,ode.status
    ,ode.city_id
    ,case 
        when ode.grass_date between date '2023-12-01' and date '2023-12-31' then '1. dec.23'
        when ode.grass_date between date '2024-01-01' and date '2024-01-20' then '2. pre tet_24'
        when ode.grass_date between date '2024-01-21' and date '2024-02-19' then '3. tet_24'
        when ode.grass_date between date '2024-02-20' and date '2024-03-04' then '4. after_tet_24'
        end as period_
    ,count(distinct ode.id) as total_cancel_no_drivers
    ,count(distinct case when aot.ref_order_id is not null then ode.id else null end) as found_drivers
    ,count(distinct case when aot.ref_order_id is not null and aot.status in (3,4) then ode.id else null end) as found_driver_and_incharge
    ,count(distinct case when aot.ref_order_id is not null and aot.status in (2,14,15) then ode.id else null end) as found_driver_and_deny
    ,count(distinct case when aot.ref_order_id is not null and aot.status in (8,9,17,18) then ode.id else null end) as found_driver_and_ignore
    ,count(distinct case when aot.ref_order_id is not null and aot.status not in (3,4,2,14,15,8,9,17,18) then ode.id else null end) as found_driver_with_other_status
    ,count(distinct case when aot.ref_order_id is not null then (order_uid,shipper_uid,order_create_time) else null end) assign_turn
    ,count(distinct case when aot.ref_order_id is not null and sm.shipper_type_id = 12 then (order_uid,shipper_uid,order_create_time) else null end) assign_turn_hub
    ,count(distinct case when aot.ref_order_id is not null and sm.shipper_type_id != 12 then (order_uid,shipper_uid,order_create_time) else null end) assign_turn_non_hub
    ,count(distinct case when aot.ref_order_id is not null and aot.assign_type in (7,8) then (order_uid,shipper_uid,order_create_time) else null end) assign_turn_stack
                                                     
             
from vnfdbi_opsndrivers.shopeefood_vn_bnp_ops_order_detail_tab__vn_daily_s0_live ode
left join vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab aot
    on ode.id = aot.ref_order_id and aot.ref_order_category = 0
left join shopeefood.foody_mart__profile_shipper_master sm
    on aot.shipper_uid = sm.shipper_id and aot.grass_date = try_cast(sm.grass_date as date)
where cancel_reason = 'No driver'
and ode.grass_date between date '2023-12-01' and date '2024-03-04'
                                
                         
group by 1,2,3