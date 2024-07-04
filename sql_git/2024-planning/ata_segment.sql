with payment_time as
(
select 
    order_id
    ,min(from_unixtime(create_time-3600)) as received_time --> start payment
from shopeefood.foody_order_db__order_status_log_tab_di
where 1=1
and status = 2 
group by 1
)

,order_level as
(select 
    date_trunc('month',o.report_date) as report_month
    ,o.grass_date
    ,lt_incharge
    ,lt_incharge_to_arrive_at_merchant
    ,lt_pick_to_arrive_at_buyer
    ,lt_arrive_at_merchant_to_pick
    ,lt_arrive_at_buyer_to_del
    ,lt_completion_original/60.0000 lt_completion_original
    ,o.is_asap
    ,o.distance

    ,o.ref_order_id
    ,date_diff('second',from_unixtime(dot.submitted_time-3600), p.received_time)/60.0000 as lt_payment
    ,date_diff('second',p.received_time,from_unixtime(go.confirm_timestamp))/60.0000 as lt_merchant_confirm
    ,case 
        when o.distance < 1 then '1. 0-1km'
        when o.distance < 3 then '2. 1-3km'
        when o.distance < 5 then '3. 3-5km'
        when o.distance < 10 then '4. 5-10km'
        when o.distance >= 10 then '5. ++10km'
        end as distance_range
    ,case when dot.real_drop_time = 0 then null else cast(cast(from_unixtime(dot.real_drop_time - 3600) as timestamp) as timestamp) end as last_delivered_timestamp
    ,case when dot.estimated_drop_time = 0 then null else cast(cast(from_unixtime(dot.estimated_drop_time - 3600) as timestamp) as timestamp) end as estimated_delivered_time
    ,case when dot.real_drop_time != 0  and dot.estimated_drop_time != 0 and dot.estimated_drop_time is not null then date_diff('second',from_unixtime(dot.estimated_drop_time - 3600),from_unixtime(dot.real_drop_time-3600))/60.0000 else null end as ata_eta
    ,case when is_stack_order = 1 or is_group_order = 1 then 1 else 0 end as is_stack_group_order
    -- ,case when date_diff('second',p.received_time,from_unixtime(go.confirm_timestamp))/60.0000
    
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev o
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    on o.ref_order_id = dot.ref_order_id and dot.ref_order_category = 0
left join payment_time p
    on dot.ref_order_id = p.order_id and dot.ref_order_category = 0
left join shopeefood.foody_mart__fact_gross_order_join_detail go
    on o.ref_order_id = go.id and o.source = 'NowFood'
where o.report_date between date_trunc('month',current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
and o.source = 'NowFood'
and o.order_status = 'Delivered'
)
-- select 
--     *
--     ,case 
--         when abs(ata_eta) <= 1 then '1. 1 min'
--         when abs(ata_eta) <= 3 then '2. 3 min'
--         when abs(ata_eta) <= 5 then '3. 5 min'
--         else null end
--         as abs_type
--     ,case 
--         when ata_eta > 0 and ata_eta < 5 then '1. overtime'
--         when ata_eta >=5 then '2. bad case'
--         else null end
--         as late_type
-- from order_level
-- where ref_order_id = 528013043

-- select 
--     dot.real_drop_time
--     ,*
-- from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
-- where dot.ref_order_id = 528013043
,time_performance as
(select 
    report_month
    ,ref_order_id
    ,is_stack_group_order
    ,case 
        when abs(ata_eta) <= 1 then '1. 1 min'
        when abs(ata_eta) <= 3 then '2. 3 min'
        when abs(ata_eta) <= 5 then '3. 5 min'
        else null end
        as abs_type
    ,case 
        when ata_eta > 0 and ata_eta < 5 then '1. overtime'
        when ata_eta >=5 then '2. bad case'
        else null end
        as late_type
    ,sum((case when lt_payment is not null then lt_payment else null end)) / count(distinct (case when lt_payment is not null then ref_order_id else null end)) as payment_time
    ,sum((case when lt_incharge is not null then lt_incharge else null end)) / count(distinct (case when lt_incharge is not null then ref_order_id else null end)) as assignment_time
    ,sum((case when lt_incharge_to_arrive_at_merchant is not null then lt_incharge_to_arrive_at_merchant else null end) ) / count(distinct (case when lt_incharge_to_arrive_at_merchant is not null then ref_order_id else null end)) as driver_to_store
    ,sum((case when lt_arrive_at_merchant_to_pick is not null then lt_arrive_at_merchant_to_pick else null end) ) / count(distinct (case when lt_arrive_at_merchant_to_pick is not null then ref_order_id else null end)) as driver_waiting_time
    
    ,sum((case when lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del else null end)) / count(distinct case when lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then ref_order_id else null end) as driver_to_buyer
    ,sum((case when lt_merchant_confirm is not null then lt_merchant_confirm else null end) ) / count(distinct (case when lt_merchant_confirm is not null then ref_order_id else null end)) as merchant_confirm_time
    ,sum((case when lt_completion_original is not null and is_asap = 1 then lt_completion_original else null end)) / count(distinct (case when lt_completion_original is not null and is_asap = 1 then ref_order_id else null end)) as lt_e2e
    -- ,
    ,sum(case when distance_range = '1. 0-1km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del else null end) 
     / count(distinct case when distance_range = '1. 0-1km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then ref_order_id else null end) driver_to_buyer_1km

    ,sum(case when distance_range = '2. 1-3km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del else null end) 
     / count(distinct case when distance_range = '2. 1-3km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then ref_order_id else null end) driver_to_buyer_3km

    ,sum(case when distance_range = '3. 3-5km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del else null end) 
     / count(distinct case when distance_range = '3. 3-5km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then ref_order_id else null end) driver_to_buyer_5km

    ,sum(case when distance_range = '4. 5-10km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del else null end) 
     / count(distinct case when distance_range = '4. 5-10km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then ref_order_id else null end) driver_to_buyer_10km
    
    ,sum(case when distance_range = '5. ++10km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del else null end) 
     / count(distinct case when distance_range = '5. ++10km'  and lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then ref_order_id else null end) driver_to_buyer_over10km

    ,sum(ata_eta) ata_eta
     

        -- ,case 
        -- when o.distance < 1 then '1. 0-1km'
        -- when o.distance < 3 then '2. 1-3km'
        -- when o.distance < 5 then '3. 3-5km'
        -- when o.distance < 10 then '4. 5-10km'
        -- when o.distance >= 10 then '5. ++10km'
        -- end as distance_range
from order_level 
where is_asap = 1
group by 1,2,3,4,5

)
-- select 
--     *
-- from time_performance
,time_performance_layer2 as 
(
    select 
        report_month
        ,avg(payment_time) as avg_payment_time
        ,avg(assignment_time) as avg_assignment_time
        ,avg(driver_to_store) as avg_driver_to_store
        ,avg(driver_waiting_time) as avg_driver_waiting_time
        ,avg(driver_to_buyer) as avg_driver_to_buyer
        ,avg(merchant_confirm_time) as avg_merchant_confirm_time
        ,avg(lt_e2e) as avg_lt_e2e
        
        ,avg(case when is_stack_group_order = 0 then payment_time else null end) as avg_payment_time_single
        ,avg(case when is_stack_group_order = 0 then assignment_time else null end) as avg_assignment_time_single
        ,avg(case when is_stack_group_order = 0 then driver_to_store else null end) as avg_driver_to_store_single
        ,avg(case when is_stack_group_order = 0 then driver_waiting_time else null end) as avg_driver_waiting_time_single
        ,avg(case when is_stack_group_order = 0 then driver_to_buyer else null end) as avg_driver_to_buyer_single
        ,avg(case when is_stack_group_order = 0 then merchant_confirm_time else null end) as avg_merchant_confirm_time_single
        ,avg(case when is_stack_group_order = 0 then lt_e2e else null end) as avg_lt_e2e_single

        ,avg(case when is_stack_group_order = 1 then payment_time else null end) as avg_payment_time_stack
        ,avg(case when is_stack_group_order = 1 then assignment_time else null end) as avg_assignment_time_stack
        ,avg(case when is_stack_group_order = 1 then driver_to_store else null end) as avg_driver_to_store_stack
        ,avg(case when is_stack_group_order = 1 then driver_waiting_time else null end) as avg_driver_waiting_time_stack
        ,avg(case when is_stack_group_order = 1 then driver_to_buyer else null end) as avg_driver_to_buyer_stack
        ,avg(case when is_stack_group_order = 1 then merchant_confirm_time else null end) as avg_merchant_confirm_time_stack
        ,avg(case when is_stack_group_order = 1 then lt_e2e else null end) as avg_lt_e2e_stack

        ,1.00000* count(distinct case when is_stack_group_order = 1 then ref_order_id else null end) / count(distinct ref_order_id) as pct_stack_group
        -- ,count(distinct case when is_stack_group_order = 1 then ref_order_id else null end) as stacked_orders
        -- ,count(distinct ref_order_id) as net_orders

    from time_performance
    group by 1
)

-- driver rating
,driver_rating as

(select date_trunc('month',date(from_unixtime(oct.submit_time -3600))) report_month 
    ,sum(case when r.rating_star is not null then r.rating_star else null end)*1.0000 / count(distinct r.order_id) as avg_rating_star
    , cast(count(distinct case when rating_star < 5 then r.order_id else null end) as double) / count(distinct r.order_id) as rating_below_five 
    , count(distinct r.order_id)*1.0000 / count(distinct oct.id ) as rating_pct
from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
left join
(
select order_id , is_five_star_flag , rating_star
from shopeefood.shopeefood_mart_cdm_dwd_vn_rating_rating_driver_da 
where dt = current_date - interval '1' day
)r on r.order_id = oct.id 

where 1=1 
and oct.status = 7
-- and date(from_unixtime(oct.submit_time -3600)) between date '2022-12-01' and date '2023-03-31'
and date(from_unixtime(oct.submit_time -3600)) between date_trunc('month',current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
group by 1
)
select 
    dr.report_month
    ,dr.rating_below_five
    ,dr.rating_pct
    ,dr.avg_rating_star
    ,tp.avg_payment_time
    ,tp.avg_assignment_time
    ,tp.avg_driver_to_store
    ,tp.avg_driver_waiting_time
    ,tp.avg_driver_to_buyer
    ,tp.avg_merchant_confirm_time
    ,tp.avg_lt_e2e

    ,tp.avg_payment_time_single
    ,tp.avg_assignment_time_single
    ,tp.avg_driver_to_store_single
    ,tp.avg_driver_waiting_time_single
    ,tp.avg_driver_to_buyer_single
    ,tp.avg_merchant_confirm_time_single
    ,tp.avg_lt_e2e_single

    ,tp.avg_payment_time_stack
    ,tp.avg_assignment_time_stack
    ,tp.avg_driver_to_store_stack
    ,tp.avg_driver_waiting_time_stack
    ,tp.avg_driver_to_buyer_stack
    ,tp.avg_merchant_confirm_time_stack
    ,tp.avg_lt_e2e_stack

    ,tp.pct_stack_group
    -- ,tp.stacked_orders
    -- ,tp.net_orders

from driver_rating dr
left join time_performance_layer2 tp
    on dr.report_month = tp.report_month

-- select 
--     date_trunc('month',date_) as report_month
--     ,count(distinct case when is_stack_group_order != 0 then order_id else null end) as stacked_orders
--     ,count(distinct order_id) as net_orders
-- from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level
-- where date_ between date '2023-06-01' and date '2023-08-13'
-- and source in ('Food','Market')
-- group by 1

