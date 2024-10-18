with payment_time as
(
select 
    order_id
    ,min(from_unixtime(create_time-3600)) as received_time                  
from shopeefood.foody_order_db__order_status_log_tab_di
where 1=1
and status = 2 
group by 1
)

,order_level as
(select 
    date_trunc('month',o.report_date) as report_month
    ,o.report_date
    ,lt_incharge
    ,lt_incharge_to_arrive_at_merchant
    ,lt_pick_to_arrive_at_buyer
    ,lt_arrive_at_merchant_to_pick
    ,lt_arrive_at_buyer_to_del
    ,lt_completion_original/60.0000 lt_completion_original
    ,o.is_asap
    ,o.distance
    ,o.city_group
    ,o.ref_order_id
    ,o.group_id
    ,date_diff('second',from_unixtime(dot.submitted_time-3600), p.received_time)/60.0000 as lt_payment
    ,date_diff('second',p.received_time,from_unixtime(go.confirm_timestamp))/60.0000 as lt_merchant_confirm
    ,case 
        when o.distance <= 3.6 then '1. 0 - 3.6km'
        when o.distance <= 5 then '2. 3.6 - 5km'
        when o.distance > 5 then '3. ++5km'
        
        end as distance_range
    ,o.distance
    ,case when dot.real_drop_time = 0 then null else cast(cast(from_unixtime(dot.real_drop_time - 3600) as timestamp) as timestamp) end as last_delivered_timestamp
    ,case when dot.estimated_drop_time = 0 then null else cast(cast(from_unixtime(dot.estimated_drop_time - 3600) as timestamp) as timestamp) end as estimated_delivered_time
    ,case when dot.real_drop_time != 0  and dot.estimated_drop_time != 0 and dot.estimated_drop_time is not null then date_diff('second',from_unixtime(dot.estimated_drop_time - 3600),from_unixtime(dot.real_drop_time-3600))/60.0000 else null end as ata_eta
                                                                                                    
    ,case 
    when o .report_date <= date '2024-07-31' 
    then (case when o.is_stack_order = 1 or is_group_order = 1 then 1 else 0 end) 
    when o.group_id > 0 then if(god.is_actual_stack_group_order > 0,1,0)
    else 0 end as is_stack_group_order
    ,case 
    when o.group_id > 0 and god.is_actual_stack_group_order = 2 and god.cnt_order_in_group > 2 then 1
    else 0 end as is_stack_group_multi
                                                                                                 

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev o

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    on o.ref_order_id = dot.ref_order_id and dot.ref_order_category = 0


left join dev_vnfdbi_opsndrivers.shopeefood_bi_group_order_detail_tab god
    on o.group_id = god.group_id and o.ref_order_id = god.ref_order_id

left join payment_time p
    on dot.ref_order_id = p.order_id and dot.ref_order_category = 0

left join shopeefood.foody_mart__fact_gross_order_join_detail go
    on o.ref_order_id = go.id and o.source = 'NowFood'

                                                                    
where o.report_date between date_trunc('month', current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
and o.source = 'NowFood'
and o.order_status = 'Delivered'
)
,time_performance as
(select 
    report_month
    ,report_date
    ,ref_order_id
    ,is_stack_group_order
    ,is_stack_group_multi
    ,case 
    when c.bad_weather_fee > 0 then 1 else 0 end as is_bw
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
    ,city_group
    ,distance_range
    ,sum((case when lt_payment is not null then lt_payment else null end)) / count(distinct (case when lt_payment is not null then ref_order_id else null end)) as payment_time
    ,sum((case when lt_incharge is not null then lt_incharge else null end)) / count(distinct (case when lt_incharge is not null then ref_order_id else null end)) as assignment_time
    ,sum((case when lt_incharge_to_arrive_at_merchant is not null then lt_incharge_to_arrive_at_merchant else null end) ) / count(distinct (case when lt_incharge_to_arrive_at_merchant is not null then ref_order_id else null end)) as driver_to_store
    ,sum((case when lt_arrive_at_merchant_to_pick is not null then lt_arrive_at_merchant_to_pick else null end) ) / count(distinct (case when lt_arrive_at_merchant_to_pick is not null then ref_order_id else null end)) as driver_waiting_time_at_mex
    ,sum((case when lt_arrive_at_buyer_to_del is not null then lt_arrive_at_buyer_to_del else null end) ) / count(distinct (case when lt_arrive_at_buyer_to_del is not null then ref_order_id else null end)) as driver_waiting_time_at_user

    ,sum((case when lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del else null end)) / count(distinct case when lt_pick_to_arrive_at_buyer + lt_arrive_at_buyer_to_del is not null then ref_order_id else null end) as driver_to_buyer
    ,sum((case when lt_merchant_confirm is not null then lt_merchant_confirm else null end) ) / count(distinct (case when lt_merchant_confirm is not null then ref_order_id else null end)) as merchant_confirm_time
    ,sum((case when lt_completion_original is not null and is_asap = 1 then lt_completion_original else null end)) / count(distinct (case when lt_completion_original is not null and is_asap = 1 then ref_order_id else null end)) as lt_e2e
        
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
     
from order_level 

left join (select id,bad_weather_fee from driver_ops_raw_order_tab where order_type = 0) c 
    on c.id = order_level.ref_order_id

where is_asap = 1
group by 1,2,3,4,5,6,7,8,9,10

)
,time_performance_layer2 as 
(
    select 
        report_month
        ,case when city_group in('HCM','HN') then city_group else 'OTH' end as city_group
                          
                 
        ,count(distinct ref_order_id)/cast(count(distinct report_date) as double) as asap_order
        ,avg(payment_time) as avg_payment_time
        ,avg(assignment_time) as avg_assignment_time
        ,avg(driver_to_store) as avg_driver_to_store
        ,avg(driver_waiting_time_at_mex) as avg_driver_waiting_time_mex
        ,avg(driver_waiting_time_at_user) as avg_driver_waiting_time_user
        ,avg(driver_to_buyer) as avg_driver_to_buyer
        ,avg(merchant_confirm_time) as avg_merchant_confirm_time
        ,avg(lt_e2e) as avg_lt_e2e
        
        ,avg(case when is_stack_group_order = 0 then payment_time else null end) as avg_payment_time_single
        ,avg(case when is_stack_group_order = 0 then assignment_time else null end) as avg_assignment_time_single
        ,avg(case when is_stack_group_order = 0 then driver_to_store else null end) as avg_driver_to_store_single
        ,avg(case when is_stack_group_order = 0 then driver_waiting_time_at_mex else null end) as avg_driver_waiting_time_mex_single
        ,avg(case when is_stack_group_order = 0 then driver_waiting_time_at_user else null end) as avg_driver_waiting_time_user_single
        ,avg(case when is_stack_group_order = 0 then driver_to_buyer else null end) as avg_driver_to_buyer_single
        ,avg(case when is_stack_group_order = 0 then merchant_confirm_time else null end) as avg_merchant_confirm_time_single
        ,avg(case when is_stack_group_order = 0 then lt_e2e else null end) as avg_lt_e2e_single

        ,avg(case when is_stack_group_order = 1 then payment_time else null end) as avg_payment_time_stack_overall
        ,avg(case when is_stack_group_order = 1 then assignment_time else null end) as avg_assignment_time_stack_overall
        ,avg(case when is_stack_group_order = 1 then driver_to_store else null end) as avg_driver_to_store_stack_overall
        ,avg(case when is_stack_group_order = 1 then driver_waiting_time_at_mex else null end) as avg_driver_waiting_time_mex_stack_overall
        ,avg(case when is_stack_group_order = 1 then driver_waiting_time_at_user else null end) as avg_driver_waiting_time_user_stack_overall
        ,avg(case when is_stack_group_order = 1 then driver_to_buyer else null end) as avg_driver_to_buyer_stack_overall
        ,avg(case when is_stack_group_order = 1 then merchant_confirm_time else null end) as avg_merchant_confirm_time_stack_overall
        ,avg(case when is_stack_group_order = 1 then lt_e2e else null end) as avg_lt_e2e_stack_overall

        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 1 then payment_time else null end) as avg_payment_time_stack_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 1 then assignment_time else null end) as avg_assignment_time_stack_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 1 then driver_to_store else null end) as avg_driver_to_store_stack_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 1 then driver_waiting_time_at_mex else null end) as avg_driver_waiting_time_mex_stack_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 1 then driver_waiting_time_at_user else null end) as avg_driver_waiting_time_user_stack_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 1 then driver_to_buyer else null end) as avg_driver_to_buyer_stack_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 1 then merchant_confirm_time else null end) as avg_merchant_confirm_time_stack_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 1 then lt_e2e else null end) as avg_lt_e2e_stack_multi

        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 0 then payment_time else null end) as avg_payment_time_stack_non_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 0 then assignment_time else null end) as avg_assignment_time_stack_non_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 0 then driver_to_store else null end) as avg_driver_to_store_stack_non_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 0 then driver_waiting_time_at_mex else null end) as avg_driver_waiting_time_mex_stack_non_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 0 then driver_waiting_time_at_user else null end) as avg_driver_waiting_time_user_stack_non_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 0 then driver_to_buyer else null end) as avg_driver_to_buyer_stack_non_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 0 then merchant_confirm_time else null end) as avg_merchant_confirm_time_stack_non_multi
        ,avg(case when is_stack_group_order = 1 and is_stack_group_multi = 0 then lt_e2e else null end) as avg_lt_e2e_stack_non_multi


        ,1.00000* count(distinct case when is_stack_group_order = 1 then ref_order_id else null end) / count(distinct ref_order_id) as pct_stack_group
        ,1.00000* count(distinct case when is_stack_group_order = 1 and is_stack_group_multi = 1 then ref_order_id else null end) / count(distinct ref_order_id) as pct_stack_multi
        ,1.00000* count(distinct case when is_stack_group_order = 1 and is_stack_group_multi = 0 then ref_order_id else null end) / count(distinct ref_order_id) as pct_stack_group_normal
        ,1.00000* count(distinct ref_order_id)/count(distinct report_date) as net_order
        ,count(distinct report_date) as days

    from time_performance
    group by 1,2
                 
)

select 
     tp.report_month
                
    ,tp.city_group 
                  
    ,tp.asap_order  
    ,tp.avg_payment_time
    ,tp.avg_assignment_time
    ,tp.avg_driver_to_store
    ,tp.avg_driver_waiting_time_mex
    ,tp.avg_driver_waiting_time_user
    ,tp.avg_driver_to_buyer
    ,tp.avg_merchant_confirm_time
    ,tp.avg_lt_e2e

    ,tp.avg_payment_time_single
    ,tp.avg_assignment_time_single
    ,tp.avg_driver_to_store_single
    ,tp.avg_driver_waiting_time_mex_single
    ,tp.avg_driver_waiting_time_user_single
    ,tp.avg_driver_to_buyer_single
    ,tp.avg_merchant_confirm_time_single
    ,tp.avg_lt_e2e_single

    ,tp.avg_payment_time_stack_overall
    ,tp.avg_assignment_time_stack_overall
    ,tp.avg_driver_to_store_stack_overall
    ,tp.avg_driver_waiting_time_mex_stack_overall
    ,tp.avg_driver_waiting_time_user_stack_overall
    ,tp.avg_driver_to_buyer_stack_overall
    ,tp.avg_merchant_confirm_time_stack_overall
    ,tp.avg_lt_e2e_stack_overall

    ,tp.avg_payment_time_stack_multi
    ,tp.avg_assignment_time_stack_multi
    ,tp.avg_driver_to_store_stack_multi
    ,tp.avg_driver_waiting_time_mex_stack_multi
    ,tp.avg_driver_waiting_time_user_stack_multi
    ,tp.avg_driver_to_buyer_stack_multi
    ,tp.avg_merchant_confirm_time_stack_multi
    ,tp.avg_lt_e2e_stack_multi

    ,tp.avg_payment_time_stack_non_multi
    ,tp.avg_assignment_time_stack_non_multi
    ,tp.avg_driver_to_store_stack_non_multi
    ,tp.avg_driver_waiting_time_mex_stack_non_multi
    ,tp.avg_driver_waiting_time_user_stack_non_multi
    ,tp.avg_driver_to_buyer_stack_non_multi
    ,tp.avg_merchant_confirm_time_stack_non_multi
    ,tp.avg_lt_e2e_stack_non_multi


    ,tp.pct_stack_group
    ,tp.pct_stack_multi
    ,tp.pct_stack_group_normal

    ,tp.net_order

from time_performance_layer2 tp;
