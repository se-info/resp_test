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
    ,o.city_name
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

                                                                    
where o.report_date between date_trunc('month', current_date - interval '1' day) - interval '1' month and current_date - interval '1' day
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
    ,CASE 
        WHEN city_name IN 
        ('HCM City',
        'Ha Noi City',
        'Da Nang City') then city_name
        WHEN city_name IN 
        ('Dong Nai',
        'Can Tho City',
        'Binh Duong',
        'Hai Phong City',
        'Hue City',
        'Vung Tau') THEN 'T2'
        WHEN city_name IN 
        ('Bac Ninh',
        'Khanh Hoa',
        'Nghe An',
        'Thai Nguyen',
        'Quang Ninh',
        'Lam Dong',
        'Quang Nam') THEN 'T3'  
        ELSE 'new_cities' END AS cities
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
select 
        report_month,
        case when is_stack_group_order = 0 then 'single' when is_stack_group_order = 1 then 'stack' end as assign_type,
        coalesce(cities,'VN') as cities,
        count(distinct ref_order_id)/cast(count(distinct report_date) as double) as net_order,
        avg(assignment_time) as avg_assignment_time,
        approx_percentile(assignment_time,0.05) as "pct5%",
        approx_percentile(assignment_time,0.1) as "pct10%",
        approx_percentile(assignment_time,0.15) as "pct15%",
        approx_percentile(assignment_time,0.2) as "pct20%",
        approx_percentile(assignment_time,0.25) as "pct25%",
        approx_percentile(assignment_time,0.3) as "pct30%",
        approx_percentile(assignment_time,0.35) as "pct35%",
        approx_percentile(assignment_time,0.4) as "pct40%",
        approx_percentile(assignment_time,0.45) as "pct45%",
        approx_percentile(assignment_time,0.5) as "pct50%",
        approx_percentile(assignment_time,0.55) as "pct55%",
        approx_percentile(assignment_time,0.6) as "pct60%",
        approx_percentile(assignment_time,0.65) as "pct65%",
        approx_percentile(assignment_time,0.7) as "pct70%",
        approx_percentile(assignment_time,0.75) as "pct75%",
        approx_percentile(assignment_time,0.8) as "pct80%",
        approx_percentile(assignment_time,0.85) as "pct85%",
        approx_percentile(assignment_time,0.9) as "pct90%",
        approx_percentile(assignment_time,0.95) as "pct95%",
        approx_percentile(assignment_time,0.99) as "pct99%",
        count(distinct report_date) as days

from time_performance
group by 1,2,grouping sets(cities,())

UNION ALL 
select 
        report_month,
        'overall' assign_type,
        coalesce(cities,'VN') as cities,
        count(distinct ref_order_id)/cast(count(distinct report_date) as double) as net_order,
        avg(assignment_time) as avg_assignment_time,
        approx_percentile(assignment_time,0.05) as "pct5%",
        approx_percentile(assignment_time,0.1) as "pct10%",
        approx_percentile(assignment_time,0.15) as "pct15%",
        approx_percentile(assignment_time,0.2) as "pct20%",
        approx_percentile(assignment_time,0.25) as "pct25%",
        approx_percentile(assignment_time,0.3) as "pct30%",
        approx_percentile(assignment_time,0.35) as "pct35%",
        approx_percentile(assignment_time,0.4) as "pct40%",
        approx_percentile(assignment_time,0.45) as "pct45%",
        approx_percentile(assignment_time,0.5) as "pct50%",
        approx_percentile(assignment_time,0.55) as "pct55%",
        approx_percentile(assignment_time,0.6) as "pct60%",
        approx_percentile(assignment_time,0.65) as "pct65%",
        approx_percentile(assignment_time,0.7) as "pct70%",
        approx_percentile(assignment_time,0.75) as "pct75%",
        approx_percentile(assignment_time,0.8) as "pct80%",
        approx_percentile(assignment_time,0.85) as "pct85%",
        approx_percentile(assignment_time,0.9) as "pct90%",
        approx_percentile(assignment_time,0.95) as "pct95%",
        approx_percentile(assignment_time,0.99) as "pct99%",
        count(distinct report_date) as days

from time_performance
group by 1,2,grouping sets(cities,())
                 


