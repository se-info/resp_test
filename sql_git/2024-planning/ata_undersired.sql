-- select 
--     *
-- from shopeefood.foody_user_activity_db__customer_feedback_order_tab__reg_daily_s0_live cfo
-- -- where date(from_unixtime(create_time-3600)) = date '2023-08-09'
-- where order_id = 578049162
-- limit 10
with order_base as
(select 
    report_date
    ,date_trunc('month',report_date) as report_month
    ,ref_order_id
    ,lt_completion_original/60.0000 lt_completion_original
    ,case 
        when o.distance <= 1 then '1. 0-1km'
        when o.distance <= 2 then '2. 1-2km'
        when o.distance <= 3 then '3. 2-3km'
        when o.distance <= 4 then '4. 3-4km'
        when o.distance <= 5 then '5. 4-5km'
        when o.distance <= 10 then '6. 5-10km'
        when o.distance >10 then '7. ++10km'
        end as distance_range
    ,case 
        when lt_completion_original/60 <= 15 then '01. 0-15 mins'
        when lt_completion_original/60 <= 20 then '02. 15-20 mins'
        when lt_completion_original/60 <= 25 then '03. 20-25 mins'
        when lt_completion_original/60 <= 30 then '04. 25-30 mins'
        when lt_completion_original/60 <= 35 then '05. 30-35 mins'
        when lt_completion_original/60 <= 40 then '06. 35-40 mins'
        when lt_completion_original/60 <= 45 then '07. 40-45 mins'
        when lt_completion_original/60 <= 50 then '08. 45-50 mins'
        when lt_completion_original/60 <= 55 then '09. 50-55 mins'
        when lt_completion_original/60 <= 60 then '10. 55-60 mins'
        when lt_completion_original/60 > 60 then '11. ++60 mins'
        end as ata_range
    ,is_stack_order
    ,is_group_order
    ,case 
        when o.distance <= 2 and lt_completion_original/60 > 35 then 1
        when o.distance <= 4 and lt_completion_original/60 > 40 then 1
        when o.distance <= 5 and lt_completion_original/60 > 45 then 1
        when o.distance <= 10 and lt_completion_original/60 > 50 then 1
        when o.distance > 10 and lt_completion_original/60 > 60 then 1
        else 0
        end as is_undesired_ata_orders
    ,case when is_stack_order = 0 and is_group_order = 0 then 1 else 0 end as is_single

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev o
where o.report_date between date_trunc('month', current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
and o.source = 'NowFood'
and o.order_status = 'Delivered'
and o.is_asap = 1

)
,raw2 as
(select 
    o.*
    ,cfo.order_id
    ,cfo.shipper_uid as shipper_id
    ,CASE WHEN cfo.shipper_rate = 0 then null
    when cfo.shipper_rate = 1 or cfo.shipper_rate = 101 then 1
    when cfo.shipper_rate = 2 or cfo.shipper_rate = 102 then 2
    when cfo.shipper_rate = 3 or cfo.shipper_rate = 103 then 3
    when cfo.shipper_rate = 104 then 4
    when cfo.shipper_rate = 105 then 5
    else null end as shipper_rate
    ,from_unixtime(cfo.create_time - 60*60) as create_ts
from order_base o
left join shopeefood.foody_user_activity_db__customer_feedback_order_tab__reg_daily_s0_live cfo 
    on o.ref_order_id = cfo.order_id
)
select 
    report_month
    -- ,distance_range
    -- ,ata_range
    -- ,case when is_stack_order = 0 and is_group_order = 0 then 1 else 0 end as is_single
    ,count(distinct case when shipper_rate is not null then ref_order_id else null end) as total_rated_orders
    ,count(distinct case when shipper_rate >= 5 then ref_order_id else null end) as five_star_orders
    ,count(distinct ref_order_id) as total_orders
    ,1.0000*count(distinct case when distance_range = '1. 0-1km' and lt_completion_original > 35 then ref_order_id else null end) / count(distinct case when distance_range = '1. 0-1km' and ata_range is not null then ref_order_id else null end) as order_dist_1km
    ,1.0000*count(distinct case when distance_range = '2. 1-2km' and lt_completion_original > 35 then ref_order_id else null end) / count(distinct case when distance_range = '2. 1-2km' and ata_range is not null then ref_order_id else null end) as order_dist_2km
    ,1.0000*count(distinct case when distance_range = '3. 2-3km' and lt_completion_original > 40 then ref_order_id else null end) / count(distinct case when distance_range = '3. 2-3km' and ata_range is not null then ref_order_id else null end) as order_dist_3km
    ,1.0000*count(distinct case when distance_range = '4. 3-4km' and lt_completion_original > 40 then ref_order_id else null end) / count(distinct case when distance_range = '4. 3-4km' and ata_range is not null then ref_order_id else null end) as order_dist_4km
    ,1.0000*count(distinct case when distance_range = '5. 4-5km' and lt_completion_original > 45 then ref_order_id else null end) / count(distinct case when distance_range = '5. 4-5km' and ata_range is not null then ref_order_id else null end) as order_dist_5km
    ,1.0000*count(distinct case when distance_range = '6. 5-10km' and lt_completion_original > 50 then ref_order_id else null end) / count(distinct case when distance_range = '6. 5-10km' and ata_range is not null then ref_order_id else null end) as order_dist_10km
    ,1.0000*count(distinct case when distance_range = '7. ++10km' and lt_completion_original > 60 then ref_order_id else null end) / count(distinct case when distance_range = '7. ++10km' and ata_range is not null then ref_order_id else null end) as order_dist_over10km
    ,1.0000*count(distinct case when lt_completion_original > 40 then ref_order_id else null end) / count(distinct case when ata_range is not null then ref_order_id else null end) as order_dist_overall_old
    -- ,1.0000*count(distinct case when lt_completion_original > 55 then ref_order_id else null end) as is_undesired_ata_orders_overall
    -- ,1.0000*count(distinct case when lt_completion_original > 40 then ref_order_id else null end)

    -- ,1.0000*count(distinct case when distance_range = '1. 0-1km' and lt_completion_original > 35 then ref_order_id else null end)
    -- +1.0000*count(distinct case when distance_range = '2. 1-2km' and lt_completion_original > 35 then ref_order_id else null end)
    -- +1.0000*count(distinct case when distance_range = '3. 2-3km' and lt_completion_original > 40 then ref_order_id else null end)
    -- +1.0000*count(distinct case when distance_range = '4. 3-4km' and lt_completion_original > 40 then ref_order_id else null end)
    -- +1.0000*count(distinct case when distance_range = '5. 4-5km' and lt_completion_original > 45 then ref_order_id else null end)
    -- +1.0000*count(distinct case when distance_range = '6. 5-10km' and lt_completion_original > 50 then ref_order_id else null end)
    -- +1.0000*count(distinct case when distance_range = '7. ++10km' and lt_completion_original > 60 then ref_order_id else null end)
    -- as undesired_ata_orders_distance_group
    -- ,count(distinct case when ata_range is not null then ref_order_id else null end) as nationwide_net_ado
    ,(1.0000*count(distinct case when distance_range = '1. 0-1km' and lt_completion_original > 35 then ref_order_id else null end)
    +1.0000*count(distinct case when distance_range = '2. 1-2km' and lt_completion_original > 35 then ref_order_id else null end)
    +1.0000*count(distinct case when distance_range = '3. 2-3km' and lt_completion_original > 40 then ref_order_id else null end)
    +1.0000*count(distinct case when distance_range = '4. 3-4km' and lt_completion_original > 40 then ref_order_id else null end)
    +1.0000*count(distinct case when distance_range = '5. 4-5km' and lt_completion_original > 45 then ref_order_id else null end)
    +1.0000*count(distinct case when distance_range = '6. 5-10km' and lt_completion_original > 50 then ref_order_id else null end)
    +1.0000*count(distinct case when distance_range = '7. ++10km' and lt_completion_original > 60 then ref_order_id else null end)
    )
    / count(distinct case when ata_range is not null then ref_order_id else null end) as order_dist_overall_new
    -- ,1.0000*count(distinct case when lt_completion_original > 40 then ref_order_id else null end)
from raw2
-- where is_single = 1
group by 1

-- where ref_order_id = 589682089
-- limit 10
