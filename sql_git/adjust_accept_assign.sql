with raw as 
(select 
        order_id,
        order_code, 
        date_diff('second',create_time,update_time) as gap_ignored,
        status,
        case when experiment_group in (3,4) then 1 ELSE 0 end as is_auto_accepted,
        case when experiment_group in (7,8) then 1 ELSE 0 end as is_auto_accepted_continuous_assign,
        driver_id,
        create_time,
        order_category

from driver_ops_order_assign_log_tab
where status in (18,8)
)
,f as 
(select
        order_code,
        order_category,
        count(distinct (order_id,driver_id,create_time)) as total_ignored_by_timeout,
        count(distinct (order_id,driver_id,create_time)) * 5 as add_5_second,
        count(distinct (order_id,driver_id,create_time)) * 10 as add_10_second,
        count(distinct (order_id,driver_id,create_time)) * 15 as add_15_second,
        count(distinct (order_id,driver_id,create_time)) * 20 as add_20_second

from raw 
group by 1,2 
)
,m as 
(select 
        raw.id as ref_id,
        raw.order_code,
        raw.source as sub_source,
        case 
        when raw.order_type = 0 then 'food'
        else 'spxi' end as source,
        raw.is_asap,
        raw.created_date,
        raw.created_timestamp,
        raw.delivered_timestamp,
        coalesce(f.add_5_second,0)*1.00/60 as add_5_second, 
        coalesce(f.add_10_second,0)*1.00/60 as add_10_second,
        coalesce(f.add_15_second,0)*1.00/60 as add_15_second,
        coalesce(f.add_20_second,0)*1.00/60 as add_20_second,
        coalesce(f.total_ignored_by_timeout,0) as total_ignored_by_timeout,
        date_diff('second',created_timestamp,delivered_timestamp)*1.00/60 as original_ata,
        date_diff('second',first_auto_assign_timestamp,last_incharge_timestamp)*1.00/60 as original_incharged_time

from driver_ops_raw_order_tab raw 

left join f 
        on f.order_code = raw.order_code and f.order_category = raw.order_type

where 1 = 1 
and order_status = 'Delivered'
and is_asap = 1
)
select  
        created_date,
        source,
        count(distinct ref_id) as total_order,
        sum(original_ata)/cast(count(distinct ref_id) as double) as avg_ata,
        (sum(original_ata) + sum(add_5_second))/cast(count(distinct ref_id) as double) as add_5_second,
        (sum(original_ata) + sum(add_10_second))/cast(count(distinct ref_id) as double) as add_10_second,
        (sum(original_ata) + sum(add_15_second))/cast(count(distinct ref_id) as double) as add_15_second,
        (sum(original_ata) + sum(add_20_second))/cast(count(distinct ref_id) as double) as add_20_second,

        sum(original_incharged_time)/cast(count(distinct ref_id) as double) as avg_incharged_time,
        (sum(original_incharged_time) + sum(add_5_second))/cast(count(distinct ref_id) as double) as add_5_second_incharged,
        (sum(original_incharged_time) + sum(add_10_second))/cast(count(distinct ref_id) as double) as add_10_second_incharged,
        (sum(original_incharged_time) + sum(add_15_second))/cast(count(distinct ref_id) as double) as add_15_second_incharged,
        (sum(original_incharged_time) + sum(add_20_second))/cast(count(distinct ref_id) as double) as add_20_second_incharged


from m

where created_date between date'2024-01-08' and date'2024-01-14'
group by 1,2

