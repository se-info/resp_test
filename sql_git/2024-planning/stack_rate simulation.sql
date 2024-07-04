with raw as
(select 
        group_code,
        report_date,
        case 
        when driver_policy = 2 and city_name in ('HCM City','Ha Noi City') then 'hub'
        when driver_policy != 2 and city_name in ('HCM City','Ha Noi City') then 'non-hub'
        else 'small_city' end as segment,
        max(rank_order) as total_order_in_group,
        max(re_stack) as system_re,
        sum(single_distance)*1.0000/max(group_distance) as new_re



from group_order_info_raw
where group_category = 0
group by 1,2,3
)
select 
        date_trunc('month',report_date) as month_,
        segment,
        avg(system_re) as avg_re_current,
        avg(new_re) as avg_re_new,
        sum(total_order_in_group)/cast(count(distinct report_date) as double) as ado_group_current,
        sum(case when new_re >= 1 then total_order_in_group else null end)/cast(count(distinct report_date) as double) as ado_group_2024,
        sum(case when new_re >= 1 then total_order_in_group else null end)/cast(count(distinct report_date) as double) as ado_group_2025,
        sum(case when new_re >= 1 then total_order_in_group else null end)/cast(count(distinct report_date) as double) as ado_group_2026,
        sum(case when new_re >= 1 then total_order_in_group else null end)/cast(count(distinct report_date) as double) as ado_group_2027

from raw 
group by 1,2