select 
        report_date,
        year(report_date)*100 + week(report_date) as created_year_week,
        city_name,
        shipper_tier,
        case 
        when total_order < 10 or total_order is null then 'a. G: <10'
        when total_order < 15 then 'b. G: 10-15'
        when total_order < 25 then 'c. G: 15-25'
        when total_order >= 25 then 'd. G: 25+'
        else null end daily_order_range,
        count(distinct (shipper_id,report_date)) as total_active,
        sum(total_order) as total_order

from driver_ops_driver_performance_tab

where total_order > 0 
and report_date between current_date - interval '30' day and current_date - interval '1' day
group by 1,2,3,4,5