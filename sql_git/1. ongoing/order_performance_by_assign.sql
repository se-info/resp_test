with raw as 
(select 
        date_trunc('month',created_date) as month_,
        created_date,
        group_id,
        if(city_name in ('HCM City','Ha Noi City'),city_name,'Others') as cities,
        if(order_type =0,'1. delivery','2. spxi') as source,
        case 
        when group_id > 0 and max_by(order_assign_type,created_timestamp) != 'Group' then 'stack'
        when group_id > 0 and max_by(order_assign_type,created_timestamp) = 'Group' then 'group'
        else 'single' end as is_stack_group,
        cardinality(array_agg(distinct order_code)) as cnt_order_in_group
        


from driver_ops_raw_order_tab
where 1 = 1 
and order_status = 'Delivered'
and created_date >= date'2023-10-01'
group by 1,2,3,4,5,group_id
)
select 
        month_,
        source,
        cities,
        case when is_stack_group = 'single' then 1
        else cnt_order_in_group end as total_order_in_group,

        sum(case when is_stack_group = 'single' then cnt_order_in_group else null end)*1.0000/count(distinct created_date) as single,
        sum(case when is_stack_group = 'stack' then cnt_order_in_group else null end)*1.0000/count(distinct created_date) as stack,
        sum(case when is_stack_group = 'group' then cnt_order_in_group else null end)*1.0000/count(distinct created_date) as group_

from raw 
group by 1,2,3,4;