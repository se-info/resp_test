with raw_check as 
(select 
        shipper_id,
        count(distinct report_date) as working_days,
        sum(total_order) as total_order

from driver_ops_driver_performance_tab

where report_date between date'2024-08-27' and date'2024-08-30'
and city_name in ('Ha Noi City','Quang Ninh','Hai Duong','Thai Nguyen','Bac Ninh','Hai Phong City')
and total_order > 0 
group by 1 
)
,agg as 
(select 
        shipper_id,
        array_agg(distinct report_date) as delivered_date_agg


from driver_ops_driver_performance_tab

where total_order > 0 
group by 1 )
select 
        *,
        case 
        when city_name not in('Ha Noi City') then 1
        when city_name in ('Ha Noi City') and shipper_tier != 'Hub' then 1 
        when city_name in ('Ha Noi City') and shipper_tier = 'Hub' and is_a60 = 1 and l3d = 0 then 1 
        else 0 end as is_qualified_join_bonus

from
(select 
        r.report_date,
        r.shipper_id,
        r.city_name,
        r.shipper_type,
        r.total_order,
        r.online_hour,
        r.sla_rate,
        rc.working_days,
        rc.total_order as total_3d_order,
        r.shipper_tier,
        if(cardinality(filter(agg.delivered_date_agg,x -> x between date'2024-08-27' - interval '59' day and date'2024-08-27')) > 0,1,0) as is_a60,
        if(cardinality(filter(agg.delivered_date_agg,x -> x between date'2024-08-27' - interval '3' day and date'2024-08-27' - interval '1' day)) > 0,1,0) as l3d




from driver_ops_driver_performance_tab r 

left join raw_check rc on rc.shipper_id = r.shipper_id

left join agg on agg.shipper_id = r.shipper_id

where r.report_date between date'2024-08-27' and date'2024-08-30'
and r.city_name in ('Ha Noi City','Quang Ninh','Thai Nguyen','Bac Ninh','Hai Phong City')
and r.total_order > 0 
and r.shipper_tier in ('Level 1','Level 2','Hub')
UNION ALL 

select 
        r.report_date,
        r.shipper_id,
        r.city_name,
        r.shipper_type,
        r.total_order,
        r.online_hour,
        r.sla_rate,
        rc.working_days,
        rc.total_order as total_3d_order,
        r.shipper_tier,
        if(cardinality(filter(agg.delivered_date_agg,x -> x between date'2024-08-27' - interval '59' day and date'2024-08-27')) > 0,1,0) as is_a60,
        if(cardinality(filter(agg.delivered_date_agg,x -> x between date'2024-08-27' - interval '3' day and date'2024-08-27' - interval '1' day)) > 0,1,0) as l3d




from driver_ops_driver_performance_tab r 

left join raw_check rc on rc.shipper_id = r.shipper_id

left join agg on agg.shipper_id = r.shipper_id

where r.report_date between date'2024-08-27' and date'2024-08-30'
and r.city_name in ('Hai Duong')
and r.total_order > 0 
)
