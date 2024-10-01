with raw as 
(select 
        h.uid,
        array_agg(kpi_failed) as kpi_check,
        sum(ignore_count) as total_ignore,
        sum(deny_count) as total_deny,
        sum(h.total_order) as total_order,
        array_agg(distinct h.date_) as delivered_date_agg

from driver_ops_hub_driver_performance_tab h 

inner join driver_ops_hub_recovery_list i on h.uid = cast(i.shipper_id as bigint)

where 1 = 1 
and h.date_ between date'2024-09-26' and date'2024-09-30'
group by 1

having (sum(h.total_order) > 0 )
)
select 
        uid as shipper_id,
        total_ignore,
        total_deny,
        cardinality(delivered_date_agg) as working_day,
        case 
        when total_ignore = 0 and total_deny = 0 and cardinality(delivered_date_agg) >= 4 then 1 else 0 end as is_qualified_bonus,
        total_order

from raw 



