-- select * from driver_ops_hub_reactivate_scheme
with raw as 
(select 
        h.date_,
        h.uid,
        array_agg(kpi_failed) as kpi_check,
        sum(ignore_count) as total_ignore,
        sum(deny_count) as total_deny,
        sum(h.total_order) as total_order

from driver_ops_hub_driver_performance_tab h 

inner join driver_ops_hub_reactivate_scheme i on h.uid = cast(i.shipper_id as bigint)

where 1 = 1 
and h.date_ = date'2024-07-07'
group by 1,2

having (sum(h.total_order) > 0 )
)
select 
        *,
        case 
        when total_ignore = 0 and total_deny = 0 and total_order between 16 and 24 then 30000
        when total_ignore = 0 and total_deny = 0 and total_order between 25 and 29 then 70000
        when total_ignore = 0 and total_deny = 0 and total_order >= 30 then 100000
        else 0 end as bonus_
from raw 




