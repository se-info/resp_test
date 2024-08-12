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

inner join driver_ops_spike_day_reactivate_hub i on h.uid = cast(i.shipper_id as bigint)

where 1 = 1 
and h.date_ = date'2024-08-08'
group by 1,2

having (sum(h.total_order) > 0 )
)
select *
from
(select 
        raw.date_,
        raw.uid as shipper_id,
        sm.shipper_name,
        sm.city_name,
        case
        when total_ignore = 0 and total_deny = 0 and total_order between 16 and 24 then 30000
        when total_ignore = 0 and total_deny = 0 and total_order between 25 and 29 then 70000
        when total_ignore = 0 and total_deny = 0 and total_order >= 30 then 100000 
        else 0 end as bonus_,
        'spf_do_0004|San don tang thu nhap_'||date_format(raw.date_,'%Y-%m-%d') as note_,
        total_order


from raw 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.uid and sm.grass_date = 'current'

)

where bonus_ > 0 


