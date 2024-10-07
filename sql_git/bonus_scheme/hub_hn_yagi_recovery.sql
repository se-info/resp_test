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
and h.total_order > 0 
group by 1

)
,eligible as 
(select 
        raw.uid as shipper_id,
        sm.shipper_name,
        sm.city_name,
        raw.total_ignore,
        raw.total_deny,
        cardinality(raw.delivered_date_agg) as working_day,
        case 
        when raw.total_ignore = 0 and raw.total_deny = 0 and cardinality(raw.delivered_date_agg) >= 4 then 1 else 0 end as is_qualified_bonus,
        raw.total_order,
        case 
        when raw.total_ignore = 0 and raw.total_deny = 0 and cardinality(raw.delivered_date_agg) >= 4 and raw.total_order between 80 and 119 then 200000
        when raw.total_ignore = 0 and raw.total_deny = 0 and cardinality(raw.delivered_date_agg) >= 4 and raw.total_order >= 120 then 400000
        else 0 end as total_bonus

from raw 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.uid and sm.grass_date = 'current'
where (case 
        when raw.total_ignore = 0 and raw.total_deny = 0 and cardinality(raw.delivered_date_agg) >= 4 and raw.total_order between 80 and 119 then 200000
        when raw.total_ignore = 0 and raw.total_deny = 0 and cardinality(raw.delivered_date_agg) >= 4 and raw.total_order >= 120 then 400000
        else 0 end) is not null 
)
select* from eligible
select
        h.date_ as report_date,
        h.uid,
        el.shipper_name,
        el.city_name,
        el.total_deny,
        el.total_ignore,
        el.working_day,
        el.total_order,
        el.total_bonus*1.00/el.working_day as bonus_allocate,
        SUM(h.total_order) as cnt_order,
        'spf_do_0004|San don tang thu nhap_'||date_format(h.date_,'%Y-%m-%d') as txn_note


from driver_ops_hub_driver_performance_tab h 

inner join eligible el on el.shipper_id = h.uid
where 1 = 1 
and h.date_ between date'2024-09-26' and date'2024-09-30'
and h.total_order > 0 
group by 1,2,3,4,5,6,7,8,9,11
order by 2,1
;

