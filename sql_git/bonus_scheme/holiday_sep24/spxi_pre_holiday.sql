-- select shipper_id from driver_ops_spxi_normal_scheme
with raw as 
(select 
        a.id,
        a.order_code,
        a.order_type,
        a.shipper_id,
        di.name_en as district_name,
        date(a.delivered_timestamp) as report_date,
        row_number()over(partition by a.shipper_id order by a.id asc) as rank_

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab a 

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = a.district_id

where 1 = 1 
and a.shipper_id > 0 
and a.order_status in ('Delivered')
and date(a.delivered_timestamp) between date'2024-08-29' and date'2024-08-30'
and a.order_type != 0
)
,check as 
(select 
        r.shipper_id,
        count(distinct r.report_date) as original_working_days,
        count(distinct case when ds.online >= 5 then report_date else null end) as qualified_working,
        array_agg(ds.online) as online_info

from driver_ops_driver_performance_tab r 

left join 
(select date(created) as created_date,uid,sum(online_by_hour/3600.00) as online from driver_ops_driver_supply_tab
group by 1,2) ds on ds.uid = r.shipper_id and ds.created_date = r.report_date

inner join (select shipper_id from driver_ops_spxi_normal_scheme) l on cast(l.shipper_id as bigint) = r.shipper_id

where r.total_order > 0
and r.report_date between date'2024-08-29' and date'2024-08-30'
group by 1
)
,eligible_driver as 
(select 
        shipper_id,
        sum(total_order_spxi) as total_order,
        sum(bonus_value) as bonus_value

from
(select 
        r.report_date,
        r.shipper_id,
        r.total_order,
        r.total_order_spxi,
        c.qualified_working,
        c.online_info,
        case 
        when c.qualified_working = 2 and r.total_order_spxi >= 12 and r.sla_rate >= 90 then 50000
        else 0 end as bonus_value
        

from driver_ops_driver_performance_tab r

left join check c on c.shipper_id = r.shipper_id

inner join (select shipper_id from driver_ops_spxi_normal_scheme) l on cast(l.shipper_id as bigint) = r.shipper_id

where r.total_order > 0
and r.report_date between date'2024-08-29' and date'2024-08-30'
)
group by 1 
having sum(bonus_value) > 0
)
select
        raw.*,
        sm.shipper_name,
        sm.city_name,
        el.total_order,
        el.bonus_value/10 as bonus_value,
        'Thuong don SPX '||date_format(raw.report_date,'%d/%m/%Y')||'_'||cast(raw.id as varchar) as note_

from raw 
left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.shipper_id and sm.grass_date = 'current'
inner join eligible_driver el on el.shipper_id = raw.shipper_id
where raw.rank_ <= 10

