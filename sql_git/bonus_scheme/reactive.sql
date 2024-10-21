with raw as 
(select 
        raw.report_date,
        raw.shipper_id,
        sm.shipper_name,
        raw.city_name,
        case 
        when regexp_like(raw.city_name,'Binh Dinh|Binh Thuan|Long An|An Giang|Tien Giang|Dak Lak|Thanh Hoa|Nam Dinh City|Hai Duong|Phu Yen|Dong Thap|Kien Giang') then 0 else 1 end as is_apply_sla,
        raw.total_order,
        raw.sla_rate



from driver_ops_driver_performance_tab raw 

inner join dev_vnfdbi_opsndrivers.driver_ops_spike_day_reactivate_non_hub rl on cast(rl.shipper_id as bigint) = raw.shipper_id

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.shipper_id and sm.grass_date = 'current'

where raw.city_name not in ('HCM City','Ha Noi City')
and raw.report_date between date'2024-10-20' and date'2024-10-20'
)
select 
        sum(bonus_value),
        count(distinct shipper_id)
from
(select 
        *,
        case 
        when raw.is_apply_sla = 1 and raw.total_order >= 16 and raw.sla_rate >= 95 then 70000
        when raw.is_apply_sla = 1 and raw.total_order between 10 and 15 and raw.sla_rate >= 95 then 30000

        when raw.is_apply_sla = 0 and raw.total_order >= 16 then 70000
        when raw.is_apply_sla = 0 and raw.total_order between 10 and 15 then 30000
        else 0 end as bonus_value

from raw 
)
where bonus_value > 0 
