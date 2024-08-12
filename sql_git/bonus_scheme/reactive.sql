with reactive_list as 
(select 
        case 
        when regexp_like(city_name,'Binh Dinh|Binh Thuan|Long An|An Giang|Tien Giang|Dak Lak|Thanh Hoa|Nam Dinh City|Hai Duong|Phu Yen|Dong Thap|Kien Giang') then 0 else 1 end as is_apply_sla,
        *

from dev_vnfdbi_opsndrivers.driver_ops_spike_day_reactivate_non_hub
-- Quy Nhơn, Phan Thiết, Tân An, Long Xuyên, Mỹ Tho, Buôn Ma Thuột, Thanh Hóa không apply sla
)
select count(*)
from
(select
        rl.*,
        raw.city_name,
        raw.total_order,
        raw.sla_rate,
        case 
        when rl.is_apply_sla = 1 and raw.total_order >= 16 and raw.sla_rate >= 95 then 50000
        when rl.is_apply_sla = 1 and raw.total_order between 10 and 15 and raw.sla_rate >= 95 then 25000

        when rl.is_apply_sla = 0 and raw.total_order >= 16 then 50000
        when rl.is_apply_sla = 0 and raw.total_order between 10 and 15 then 25000
        else 0 end as bonus_value,
        sm.shipper_name


from reactive_list rl

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = cast(rl.shipper_id as bigint) and sm.grass_date = 'current'

left join driver_ops_driver_performance_tab raw on cast(rl.shipper_id as bigint) = raw.shipper_id and raw.report_date = date'2024-08-08'
)
-- where total_order > 0
where bonus_value > 0 