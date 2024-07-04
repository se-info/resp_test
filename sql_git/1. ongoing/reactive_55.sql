with reactive_list as 
(select 
        case 
        when regexp_like(sm.city_name,'Binh Dinh|Binh Thuan|Long An|An Giang|Tien Giang|Dak Lak|Thanh Hoa|Hai Duong|Nam Dinh City') then 0 else 1 end as is_apply_sla,
        a.*,
        sm.shipper_name,
        sm.city_name

from vnfdbi_opsndrivers.phong_test_table a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = cast(a.shipper_id as bigint) and sm.grass_date = 'current'

-- Quy Nhơn, Phan Thiết, Tân An, Long Xuyên, Mỹ Tho, Buôn Ma Thuột, Thanh Hóa không apply sla
)
-- select count(*) from reactive_list
select
        rl.*,
        raw.city_name,
        raw.total_order,
        raw.sla_rate,
        case 
        when rl.is_apply_sla = 1 and raw.total_order between 10 and 15 and raw.sla_rate >= 95 then 25000
        when rl.is_apply_sla = 1 and raw.total_order >= 16 and raw.sla_rate >= 95 then 50000

        when rl.is_apply_sla = 0 and raw.total_order between 10 and 15 then 25000
        when rl.is_apply_sla = 0 and raw.total_order >= 16 then 50000
        else 0 end as bonus_value


from reactive_list rl

left join driver_ops_driver_performance_tab raw on cast(rl.shipper_id as bigint) = raw.shipper_id and raw.report_date = date'2024-05-05'
where raw.total_order > 0
