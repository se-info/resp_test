select 
         slot.report_date
        ,hour_
        ,CASE 
         WHEN sm.city_name = 'HCM City' then 'HCM'
         WHEN sm.city_name = 'Ha Noi City' then 'HN'
         WHEN sm.city_name = 'Da Nang City' then 'DN'
         ELSE 'OTH' end as city_group
        ,count(distinct case when slot.work_time > 0 then slot.shipper_id else null end) total_work_driver_overall
        ,count(distinct case when slot.online_time > 0 then slot.shipper_id else null end) total_online_driver_overall

from vnfdbi_opsndrivers.shopeefood_vn_driver_supply_hour_by_time_slot slot
left join shopeefood.foody_mart__profile_shipper_master sm
    on slot.shipper_id = sm.shipper_id and slot.report_date = try_cast(sm.grass_date as date)