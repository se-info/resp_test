select 
        DATE(FROM_UNIXTIME(r.date_ts - 3600)) as "ngày_đăng_ký",
        r.slot_id,
        r.uid as shipper_id,
        sp.shopee_uid,
        sm.shipper_name,
        sm.city_name,
        FROM_UNIXTIME(r.create_time - 3600) as "thời_gian_đăng_ký",
        hp.hub_type_x_start_time as "ca_làm_việc",
        date_diff('day',date(from_unixtime(sp.create_time - 3600)),current_date) as "thâm_niên_ngày",
        dp.sla_rate,
        hp.total_order as "Số_đơn_hoàn_thành_trong_ca"


from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live r 

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live sp on sp.uid = r.uid 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = r.uid and sm.grass_date = 'current'

left join driver_ops_driver_performance_tab dp on dp.report_date = DATE(FROM_UNIXTIME(date_ts - 3600)) and dp.shipper_id = r.uid 

left join driver_ops_hub_driver_performance_tab hp on hp.date_ = DATE(FROM_UNIXTIME(date_ts - 3600)) and hp.uid = r.uid and hp.slot_id = r.slot_id

where 1 = 1 
and DATE(FROM_UNIXTIME(r.date_ts - 3600)) = date'2024-05-05'
and FROM_UNIXTIME(r.create_time - 3600) >= cast('2024-05-05 17:00:00' as timestamp)
and r.registration_status != 2    