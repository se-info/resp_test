select 
		uid,
        from_unixtime(create_time - 3600) as created,
        from_unixtime(location_create_time- 3600) as loc_created,
        json_extract(extra_data,'$.prev_update_status.is_success')

from shopeefood.foody_partner_archive_db__shipper_location_log_tab__reg_daily_s0_live
where date(from_unixtime(create_time - 3600)) < date(from_unixtime(location_create_time- 3600)) 
and date(from_unixtime(create_time)) = current_date - interval '1' day
