select  reference_id
        ,case when type = 1 then 'News'
             when type = 2 then 'Policy'
             when type = 3 then 'Annoucement'
             when type = 4 then 'System'
             else 'Default' end as push_type
        ,a.shipper_uid
        ,sm.shipper_name
        ,sm.city_name
        ,case when sm.shipper_type_id = 12 then 'hub' else 'non hub' end as working_group
        ,case when view_time > 0 then 'Viewed'
              when receive_time > 0 then 'Received'
              else 'Non received' end as driver_action              
        ,from_unixtime(receive_time - 3600) as receive_time
        ,from_unixtime(view_time - 3600) as view_time
        ,from_unixtime(push_time - 3600) as push_time
        ,date(from_unixtime(push_time - 3600)) as push_date               









from shopeefood.foody_internal_db__shipper_push_user_tab__reg_daily_s0_live a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_uid and try_cast(sm.grass_date as date) = date(from_unixtime(a.push_time - 3600))


where reference_id in (9124,9130)