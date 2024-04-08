SELECT   date(from_unixtime(slot.date_ts - 3600)) as date_ 
        ,year(from_unixtime(slot.date_ts - 3600))*100+week(from_unixtime(slot.date_ts - 3600)) as created_year_week
        ,slot.uid
        ,sm.city_name
        ,date_format(from_unixtime(slot.start_time - 27000),'%H:%i:%S') as start_time
        --,date_trunc('hour',from_unixtime(slot.start_time-21600)) as start_time_
        ,case when slot.registration_status = 1 then 'Registered'
              when slot.registration_status = 2 then 'OFF'
              when slot.registration_status = 3 then 'Worked'
              else null end as registration_status
        ,case when slot.registration_status = 2 then from_unixtime(slot.update_time - 3600)
              else null end as cancel_slot_ts
              
        FROM shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot 
        LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = slot.uid 
                  and try_cast(sm.grass_date as date) = date(from_unixtime(slot.date_ts - 3600))
                  
        WHERE 1 = 1 
        AND date(from_unixtime(slot.date_ts - 3600)) between date'2022-02-21' and date'2022-02-27'
        AND sm.city_id in (217,218)