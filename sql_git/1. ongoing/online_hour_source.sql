with dt_array_tab as
(select 1 mapping, sequence(cast(current_date - interval '90' day as timestamp ), cast(current_date as timestamp ), interval '3600' second  ) dt_array
) 
,list_time_range as
(select 
     t1.mapping
    ,DATE(t2.dt_array) AS date_
    ,t2.dt_array as start_time
    ,t2.dt_array + interval '3599.99' second as end_time
from dt_array_tab t1
cross join unnest (dt_array) as t2(dt_array)
)
,online_raw AS
(SELECT 
        1 AS mapping,
        DATE(FROM_UNIXTIME(create_time - 3600)) AS created,
        FROM_UNIXTIME(create_time - 3600) AS created_ts,
        uid,
        FROM_UNIXTIME(checkin_time - 3600) AS checkin_time,
        FROM_UNIXTIME(checkout_time - 3600) AS checkout_time,
        IF(status=1,'checked out','still online') AS status,
        DATE_DIFF('second',FROM_UNIXTIME(checkin_time - 3600),FROM_UNIXTIME(checkout_time - 3600))*1.0000/3600 AS online_hour,
        checkin_latitude,
        checkin_longitude


FROM shopeefood.foody_partner_db__shipper_checkin_checkout_log_tab__reg_daily_s0_live 


WHERE 1 = 1 
AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN CURRENT_DATE - INTERVAL '90' DAY AND CURRENT_DATE - INTERVAL '1' DAY
)
,f AS
(SELECT 
        ol.*,
        lt.start_time,
        lt.end_time,
        lt.date_

FROM online_raw ol 

LEFT JOIN list_time_range lt 
        ON lt.mapping = ol.mapping
        -- AND lt.date_ = ol.created
)
,driver_online AS 
(SELECT * FROM
(SELECT 
        created,
        uid,
        status,
        checkin_latitude,
        checkin_longitude,
        start_time,
        end_time,
        CASE 
        WHEN start_time <= checkin_time and checkout_time <= end_time THEN DATE_DIFF('second',GREATEST(checkin_time,start_time),LEAST(checkout_time,end_time))
        WHEN start_time >= checkin_time and start_time <= checkout_time and checkout_time < end_time THEN DATE_DIFF('second',GREATEST(checkin_time,start_time),LEAST(checkout_time,end_time))
        WHEN start_time <= checkin_time and checkin_time <= end_time and end_time <= checkout_time THEN DATE_DIFF('second',GREATEST(checkin_time,start_time),LEAST(checkout_time,end_time))
        WHEN start_time >= checkin_time and end_time <= checkout_time THEN DATE_DIFF('second',GREATEST(checkin_time,start_time),LEAST(checkout_time,end_time)) 
        ELSE 0 END AS online_by_hour,
        CASE 
        WHEN start_time <= checkin_time and checkout_time <= end_time THEN 1
        WHEN start_time >= checkin_time and start_time <= checkout_time and checkout_time < end_time THEN 2
        WHEN start_time <= checkin_time and checkin_time <= end_time and end_time <= checkout_time THEN 3
        WHEN start_time >= checkin_time and end_time <= checkout_time THEN 4        
        ELSE 0 END AS rule_checked
FROM f 
WHERE created = date_
)
WHERE rule_checked > 0)
SELECT * FROM driver_online

