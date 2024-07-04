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
,quit_log AS 
(SELECT
osl.order_id
, max(from_unixtime(osl.create_time - 3600)) quit_timestamp
FROM shopeefood.foody_order_db__order_status_log_tab_di osl
WHERE 1 = 1 
AND status = 9
GROUP BY 1
)
,order_raw AS 
(select 
        1 AS mapping,
        raw.order_code,
        raw.city_name,
        raw.shipper_id,
        raw.driver_policy,
        date(delivered_timestamp) as report_date,
        date(last_incharge_timestamp) as incharged_date,
        hour(delivered_timestamp) as hour_,
        raw.group_id,
        ogi.min_group_created,
        ogi.max_group_delivered,
        raw.created_timestamp,
        CASE 
        WHEN raw.group_id > 0 THEN ogi.min_group_created 
        ELSE raw.last_incharge_timestamp END AS last_incharge_timestamp, 
        CASE 
        WHEN raw.group_id > 0 THEN ogi.max_group_delivered 
        ELSE (case 
            when order_status = 'Delivered' THEN delivered_timestamp
            when order_status = 'Returned' THEN returned_timestamp
            when order_status = 'Quit' THEN quit.quit_timestamp END) END AS delivered_timestamp, 
        ogi.total_order_in_group,
        CASE 
        WHEN raw.group_id > 0 THEN (DATE_DIFF('second',ogi.min_group_created,ogi.max_group_delivered)*1.0000/60)/ogi.total_order_in_group
        ELSE DATE_DIFF('second',raw.last_incharge_timestamp,(case 
            when order_status = 'Delivered' THEN delivered_timestamp
            when order_status = 'Returned' THEN returned_timestamp
            when order_status = 'Quit' THEN quit.quit_timestamp END))*1.0000/60 END AS ata_adjust,
        raw.order_status,
        raw.id,
        ogi.group_code

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join 
(select 
        oct.group_id,
        ogi.group_code,
        count(oct.id) as total_order_in_group,
        min(last_incharge_timestamp) as min_group_created,
        max(case 
            when order_status = 'Delivered' THEN delivered_timestamp
            when order_status = 'Returned' THEN returned_timestamp
            when order_status = 'Quit' THEN quit.quit_timestamp END) as max_group_delivered
from driver_ops_raw_order_tab oct
LEFT JOIN (select id,group_code FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE date(dt) = current_date - interval '1' day) ogi
        on ogi.id = oct.group_id
LEFT JOIN quit_log quit ON quit.order_id = oct.id
where group_id > 0 
and order_status IN ('Delivered','Quit','Returned')
group by 1,2 
) ogi on ogi.group_id = (case when raw.group_id > 0 then raw.group_id else 0 end)

LEFT JOIN quit_log quit ON quit.order_id = raw.id

where 1 = 1 
and date(delivered_timestamp) >= date'2023-10-01'
and raw.order_status IN ('Delivered','Quit','Returned')
and raw.shipper_id > 0 
)
,sub_raw AS
(SELECT 
        report_date,
        shipper_id,
        IF(group_id > 0,group_code,order_code) AS id,
        mapping,
        COUNT(DISTINCT id) AS total_order_in_group,
        max_by(last_incharge_timestamp,id) as last_incharge_timestamp,
        max_by(delivered_timestamp,id) as delivered_timestamp

FROM order_raw
group by 1,2,3,4)
,o AS 
(SELECT 
        ol.*,
        lt.start_time,
        lt.end_time,
        lt.date_

FROM sub_raw ol 

LEFT JOIN list_time_range lt 
        ON lt.mapping = ol.mapping
        -- AND lt.date_ = ol.created
)
,driver_work AS 
(SELECT 
        start_time,
        end_time,
        shipper_id,
        SUM(work_by_hour*1.0000/60) AS work_hour
-- SELECT *,work_by_hour*1.0000/60,SUM(work_by_hour*1.0000/60)OVER(PARTITION BY shipper_id,start_time ORDER BY last_incharge_timestamp ASC)
FROM
(SELECT 
        report_date,
        shipper_id,   
        id,
        start_time,
        end_time,
        last_incharge_timestamp,
        delivered_timestamp,
        CASE 
        WHEN start_time <= last_incharge_timestamp and delivered_timestamp <= end_time THEN DATE_DIFF('second',GREATEST(last_incharge_timestamp,start_time),LEAST(delivered_timestamp,end_time))
        WHEN start_time >= last_incharge_timestamp and start_time <= delivered_timestamp and delivered_timestamp < end_time THEN DATE_DIFF('second',GREATEST(last_incharge_timestamp,start_time),LEAST(delivered_timestamp,end_time))
        WHEN start_time <= last_incharge_timestamp and last_incharge_timestamp <= end_time and end_time <= delivered_timestamp THEN DATE_DIFF('second',GREATEST(last_incharge_timestamp,start_time),LEAST(delivered_timestamp,end_time))
        WHEN start_time >= last_incharge_timestamp and end_time <= delivered_timestamp THEN DATE_DIFF('second',GREATEST(last_incharge_timestamp,start_time),LEAST(delivered_timestamp,end_time)) 
        ELSE 0 END AS work_by_hour,
        CASE 
        WHEN start_time <= last_incharge_timestamp and delivered_timestamp <= end_time THEN 1
        WHEN start_time >= last_incharge_timestamp and start_time <= delivered_timestamp and delivered_timestamp < end_time THEN 2
        WHEN start_time <= last_incharge_timestamp and last_incharge_timestamp <= end_time and end_time <= delivered_timestamp THEN 3
        WHEN start_time >= last_incharge_timestamp and end_time <= delivered_timestamp THEN 4        
        ELSE 0 END AS rule_checked
FROM o 
WHERE 1 = 1 
AND (report_date = date_ OR date(last_incharge_timestamp) = date_)
)
WHERE rule_checked > 0
GROUP BY 1,2,3
)
,driver_online AS 
(SELECT 
        d.created,
        d.start_time,
        d.end_time,
        d.uid AS shipper_id,
        SUM(online_by_hour) AS online_hour
FROM
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
) d 
WHERE rule_checked > 0
GROUP BY 1,2,3,4
)
select * from online_raw where uid = 42162630 AND created = date'2024-06-19'

SELECT 
        d.*,
        o.work_hour


FROM driver_online d 

LEFT JOIN driver_work o 
        ON o.start_time = d.start_time 
        AND o.shipper_id = d.shipper_id 

WHERE 1 = 1 
AND d.shipper_id = 42162630
AND d.created = current_date - interval '1' day
