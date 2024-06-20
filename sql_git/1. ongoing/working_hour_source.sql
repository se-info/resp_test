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
        raw.id

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join 
(select 
        group_id,
        count(id) as total_order_in_group,
        min(last_incharge_timestamp) as min_group_created,
        max(case 
            when order_status = 'Delivered' THEN delivered_timestamp
            when order_status = 'Returned' THEN returned_timestamp
            when order_status = 'Quit' THEN quit.quit_timestamp END) as max_group_delivered
from driver_ops_raw_order_tab oct
LEFT JOIN quit_log quit ON quit.order_id = oct.id
where group_id > 0 
and order_status IN ('Delivered','Quit','Returned')
group by 1 
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
        IF(group_id > 0,group_id,id) AS id,
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
-- SELECT 
--         start_time,
--         end_time,
--         shipper_id,
--         SUM(work_by_hour) AS work_by_hour
SELECT *,work_by_hour*1.0000/60
FROM
(SELECT 
        report_date,
        shipper_id,   
        id,
        start_time,
        end_time,
        delivered_timestamp,
        last_incharge_timestamp,
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
AND shipper_id = 9407689
-- AND id = 75124571
-- AND order_code = '19064-422249453'
AND report_date = current_date - interval '1' day
-- GROUP BY 1,2,3