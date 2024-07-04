WITH raw AS 
(SELECT
    reg.date_,
    reg.slot_id,
    reg.uid,
    reg.shift_hour AS hub_type_original,
    reg.hub_type_x_start_time,
    reg.start_shift_original,
    reg.end_shift_original,
    shift_hour_original AS shift_hour

FROM
(SELECT
    date(from_unixtime(date_ts - 3600)) as date_
    ,uid
    ,case when registration_status = 1 then 'Registered'
    when registration_status = 2 then 'OFF'
    when registration_status = 3 then 'Worked'
    end as registration_status
        
    ,CAST(((end_time - start_time)/3600) AS VARCHAR) ||' '||'hour shift' as shift_hour
    ,CAST(((end_time - start_time)/3600) AS VARCHAR) ||' '||'hour shift'||'-'||CAST(ROUND(start_time*1.0/3600) AS VARCHAR) AS hub_type_x_start_time
    ,start_time/3600 as start_shift
    ,end_time/3600 AS end_shift  
    ,(end_time-start_time)/3600 as shift_hour_original 
    ,slot_id
    ,DATE_ADD('second',(start_time),from_unixtime(date_ts - 3600)) AS start_shift_original
    ,DATE_ADD('second',(end_time),from_unixtime(date_ts - 3600)) AS end_shift_original

FROM shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
WHERE registration_status != 2 
) reg   
WHERE 1 = 1 
AND reg.date_ BETWEEN current_date - interval '35' day and current_date - interval '1' day
)
,base AS
(SELECT 
         sts.uid as shipper_id
        ,raw.hub_type_original 
        ,raw.slot_id 
        ,raw.start_shift_original
        ,raw.end_shift_original 
        ,raw.shift_hour
        ,date(from_unixtime(sts.create_time - 60*60)) as report_date
        ,from_unixtime(sts.check_in_time - 60*60) as check_in_time
        ,from_unixtime(sts.check_out_time - 60*60) as check_out_time
        ,from_unixtime(sts.order_start_time - 60*60) as order_start_time
        ,from_unixtime(sts.order_end_time - 60*60) as order_end_time
                          
        ,sts.total_online_seconds*1.00/(60*60) as total_online_hours
        ,(sts.check_out_time - sts.check_in_time)*1.00/(60*60) as online
        ,sts.total_work_seconds*1.00/(60*60) as total_work_hours
        ,(sts.order_end_time - sts.order_start_time)*1.00/(60*60) as work

        -- for checking
        ,check_in_time as check_in_time_original
        ,check_out_time as check_out_time_original
        ,order_start_time as order_start_time_original
        ,order_end_time as order_end_time_original

        ,from_unixtime(check_out_time - 3600) as original_end_time_online
                     
        ,from_unixtime(sts.check_in_time - 60*60) as actual_start_time_online
        ,greatest(from_unixtime(sts.check_out_time - 60*60),from_unixtime(sts.order_end_time - 60*60)) as actual_end_time_online

        ,case when sts.order_start_time = 0 then from_unixtime(sts.check_in_time - 60*60) else from_unixtime(sts.order_start_time - 60*60) end as actual_start_time_work
        ,case when sts.order_end_time = 0 then from_unixtime(sts.check_in_time - 60*60) else from_unixtime(sts.order_end_time - 60*60) end as actual_end_time_work
        ,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP) + interval '11' hour as noon_peak_start
        ,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP) + interval '12' hour as noon_peak_end
        ,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP) + interval '18' hour as night_peak_start
        ,cast(date(from_unixtime(check_in_time - 3600)) as TIMESTAMP) + interval '19' hour as night_peak_end

FROM shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live sts
LEFT JOIN raw 
    on raw.date_ = date(from_unixtime(sts.create_time - 60*60))
    and raw.uid = sts.uid
    and from_unixtime(sts.check_in_time - 60*60) between raw.start_shift_original and raw.end_shift_original

WHERE date(from_unixtime(sts.create_time - 60*60)) between current_date - interval '7' day and current_date - interval '1' day
)

,online AS
(SELECT 
        shipper_id,
        report_date,
        slot_id,
        MAX(shift_hour_second) AS shift_hour,
        SUM(in_shift_online_time) AS online_in_shift,
        SUM(in_shift_online_time)/CAST(MAX(shift_hour_second)/3600 AS DOUBLE) AS pct_online,
        SUM(COALESCE(noon_peak_online_time,0)+COALESCE(night_peak_online_time,0))/CAST(MAX(peak_online_time) AS DOUBLE) AS pct_peak

FROM
(SELECT 
         base.report_date 
        ,base.shipper_id
        ,base.hub_type_original 
        ,base.start_shift_original,base.end_shift_original,base.noon_peak_start,noon_peak_end,night_peak_start,night_peak_end
        ,base.slot_id,
        CASE 
        WHEN shift_hour IN (8,10) THEN 2 
        WHEN shift_hour = 5 AND HOUR(start_shift_original) >= 8 THEN 1 
        WHEN shift_hour = 5 AND HOUR(start_shift_original) >= 17 THEN 1 
        WHEN shift_hour = 3 AND HOUR(start_shift_original) >= 10 THEN 1
        ELSE 0 END AS peak_online_time

        ,DATE_DIFF('second',base.start_shift_original,base.end_shift_original) AS shift_hour_second
                
        ,date_diff('second',base.actual_start_time_online,base.actual_end_time_online)*1.0000/(60*60) as total_online_time
        ,date_diff('second',base.actual_start_time_work,base.actual_end_time_work)*1.0000/(60*60) as total_working_time
                 
        ,case when base.actual_end_time_online < base.start_shift_original then 0
            when base.actual_start_time_online > base.end_shift_original then 0
            else date_diff('second',   greatest(base.start_shift_original,base.actual_start_time_online)   ,   least(base.end_shift_original,base.actual_end_time_online)   )*1.0000/(60*60)
            end as in_shift_online_time
        ,case when base.actual_end_time_work < base.start_shift_original then 0
            when base.actual_start_time_work > base.end_shift_original then 0
            else date_diff('second',   greatest(base.start_shift_original,base.actual_start_time_work)   ,   least(base.end_shift_original,base.actual_end_time_work)   )*1.0000/(60*60)
            end as in_shift_work_time

        ,case when base.original_end_time_online < base.noon_peak_start then 0
            when base.actual_start_time_online > base.noon_peak_end then 0
            else date_diff('second',   greatest(base.noon_peak_start,base.actual_start_time_online)   ,   least(base.noon_peak_end,base.original_end_time_online)   )*1.0000/(3600)
            end as noon_peak_online_time

        -- ,case when base.actual_end_time_work < base.noon_peak_start then 0
        --     when base.actual_start_time_work > base.noon_peak_end then 0
        --     else date_diff('second',   greatest(base.noon_peak_start,base.actual_start_time_work)   ,   least(base.noon_peak_end,base.actual_end_time_work)   )*1.0000/(3600)
        --     end as noon_peak_work_time

        --- peak night
        ,case when base.original_end_time_online < base.night_peak_start then 0
            when base.actual_start_time_online > base.night_peak_end then 0
            else date_diff('second',   greatest(base.night_peak_start,base.actual_start_time_online)   ,   least(base.night_peak_end,base.original_end_time_online)   )*1.0000/(3600)
            end as night_peak_online_time

        -- ,case when base.actual_end_time_work < base.night_peak_start then 0
        --     when base.actual_start_time_work > base.night_peak_end then 0
        --     else date_diff('second',   greatest(base.night_peak_start,base.actual_start_time_work)   ,   least(base.night_peak_end,base.actual_end_time_work)   )*1.0000/(3600)
        --     end as night_peak_work_time


FROM base)
-- WHERE shipper_id = 858051 
WHERE report_date between current_date - interval '7' day and current_date - interval '1' day
GROUP BY 1,2,3
)
,assignment AS 
(SELECT
        shipper_id,
        slot_id,
        COUNT(CASE WHEN status IN (8,9,17,18) THEN ref_order_id ELSE NULL END) AS total_ignore,
        COUNT(CASE WHEN status IN (2,14,15) THEN ref_order_id ELSE NULL END) AS total_denied

FROM    
(SELECT 
        sa.ref_order_id,
        FROM_UNIXTIME(sa.create_time) AS created_timestamp,
        sa.shipper_uid AS shipper_id, 
        status,
        raw.hub_type_original,
        raw.slot_id


FROM vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab sa  
LEFT JOIN raw 
    on raw.uid = sa.shipper_uid 
    and FROM_UNIXTIME(sa.create_time) BETWEEN raw.start_shift_original AND raw.end_shift_original

WHERE raw.uid IS NOT NULL 
AND status IN (8,9,17,18,2,14,15)
)
GROUP BY 1,2
)
,final AS 
(SELECT  
      raw.date_,
      raw.uid,
      raw.slot_id,
      raw.hub_type_original,
      ol.online_in_shift,
      ol.pct_online,
      ol.pct_peak,
      COALESCE(sa.total_ignore,0) AS total_ignore,
      COALESCE(sa.total_denied,0) AS total_denied,
      COALESCE(hub.total_order,0) AS total_order,
      CASE 
      WHEN ol.pct_online >= 0.9 AND ol.pct_peak >= 1
        AND COALESCE(sa.total_ignore,0) = 0
        AND COALESCE(sa.total_denied,0) = 0 THEN 1 ELSE 0 END AS kpi 


FROM raw 

LEFT JOIN dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab hub
    on hub.uid = raw.uid 
    and hub.slot_id = raw.slot_id

LEFT JOIN online ol 
    on ol.shipper_id = raw.uid 
    and ol.slot_id = raw.slot_id

LEFT JOIN assignment sa 
    on sa.shipper_id = raw.uid 
    and sa.slot_id = raw.slot_id

WHERE 1 = 1 
AND raw.date_ BETWEEN current_date - interval '7' day AND current_date - interval '1' day
)
SELECT * FROM final WHERE total_order = 0 AND kpi = 1 
