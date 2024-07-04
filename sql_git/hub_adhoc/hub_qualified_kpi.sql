with kpi as 
(SELECT
    hc.uid AS shipper_id
    , DATE(FROM_UNIXTIME(hc.report_date - 3600)) AS report_date
    , CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) AS hub_shift
    , CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count
    , CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 AS online_in_shift
    , CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour
    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) start_shift
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) end_shift
    ,date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.00 as time_in_shift
    ,case 
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '10 hour shift' then 2
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '8 hour shift' then 2
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift' 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) > 6 then 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift' then 0             
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) > 6 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) < 20 then 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' then 0
        else null end as kpi_peak_hour
    ,extra_data        
FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
WHERE DATE(FROM_UNIXTIME(hc.report_date - 3600)) BETWEEN DATE'2022-08-27' AND DATE'2022-09-21'
-- and uid = 22937881  
)

select  shipper_id
    ,   case 
        when kpi.online_in_shift/kpi.time_in_shift >=0.9
        and kpi.deny_count = 0
        and kpi.ignore_count = 0
        and kpi.is_auto_accept = true
        and kpi.online_peak_hour >= kpi.kpi_peak_hour
        then 1 else 0 end is_qualified_kpi
    , case 
        when kpi.online_in_shift/kpi.time_in_shift >=0.9
        and kpi.deny_count = 0
        and kpi.ignore_count = 0
        and kpi.is_auto_accept = true
        and kpi.online_peak_hour >= kpi.kpi_peak_hour
        and COALESCE(o.inshift_delivered_orders, 0) > 0
        then 1 else 0 end is_eligible_day

from kpi











