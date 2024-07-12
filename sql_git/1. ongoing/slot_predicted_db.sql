with mapping_tab as
(SELECT
        (end_time - start_time)/3600 as shift_category,
        CAST((end_time - start_time)/3600 AS VARCHAR)||'_hour_shift_'||CAST(start_time/3600 AS VARCHAR) AS shift_hour,
        id AS working_time_id,
        hi.hub_id,
        hi.hub_name,
        hi.city,
        t.report_date,
        day_of_week(t.report_date) as "day_of_week"

FROM (SELECT 1 AS mapping,* FROM shopeefood.foody_internal_db__shipper_config_working_time_tab__vn_daily_s0_live) config


LEFT JOIN 
(SELECT 
        1 as mapping,
        hub_name,
        id as hub_id,
        IF(city_id=217,'HCM','HN') as city
FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
WHERE city_id IN (217,218)
AND driver_count > 0
) hi ON config.mapping = hi.mapping

CROSS JOIN UNNEST (sequence(date'2024-07-15',date'2024-07-15' + interval '6' day)) as t(report_date)

WHERE config_status = 1
and id not in (1,11)
and id in (5,6,25,8,27,21,3,30,31,18,32,24,26,29)
and hi.city = 'HCM'
UNION ALL 
SELECT
        (end_time - start_time)/3600 as shift_category,
        CAST((end_time - start_time)/3600 AS VARCHAR)||'_hour_shift_'||CAST(start_time/3600 AS VARCHAR) AS shift_hour,
        id AS working_time_id,
        hi.hub_id,
        hi.hub_name,
        hi.city,
        t.report_date,
        day_of_week(t.report_date) as "day_of_week"

FROM (SELECT 1 AS mapping,* FROM shopeefood.foody_internal_db__shipper_config_working_time_tab__vn_daily_s0_live) config


LEFT JOIN 
(SELECT 
        1 as mapping,
        id as hub_id,
        hub_name,
        IF(city_id=217,'HCM','HN') as city
FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
WHERE city_id IN (217,218)
AND driver_count > 0
) hi ON config.mapping = hi.mapping

CROSS JOIN UNNEST (sequence(date'2024-07-15',date'2024-07-15' + interval '6' day)) as t(report_date)

WHERE config_status = 1
and id not in (1,11)
and id in (5,6,25,8,27,21,3,30,31,28,17,32,16,26,29)
and hi.city = 'HN'
)
select
        m.city,
        '' AS "num",
        -- m.shift_hour,
        m.report_date,
        m.hub_id,
        m.shift_category,
        1 AS "config_status",
        m.working_time_id,
        '' AS start_time,
        '' AS end_time,
        case 
        when m.shift_hour = '3_hour_shift_0' then hs."3_hour_shift_0"
        when m.shift_hour = '3_hour_shift_3' then hs."3_hour_shift_3"
        when m.shift_hour = '3_hour_shift_6' then hs."3_hour_shift_6"
        when m.shift_hour = '3_hour_shift_19' then hs."3_hour_shift_19"
        when m.shift_hour = '3_hour_shift_16' then hs."3_hour_shift_16"
        when m.shift_hour = '3_hour_shift_20' then hs."3_hour_shift_20"
        when m.shift_hour = '3_hour_shift_21' then hs."3_hour_shift_21"
        when m.shift_hour = '5_hour_shift_5' then hs."5_hour_shift_6"
        when m.shift_hour = '5_hour_shift_8' then hs."5_hour_shift_8"
        when m.shift_hour = '5_hour_shift_10' then hs."5_hour_shift_11"
        when m.shift_hour = '5_hour_shift_16' then hs."5_hour_shift_16"
        when m.shift_hour = '5_hour_shift_17' then hs."5_hour_shift_18"
        when m.shift_hour = '8_hour_shift_10' then hs."8_hour_shift_11"
        when m.shift_hour = '10_hour_shift_10' then hs."10_hour_shift_10"
        else '' end as max_drivers


from mapping_tab m 

left join (select 
                *,row_number()over(partition by hub_location,"day_of_week" order by cast(updated_date as bigint) desc) as rank_ 
            from shopeefood_assignment.hcm_slot_schedule_tab
            ) hs 
        on hs.hub_location = m.hub_name 
        and cast(hs."day_of_week" as bigint) = m."day_of_week"
        and hs.rank_ = 1 

WHERE m.city = 'HCM'
UNION ALL
select 
        m.city,
        '' AS "num",
        -- m.shift_hour,
        m.report_date,
        m.hub_id,
        m.shift_category,
        1 AS "config_status",
        m.working_time_id,
        '' AS start_time,
        '' AS end_time,
        case 
        when m.shift_hour = '3_hour_shift_0' then hs."3_hour_shift_0"
        when m.shift_hour = '3_hour_shift_3' then hs."3_hour_shift_3"
        when m.shift_hour = '3_hour_shift_6' then hs."3_hour_shift_7"
        when m.shift_hour = '3_hour_shift_10' then hs."3_hour_shift_10"
        when m.shift_hour = '3_hour_shift_16' then hs."3_hour_shift_16"
        when m.shift_hour = '3_hour_shift_17' then hs."3_hour_shift_18"
        when m.shift_hour = '3_hour_shift_20' then hs."3_hour_shift_20"
        when m.shift_hour = '3_hour_shift_21' then hs."3_hour_shift_21"
        when m.shift_hour = '5_hour_shift_5' then hs."5_hour_shift_6"
        when m.shift_hour = '5_hour_shift_8' then hs."5_hour_shift_8"
        when m.shift_hour = '5_hour_shift_10' then hs."5_hour_shift_11"
        when m.shift_hour = '5_hour_shift_16' then hs."5_hour_shift_16"
        when m.shift_hour = '5_hour_shift_17' then hs."5_hour_shift_18"
        when m.shift_hour = '8_hour_shift_10' then hs."8_hour_shift_11"
        when m.shift_hour = '10_hour_shift_10' then hs."10_hour_shift_10"
        else '' end as max_drivers


from mapping_tab m 

left join (select 
                *,row_number()over(partition by hub_location,"day_of_week" order by cast(updated_date as bigint) desc) as rank_ 
            from shopeefood_assignment.hn_slot_schedule_tab
            ) hs 
        on hs.hub_location = m.hub_name 
        and cast(hs."day_of_week" as bigint) = m."day_of_week"
        and hs.rank_ = 1 
WHERE m.city = 'HN'