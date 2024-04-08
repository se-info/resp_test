with hub_info as 
(select 
    date(from_unixtime(hub.report_date - 3600)) as report_date
    ,uid as shipper_id
    ,cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) as shift_category_name
    ,cast(json_extract(hub.extra_data,'$.total_order') as bigint) as total_order_inshift
    ,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
    else 0 end as extra_ship
    ,cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) as is_apply_fixed_amount -- check driver has order <= threshold and pass all kpi >> dieu kien de duoc bu min
    ,cast(json_extract(hub.extra_data,'$.total_income') as bigint) as total_income
    ,cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint) as calculated_shipping_shared
    -- ,hub.extra_data
    ,cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) as city_id
    ,case 
    	when cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) not in (217,218,220) then 999 
    	else cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) end as dummy_city_id
    ,cast(json_extract(hub.extra_data,'$.hub_ids') as array<int>) as hub_id
    ,cast(json_extract(hub.extra_data,'$.total_bonus') as bigint) as total_bonus
        , CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) AS hub_shift
    , CAST(json_extract(hub.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hub.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count
    , CAST(json_extract(hub.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 AS online_in_shift
    , CAST(json_extract(hub.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour
    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) start_shift
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) end_shift
    ,date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.00 as time_in_shift
    ,case 
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '10 hour shift' then 2
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '8 hour shift' then 2
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift' 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) > 6 then 1
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift' then 0             
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) > 6 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) < 20 then 1
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' then 0
        else null end as kpi_peak_hour


from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
)
,raw as 
(SELECT 
       from_unixtime(dot.real_drop_time - 3600) as last_delivered_timestamp
      ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else date(from_unixtime(dot.submitted_time- 60*60)) end as created_date                    
      ,case when order_status = 400 then 'Delivered' else 'Other' end as order_status
      ,case 
        when pick_city_id in (217,218,219) then 'T1'
        when pick_city_id in (222,273,221,230,220,223) then 'T2'
        when pick_city_id in (248,271,257,228,254,265,263) then 'T3'
        end as city_tier
       ,city.name_en as city_name
       ,ref_order_id as id 
       ,ref_order_code
       ,dot.uid as shipper_id
       ,dot.is_asap
       ,dot.ref_order_category
       ,dot.delivery_distance/cast(1000 as double) as distance
       ,dot.delivery_cost/cast(100 as double) as ship_fee 
       ,hi.shift_category_name
       ,hi.total_bonus
        ,case 
            when hi.online_in_shift/hi.time_in_shift >=0.9
            and hi.deny_count = 0
            and hi.ignore_count = 0
            and hi.is_auto_accept = true
            and hi.online_peak_hour >= hi.kpi_peak_hour
            then 1 else 0 end is_qualified_kpi        
       ,row_number()over(partition by dot.uid,date(from_unixtime(dot.real_drop_time - 3600)) order by from_unixtime(dot.real_drop_time - 3600) asc) as rank_order
       ,row_number()over(partition by dot.group_id order by from_unixtime(dot.submitted_time- 60*60) asc) as rank_group_order   
       ,dotet.order_data
       ,CASE 
            WHEN cast(json_extract(dotet.order_data,'$.shipper_policy.shift_category') as bigint) = 1 then '5 hour shift'
            WHEN cast(json_extract(dotet.order_data,'$.shipper_policy.shift_category') as bigint) = 2 then '8 hour shift'
            WHEN cast(json_extract(dotet.order_data,'$.shipper_policy.shift_category') as bigint) = 3 then '10 hour shift'
            WHEN cast(json_extract(dotet.order_data,'$.shipper_policy.shift_category') as bigint) = 4 then '3 hour shift'
            ELSE 'Non Hub' end as shift_category_name_v2

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

LEFT JOIN hub_info hi on hi.shipper_id = dot.uid and hi.report_date = date(from_unixtime(dot.real_drop_time - 3600))

-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

where 1 = 1 
and cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) = 2
and date(from_unixtime(dot.real_drop_time - 3600)) BETWEEN current_date - interval '7' day and current_date - interval '1' day
)
-- select * from raw
,metrics as 
(SELECT 
        date(raw.last_delivered_timestamp) as report_date
       ,raw.last_delivered_timestamp
       ,raw.rank_order
       ,raw.shipper_id
       ,raw.ref_order_code
       ,raw.is_qualified_kpi
       ,raw.shift_category_name_v2
       ,raw.city_name
       ,raw.distance
       ,raw.ship_fee
       ,raw.total_bonus
       ,13500 as current_ship_shared
       ,CASE 
            WHEN raw.distance <= 2 then 12500
            -- WHEN raw.distance <= 3.6 then 13500
            WHEN raw.distance > 2 then 13500
            END AS opt1_ship_shared
    --    ,CASE 
    --         WHEN raw.distance < 2 then 12500
    --         WHEN raw.distance <= 3.6 then 13500
    --         WHEN raw.distance > 3.6 then 15000
    --         END AS opt2_ship_shared
        ,CASE 
            WHEN raw.distance <= 2 then '1. 0 - 2km'
            -- WHEN raw.distance <= 3.6 then 13500
            WHEN raw.distance > 2 then '2. > 2km'
            END AS distance_range
       ,case /*when shift_category_name = '10 hour shift' and rank_order between 31 and 40 then 8000*/
             when shift_category_name_v2 = '10 hour shift' and rank_order > 30 then 6000
             
             when shift_category_name_v2 = '8 hour shift' and rank_order between 26 and 30 then 4000
             when shift_category_name_v2 = '8 hour shift' and rank_order > 30 then 6000

             when shift_category_name_v2 = '5 hour shift' and rank_order between 14 and 24 then 4000
             when shift_category_name_v2 = '5 hour shift' and rank_order > 24 then 6000             

             when shift_category_name_v2 = '3 hour shift' and rank_order between 7 and 14 then 2000
             when shift_category_name_v2 = '3 hour shift' and rank_order > 14 then 3000                             
             
             else 0 end as current_bonus

from raw

)

SELECT
        report_date
    --    ,shipper_id 
    --    ,city_name
       ,shift_category_name_v2
    --    ,distance_range
    --    ,SUM(current_ship_shared) AS current_ship_shared
    --    ,SUM(opt1_ship_shared) AS opt1_ship_shared
    --    ,SUM(CASE WHEN is_qualified_kpi = 1 then current_bonus ELSE 0 END) AS daily_bonus
    --    ,SUM( (CASE WHEN is_qualified_kpi = 1 then current_bonus ELSE 0 END) + current_ship_shared) AS total_income_current
    --    ,SUM( (CASE WHEN is_qualified_kpi = 1 then current_bonus ELSE 0 END) + opt1_ship_shared) AS total_income_opt1
    --    ,COUNT(DISTINCT ref_order_code) AS total_order
       ,COUNT(DISTINCT shipper_id) AS total_driver

FROM metrics 

-- WHERE shipper_id = 40021664
GROUP BY 1,2