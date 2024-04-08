with hub_onboard AS
(SELECT
      shipper_id
    , shipper_ranking - type_ranking AS groupx_
    , MIN(report_date) AS first_join_hub
    , MAX(report_date) AS last_drop_hub
    , ROW_NUMBER()OVER(partition by shipper_id order by MIN(report_date) desc) as rank

FROM
    (SELECT
        shipper_id
        , shipper_type_id
        , DATE(grass_date) AS report_date
        , RANK() OVER (PARTITION BY shipper_id ORDER BY DATE(grass_date)) AS shipper_ranking
        , RANK() OVER (PARTITION BY shipper_id, shipper_type_id ORDER BY DATE(grass_date)) AS type_ranking
    FROM shopeefood.foody_mart__profile_shipper_master
    WHERE shipper_type_id IN (12, 11)
    AND grass_date != 'current'
    )
WHERE shipper_type_id = 12
GROUP BY 1,2
) 

,active as 
(
SELECT 
        a.uid as shipper_id
       ,a.report_date 
       ,a.city_name
       ,case when date_diff('day',hb.first_join_hub,a.report_date) <= 7 then 'New - 7 days' 
             when date_diff('day',hb.first_join_hub,a.report_date) <= 14 then 'New - 14 days'
             when date_diff('day',hb.first_join_hub,a.report_date) <= 30 then 'New - 30 days'
             else 'Existing Drivers' end as group_driver
       ,case 
            when do.shift_hour = 3 and do.start_time <= 6 then cast(do.shift_hour as varchar)||'hour shift - S'
            when do.shift_hour = 3 and do.start_time > 6 then cast(do.shift_hour as varchar)||'hour shift - C'
            when do.shift_hour = 5 and do.start_time <= 6 then cast(do.shift_hour as varchar)||'hour shift - S1'    
            when do.shift_hour = 5 and do.start_time <= 8 then cast(do.shift_hour as varchar)||'hour shift - S2'
            when do.shift_hour = 5 and do.start_time > 8 then cast(do.shift_hour as varchar)||'hour shift - C'              
            else cast(do.shift_hour as varchar)||'hour shift' end as hub_type
       ,count(distinct case when policy = 2 then ref_order_code else null end) as inshift_order
       ,count(distinct ref_order_code) as total_del
       
FROM
(SELECT   dot.uid
        ,case
        WHEN dot.pick_city_id = 217 then 'HCM'
        WHEN dot.pick_city_id = 218 then 'HN'
        ELSE NULL end as city_group
        ,date(FROM_UNIXTIME(dot.real_drop_time - 60*60)) as report_date
        -- case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        -- when dot.order_status in (402,403,404) and cast(json_extract(doet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(doet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
        -- else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
        --,(slot.end_time - slot.start_time)/3600 as shift_hour
        ,sm.city_name
        ,cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) policy
        ,dot.ref_order_code

        
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

--LEFT JOIN shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot on slot.uid = dot.uid and date(from_unixtime(slot.date_ts - 3600)) = date(from_unixtime(dot.real_drop_time - 3600))

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on dot.id = doet.order_id

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
WHERE 1=1
AND dot.order_status = 400
and dot.pick_city_id not in (238,469)
)a
LEFT JOIN 
            (
            SELECT 
                    date(from_unixtime(date_ts - 3600)) as date_
                    ,uid
                    ,slot_id
                    ,case when registration_status = 1 then 'Registered'
                        when registration_status = 2 then 'OFF'
                        when registration_status = 3 then 'Worked'
                        end as registration_status
                    ,(end_time - start_time)/3600 as shift_hour
                    ,start_time/3600 as start_time

            from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live 
            ) do on do.uid = a.uid and do.date_ = a.report_date

left join hub_onboard hb on hb.shipper_id = a.uid and hb.rank = 1 

where report_date = current_date - interval '1' day
-- between ${start_date} and ${end_date}

and policy = 2 
group by 1,2,3,4,5
)

select 
        report_date
       ,city_name
       ,hub_type
       ,group_driver  
       ,count(distinct shipper_id) as total_active 
       ,sum(inshift_order) as total_del

from active 


group by 1,2,3
