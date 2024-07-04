with data as 
(SELECT 
      from_unixtime(dot.real_drop_time - 3600) as last_delivered_timestamp
      ,dot.real_drop_time
      ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else date(from_unixtime(dot.submitted_time- 60*60)) end as created_date                    
      ,case when order_status = 400 then 'Delivered' else 'Other' end as order_status
      ,case when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
    when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
    when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
    when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
    when cast(from_unixtime(dot.submitted_time - 60*60) as date) between DATE('2022-01-01') and DATE('2022-01-02') then 202152
    else YEAR(cast(from_unixtime(dot.submitted_time - 60*60) as date))*100 + WEEK(cast(from_unixtime(dot.submitted_time - 60*60) as date)) end as created_year_week
      ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            else 'OTH' end as city_group 
       ,city.name_en as city_name
       ,dot.ref_order_id  
       ,dot.uid as shipper_id
       ,dot.is_asap
       ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as policy 
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_type
       ,date_diff('second',from_unixtime(dot.submitted_time - 3600),from_unixtime(dot.real_drop_time - 3600))*1.00/60 as completion_time



from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid 
                                                          and try_cast(sm.grass_date as date) = (case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                                                                                                when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                                                                                                else date(from_unixtime(dot.submitted_time- 60*60)) end )
where 1 = 1 
and (case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else date(from_unixtime(dot.submitted_time- 60*60)) end) between current_date - interval '90' day and current_date - interval '1' day
and dot.ref_order_category = 0
and dot.order_status = 400
and dot.is_asap = 1 
)
,params(period, start_date, end_date, days) AS (
    VALUES
    (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day), '%b'), DATE_TRUNC('month', current_date - interval '1' day), current_date - interval '1' day, CAST(DAY(current_date - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day), 'W%v'), DATE_TRUNC('week', current_date - interval '1' day), current_date - interval '1' day, CAST(DATE_DIFF('day', DATE_TRUNC('week', current_date - interval '1' day), current_date) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '1' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '8' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '15' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '22' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '35' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '35' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '29' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '42' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '42' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '36' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '49' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '49' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '43' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '56' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '56' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '50' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '63' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '63' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '57' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '70' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '70' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '64' day, CAST(7 AS DOUBLE))

    )

select   p.period
        ,p.days
        ,case when working_type = 'Hub' and policy = 2 then 'Hub' else 'Non Hub' end as order_type
        ,count(distinct ref_order_id) as total_order
        ,sum(case when is_asap = 1 and real_drop_time > 0 then completion_time else null end)/count(distinct case when is_asap = 1  then ref_order_id else null end) as lt_e2e

        from data a 
        inner  join params p on a.created_date between  p.start_date and p.end_date
        --where created_date between  DATE_TRUNC('week', current_date) - interval '70' day AND  DATE_TRUNC('week', current_date) - interval '1' day

        group by 1,2,3





