with date_dim AS (
SELECT
    date_
FROM
    ((SELECT sequence(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, current_date - interval '1' day) bar)
CROSS JOIN
    unnest (bar) as t(date_)
))
,num_of_week as
(select 
    DATE_FORMAT(DATE_TRUNC('week',date_), 'W%v') as week_
    ,count(distinct date_) as days
from date_dim 
group by 1
)
,driver_order AS 
(
SELECT *
FROM
    (SELECT
        shipper_id
        , report_date
        , MIN(report_date) OVER (PARTITION BY shipper_id) AS first_date
        , order_uid
        , order_status
    FROM
        (SELECT dot.uid as shipper_id
              ,dot.ref_order_id as order_id
              ,dot.ref_order_code as order_code
              ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
              ,dot.ref_order_category
              ,case when dot.ref_order_category = 0 then 'order_delivery'
                    when dot.ref_order_category = 3 then 'now_moto'
                    when dot.ref_order_category = 4 then 'now_ship'
                    when dot.ref_order_category = 5 then 'now_ship'
                    when dot.ref_order_category = 6 then 'now_ship_shopee'
                    when dot.ref_order_category = 7 then 'now_ship_sameday'
                    else null end source
              ,dot.ref_order_status
              ,dot.order_status
              ,case when dot.order_status = 1 then 'Pending'
                    when dot.order_status in (100,101,102) then 'Assigning'
                    when dot.order_status in (200,201,202,203,204) then 'Processing'
                    when dot.order_status in (300,301) then 'Error'
                    when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
                    else null end as order_status_group
              ,dot.is_asap
              ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
              ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
              ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
            --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
              ,case when dot.pick_city_id = 217 then 'HCM'
                    when dot.pick_city_id = 218 then 'HN'
                    when dot.pick_city_id = 219 then 'DN'
                    ELSE 'OTH' end as city_group
        FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
        LEFT JOIN shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet 
            on dot.id = dotet.order_id
        where dot.pick_city_id <> 238
        and dot.order_status = 400
        )
    )

)
,agg_delivered_date_tab as
(
    select 
        shipper_id
        ,first_date
        ,array_agg(distinct report_date) as agg_delivered_date
    from driver_order
    group by 1,2
)
,account_open_data as
(select 
    uid as shipper_id
    ,min(date(from_unixtime(create_time-3600))) account_open_date
from shopeefood.foody_internal_db__shipper_log_change_tab__reg_daily_s0_live
where 1=1
and change_type = 'IsActive'
and from_value = '3' and to_value = '1'
group by 1
)
,driver_base as
(select 

    TRY_CAST(sm.grass_date AS DATE) AS report_date
    ,sm.shipper_id
    ,sm.shipper_status_code
    ,sm.shipper_type_id
    ,sm.city_name
    ,ad.agg_delivered_date
    ,ad.first_date
    ,new.account_open_date
    ,coalesce(cardinality(filter(agg_delivered_date, x -> x = try_cast(grass_date as date))),0) is_a1
    ,coalesce(cardinality(filter(agg_delivered_date, x -> x between try_cast(grass_date as date) - interval '7' day and try_cast(grass_date as date) - interval '1' day )),0) is_a2_8
    ,coalesce(cardinality(filter(agg_delivered_date, x -> x between try_cast(grass_date as date) - interval '14' day and try_cast(grass_date as date) - interval '8' day )),0) is_a9_15
    ,coalesce(cardinality(filter(agg_delivered_date, x -> x between try_cast(grass_date as date) - interval '29' day and try_cast(grass_date as date) - interval '15' day )),0) is_a16_30
    ,coalesce(cardinality(filter(agg_delivered_date, x -> x < try_cast(grass_date as date) - interval '29' day )),0) is_over_30
FROM shopeefood.foody_mart__profile_shipper_master sm
-- INNER JOIN params p ON TRY_CAST(sm.grass_date AS DATE) BETWEEN p.start_date AND p.end_date
left join agg_delivered_date_tab ad on sm.shipper_id =  ad.shipper_id
left join account_open_data as new on sm.shipper_id = new.shipper_id
where sm.grass_date != 'current'
and try_cast(sm.grass_date as date) BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month AND current_date - interval '1' day
)
,base as
(select 
    DATE_FORMAT(DATE_TRUNC('week',report_date), 'W%v') as week_
    ,report_date
    ,city_name
    ,case when shipper_type_id = 12 then 'Hub' else 'Non-Hub' end hub_type
    ,case 
    when is_a1 > 0 and report_date = first_date then '1.new'
    when is_a1 > 0 and is_a2_8 > 0 then '2.A2_8'
    when is_a1 > 0 and is_a9_15 > 0 then '3.A9_15'
    when is_a1 > 0 and is_a16_30 > 0 then '4.A16_30'
    when is_a1 > 0 and is_over_30 > 0 then '5.AOver_30'
    end as driver_type
    ,case 
        when is_a1 > 0 and date(account_open_date) between report_date - interval '13' day and report_date - interval '0' day then 'new_onboarding' 
        when is_a1 > 0 and date(account_open_date) between report_date - interval '29' day and report_date - interval '14' day then 'onboard_15_30'
        else null end as new_onboard
    -- ,case when is_a1 > 0 and date(account_open_date) between report_date - interval '14' day and report_date - interval '29' day then 'new_onboarding' else null end as new_onboard
    ,count(distinct case when is_a1 = 1 then shipper_id else null end) as total_drivers

from driver_base
group by 1,2,3,4,5,6
)
select 
    b.week_
    ,b.report_date
    ,b.city_name
    ,b.hub_type
    ,case 
    when b.new_onboard = 'new_onboarding' then '6.new_within_14_day' 
    when b.new_onboard = 'onboard_15_30' then '7.existing_onboard_15_30' 
    else b.driver_type end as category
    ,sum(b.total_drivers) as total_drivers
    ,w.days
from base as b
left join num_of_week as w on b.week_ = w.week_
where city_name in ('HCM City','Ha Noi City') and driver_type is not null
group by 1,2,3,4,5,7


