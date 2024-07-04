-- drop table if exists shopeefood_bnp_adhoc_ticket_506;
-- create table shopeefood_bnp_adhoc_ticket_506 as
-- with transacting_driver as
-- (SELECT dot.uid as shipper_id
--               ,dot.ref_order_id as order_id
--               ,dot.ref_order_code as order_code
--               ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
--               ,dot.ref_order_category
--               ,case when dot.ref_order_category = 0 then 'order_delivery'
--                     when dot.ref_order_category = 3 then 'now_moto'
--                     when dot.ref_order_category = 4 then 'now_ship'
--                     when dot.ref_order_category = 5 then 'now_ship'
--                     when dot.ref_order_category = 6 then 'now_ship_shopee'
--                     when dot.ref_order_category = 7 then 'now_ship_sameday'
--                     else null end source
--               ,dot.ref_order_status
--               ,dot.order_status
--               ,case when dot.order_status = 1 then 'Pending'
--                     when dot.order_status in (100,101,102) then 'Assigning'
--                     when dot.order_status in (200,201,202,203,204) then 'Processing'
--                     when dot.order_status in (300,301) then 'Error'
--                     when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
--                     else null end as order_status_group
--               ,dot.is_asap
--               ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
--                     when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
--                     else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
--               ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
--               ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
--             --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
--               ,case when dot.pick_city_id = 217 then 'HCM'
--                     when dot.pick_city_id = 218 then 'HN'
--                     when dot.pick_city_id = 219 then 'DN'
--                     ELSE 'OTH' end as city_group
--         FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
--         LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet 
--             on dot.id = dotet.order_id
--         where dot.pick_city_id not in (0,238,468,469,470,471,472,227,269) -- 227: Bac Giag, 269 Tay Ninh
--         and dot.order_status = 400
--         and case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
--                     when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
--                     else date(from_unixtime(dot.submitted_time- 60*60)) end between date '2020-12-01' and current_date - interval '1' day
--         and dot.pick_city_id not in (0,238,468,469,470,471,472,227,269) -- 227: Bac Giag, 269 Tay Ninh
        
-- )
-- select 
--     *
-- from transacting_driver
-- -- limit 10

---- get a1,a30, net_ado
with order_level as
(select 
    shipper_id
    ,report_date
    ,count(distinct order_uid) orders
from shopeefood_bnp_adhoc_ticket_506
group by 1,2
)
,agg_delivered_date_tab as 
(select 
    shipper_id
    ,map_agg(report_date,orders) agg_delivered_date
from order_level
group by 1
)
,date_dim AS (
SELECT
    report_date
FROM
    ((SELECT sequence(date '2020-12-01',current_date - interval '1' day) bar)
CROSS JOIN
    unnest (bar) as t(report_date)
))
-- ,agg_delivered_date_tab as
-- (
--     select 
--         shipper_id
--         ,array_agg(distinct report_date) as agg_delivered_date
--     from shopeefood_bnp_adhoc_ticket_506
--     group by 1
-- )
,temp_tab as 
(select 
    d.report_date
    ,agg.shipper_id
    ,agg_delivered_date
    ,array_min(map_keys(agg_delivered_date)) as min_delivered_date
    ,array_max(map_keys(agg_delivered_date)) as max_delivered_date
    ,sm.city_name
    -- ,case when sm.shipper_type_id = 12 then 'hub' else 'non-hub' end as shipper_type
from agg_delivered_date_tab agg
cross join date_dim d
left join shopeefood.foody_mart__profile_shipper_master sm
    on d.report_date = try_cast(sm.grass_date as date) and agg.shipper_id = sm.shipper_id
where d.report_date >= array_min(map_keys(agg_delivered_date)) and d.report_date <= array_max(map_keys(agg_delivered_date))
)

-- select 
--     *
-- from temp_tab
-- limit 10


,raw as
(select 
    report_date
    ,city_name
    -- ,shipper_type
    -- ,case when cardinality(map_values(map_filter(agg_delivered_date,(k, v) -> cast(k as date) between report_date and report_date))) > 0 then shipper_id else null end as a1_shipper_id
    ,reduce((map_values(map_filter(agg_delivered_date,(k, v) -> cast(k as date) between report_date and report_date))), 0, (s,x) -> s + x, s -> s) as net_orders
    ,(map_values(map_filter(agg_delivered_date,(k, v) -> cast(k as date) between report_date and report_date))) as check
    -- ,case when cardinality(map_values(map_filter(agg_delivered_date,(k, v) -> cast(k as date) between report_date - interval '30' day and report_date))) > 0 then shipper_id else null end as a30_shipper_id
    ,case when cardinality(filter(map_keys(agg_delivered_date), x -> x between report_date - interval '1' day + interval '1' day and report_date)) > 0 then shipper_id else null end a1_shipper_id
    ,case when cardinality(filter(map_keys(agg_delivered_date), x -> x between report_date - interval '30' day + interval '1' day and report_date)) > 0 then shipper_id else null end a30_shipper_id
from temp_tab
)
-- select * from raw
,final as
(select 
    report_date
    ,city_name
    ,count(distinct a1_shipper_id) as a1
    ,count(distinct a30_shipper_id) as a30
    ,cast(sum(net_orders) as double) / count(distinct a1_shipper_id) as net_ado_per_transacting_driver
from raw
group by 1,2
)
select 
    concat(cast(year(report_date) as varchar),'-',case when month(report_date) < 10 then concat('0', cast(month(report_date) as varchar)) else cast(month(report_date) as varchar) end) year_month
    ,city_name
    ,avg(a1) avg_a1
    ,avg(a30) as avg_a30
    ,avg(net_ado_per_transacting_driver) as avg_net_ado
    ,max(a1) as max_a1
    ,max(a30) as max_30
    ,max(net_ado_per_transacting_driver) as max_net_ado
from final
group by 1,2
;
-- unique transacting driver per month
select 
    concat(cast(year(report_date) as varchar),'-',case when month(report_date) < 10 then concat('0', cast(month(report_date) as varchar)) else cast(month(report_date) as varchar) end) year_month
    ,sm.city_name
    ,count(distinct d.shipper_id) as unique_transacting_drivers
from shopeefood_bnp_adhoc_ticket_506 d
left join shopeefood.foody_mart__profile_shipper_master sm
    on d.report_date = try_cast(sm.grass_date as date) and d.shipper_id = sm.shipper_id
group by 1,2
;

--- onboard_driver_tab
with shopeefood_bnp_onboard_driver_tab_adhoc_506 as
(select
    DATE(FROM_UNIXTIME(si.create_time - 3600)) AS onboard_date 
    , si.uid AS shipper_id
    , sm.city_name
    FROM shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live si
    LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON si.uid = sm.shipper_id AND TRY_CAST(sm.grass_date AS DATE) = DATE(FROM_UNIXTIME(si.create_time - 3600))
    where DATE(FROM_UNIXTIME(si.create_time - 3600)) between date '2021-01-01' and current_date - interval '1' day
)
,raw as
(select 
    onboard_date as report_date
    ,city_name
    ,count(distinct shipper_id) as total_onboard_drivers
from shopeefood_bnp_onboard_driver_tab_adhoc_506
group by 1,2
)
select 
    concat(cast(year(report_date) as varchar),'-',case when month(report_date) < 10 then concat('0', cast(month(report_date) as varchar)) else cast(month(report_date) as varchar) end) year_month
    ,city_name
    ,avg(total_onboard_drivers) as avg_onboard_driver
    ,max(total_onboard_drivers) as max_onboard_driver
from raw
group by 1,2
