-- ado
with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2023-01-01',current_date - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(
-- SELECT 
--         '1. Daily'
--         ,CAST(report_date as varchar)
--         ,CAST(report_date as date)
--         ,CAST(report_date as date)
--         ,CAST(1 as double)

-- from raw_date

-- UNION 

-- SELECT 
--         '2. Weekly'
--         ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
--         ,CAST(date_trunc('week',report_date) as date)
--         ,max(report_date)
--         ,date_diff('day',cast(date_trunc('week',report_date) as date),max(report_date)) + 1

-- from raw_date

-- group by 1,2,3

-- UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
)
,raw_assignment AS 
(SELECT 
        sa.order_id
       ,COALESCE(ogm.ref_order_id,dot.ref_order_id) AS ref_order_id 
       ,COALESCE(ogm.ref_order_code,dot.ref_order_code) AS order_code
       ,COALESCE(ogi.ref_order_category,sa.order_type) AS order_category
       ,sa.status
       ,sa.shipper_uid AS driver_id
       ,experiment_group
       ,FROM_UNIXTIME(sa.create_time - 3600) AS create_time
       ,CASE 
            WHEN sa.assign_type = 1 then '1. Single Assign'
            WHEN sa.assign_type in (2,4) then '2. Multi Assign'
            WHEN sa.assign_type = 3 then '3. Well-Stack Assign'
            WHEN sa.assign_type = 5 then '4. Free Pick'
            WHEN sa.assign_type = 6 then '5. Manual'
            WHEN sa.assign_type in (7,8) then '6. New Stack Assign'
            ELSE NULL END AS assign_type
       ,CASE 
            WHEN sa.order_type = 200 then 'Group'
            ELSE 'Single' END AS order_type
       ,dot.order_status                      

FROM 
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live


        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
) sa 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogi
    on ogi.id = (CASE WHEN sa.order_type = 200 THEN sa.order_id ELSE 0 END)

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogm 
    on ogm.group_id = ogi.id
    and ogm.ref_order_category = ogi.ref_order_category
    and ogm.create_time <= sa.create_time

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_id ELSE sa.order_id END) 
    and dot.ref_order_category = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_category ELSE sa.order_type END)


WHERE 1 = 1
AND DATE(FROM_UNIXTIME(sa.create_time - 3600)) >= date'2023-01-01'
)  
,assignment AS
(SELECT  
         ref_order_id
        ,order_category
        ,max_by(assign_type,create_time) as assign_type
        ,max_by(order_type,create_time) as order_type
        ,max_by(status,create_time) as status
       ,COUNT(CASE WHEN status in (3,4,2,14,15,8,9,17,18) then order_id ELSE NULL END) AS total_assign
       ,COUNT(CASE WHEN status in (8,9,17,18) then order_id ELSE NULL END) as ignore_order
       ,COUNT(CASE WHEN status in (3,4) then order_id ELSE NULL END) as incharge_order
       ,COUNT(CASE WHEN status in (2,14,15) then order_id ELSE NULL END) as deny_order



FROM raw_assignment    

GROUP BY 1,2
)
,final as 
(SELECT 
        date(from_unixtime(dot.real_drop_time - 3600)) AS report_date
       ,dot.ref_order_id
       ,dot.ref_order_code
       ,case 
                when dot.ref_order_category = 0 then 'order_delivery'
                when dot.ref_order_category = 6 then 'now_ship_on_shopee'
                else 'now_ship_off_shopee' end source
       ,CASE 
            WHEN dot.group_id > 0 AND sa.order_type = 'Group' THEN 1
            WHEN dot.group_id > 0 AND sa.order_type != 'Group' THEN 2
            ELSE 0 END AS is_stack_group_order
        ,sa.total_assign
        ,sa.ignore_order as total_ignore
        ,sa.incharge_order as total_incharged
        ,sa.deny_order as total_deny                                




FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day ) dot

LEFT JOIN assignment sa
    on sa.ref_order_id = dot.ref_order_id
    and sa.order_category = dot.ref_order_category



WHERE 1 = 1 
AND dot.order_status = 400 
AND date(from_unixtime(dot.real_drop_time - 3600)) between date'2023-01-01' and current_date - interval '1' day
AND dot.ref_order_category != 0
)
,metrics as 
(SELECT 
         report_date
        ,source 
        ,SUM(total_assign) AS total_assign
        ,SUM(total_assign + total_deny) AS total_acceptance
        ,SUM(total_incharged) AS total_completed
        ,SUM(total_deny) AS total_deny
        ,SUM(total_ignore) AS total_ignore

        ,COUNT(DISTINCT CASE WHEN is_stack_group_order = 1 THEN ref_order_code ELSE NULL END) AS group_ado
        ,COUNT(DISTINCT CASE WHEN is_stack_group_order = 2 THEN ref_order_code ELSE NULL END) AS stack_ado
        ,COUNT(DISTINCT CASE WHEN is_stack_group_order = 0 THEN ref_order_code ELSE NULL END) AS single_ado
 

FROM final 

GROUP BY 1,2
)
select  
         p.period
        ,m.source

        ,SUM(group_ado)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS group_ado
        ,SUM(stack_ado)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS stack_ado
        ,SUM(single_ado)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS single_ado

        -- ,SUM(total_assign)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS total_assign
        -- ,SUM(total_acceptance)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS total_acceptance
        -- ,SUM(total_completed)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS total_completed
        -- ,SUM(total_deny)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS total_deny
        -- ,SUM(total_ignore)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS total_ignore


from metrics m 

inner join params_date p 
    on m.report_date between cast(p.start_date as date) and cast(p.end_date as date)

GROUP BY 1,2
;
-- attemp

with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2023-01-01',current_date - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(
-- SELECT 
--         '1. Daily'
--         ,CAST(report_date as varchar)
--         ,CAST(report_date as date)
--         ,CAST(report_date as date)
--         ,CAST(1 as double)

-- from raw_date

-- UNION 

-- SELECT 
--         '2. Weekly'
--         ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
--         ,CAST(date_trunc('week',report_date) as date)
--         ,max(report_date)
--         ,date_diff('day',cast(date_trunc('week',report_date) as date),max(report_date)) + 1

-- from raw_date

-- group by 1,2,3

-- UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
)
, daily AS (
SELECT order_source
      ,food_service
      ,date_
      ,sum(no_assign) sum_total_assign
      ,sum(no_incharged) sum_total_incharged
      ,sum(no_ignored) sum_total_ignored
      ,sum(no_deny) sum_total_deny
FROM
(
SELECT   ns.order_id, ns.order_type, ns.order_category, order_group_type
        ,ns.order_source
        ,ns.city_name
        ,ns.city_group
        ,ns.food_service
        ,min(ns.date_) date_
        ,count(ns.order_id) no_assign
        ,count(case when status in (3,4) then order_id else null end) no_incharged
        ,count(case when status in (8,9,17,18) then order_id else null end) no_ignored
        ,count(case when status in (2,14,15) then order_id else null end) no_deny
FROM
(
SELECT   ns.order_id
        ,ns.order_type
        ,ns.status
        ,case when ns.order_type = 0 then '1. Food/Market'
                -- when ns.order_type = 4 then '2. NowShip Instant'
                -- when ns.order_type = 5 then '3. NowShip Food Mex'
                when ns.order_type = 6 then '2. NowShip On Shopee'
                when ns.order_type in (4,5,7,8) THEN '3. NowShip Off Shopee'
                -- when ns.order_type = 7 then '5. NowShip Same Day'
                -- when ns.order_type = 8 then '6. NowShip Multi Drop'
                when ns.order_type = 200 and ogi.ref_order_category = 0 then '1. Food/Market'
                when ns.order_type = 200 and ogi.ref_order_category = 6 then '2. NowShip On Shopee'
                when ns.order_type = 200 and ogi.ref_order_category = 7 then '3. NowShip Off Shopee'
                -- when ns.order_type = 200 and ogi.ref_order_category = 7 then '5. NowShip Same Day'
                else 'Others' end as order_source
        ,case when ns.order_type <> 200 then ns.order_type else ogi.ref_order_category end as order_category
        ,case when ns.order_type = 200 then '1. Group Order'
              when coalesce(dot.group_id,0) > 0 then '2. Stack Order' else '3. Single Order' end as order_group_type
        ,ns.city_id
        ,city.name_en as city_name
        ,case when ns.city_id  = 217 then 'HCM'
            when ns.city_id  = 218 then 'HN'
            when ns.city_id  = 219 then 'DN' else 'OTH'
            end as city_group
        ,from_unixtime(ns.create_time - 3600) as create_time
        ,from_unixtime(ns.update_time - 3600) as update_time
        ,date(from_unixtime(ns.create_time - 3600)) as date_
        ,case when ns.order_type = 200 and ogi.ref_order_category = 0 then coalesce(g.food_service,'NA')
              when ns.order_type = 0 then coalesce(s.food_service,'NA')
              else 'NowShip' end as food_service
FROM
        ( SELECT order_id, order_type , create_time , assign_type, update_time, status, city_id

         from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
         where 1=1
         and status in (3,4,8,9,2,14,15,17,18)

         UNION ALL

         SELECT order_id, order_type, create_time , assign_type, update_time, status, city_id

         from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
         where 1=1
         and status in (3,4,8,9,2,14,15,17,18)
         )ns
LEFT JOIN (select id, ref_order_category from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) group by 1,2) ogi on ogi.id > 0 and ogi.id = case when ns.order_type = 200 then ns.order_id else 0 end

LEFT JOIN
            (SELECT ogm.group_id
                   ,ogi.group_code
                   ,count (distinct ogm.ref_order_id) as total_order_in_group
             FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm
             LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
             WHERE 1=1
             and ogm.group_id is not null
             GROUP BY 1,2
             )order_rank on order_rank.group_id = case when ns.order_type = 200 then ns.order_id else 0 end
-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = ns.city_id and city.country_id = 86

left join
            (select ref_order_id, ref_order_category, group_id
             from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day)
            --  where grass_schema = 'foody_partner_db' 
             group by 1,2,3
             ) dot on dot.ref_order_id = ns.order_id and (ns.order_type <> 200 and ns.order_type = dot.ref_order_category)

left join
            (select dot.ref_order_id, dot.ref_order_category
                   ,case when go.now_service_category_id = 1 then 'Food'
                         when go.now_service_category_id > 0 then 'Fresh/Market'
                         else 'Others' end as food_service
             from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
             left join (select id, now_service_category_id from shopeefood.foody_mart__fact_gross_order_join_detail where grass_region = 'VN' GROUP BY 1,2) go on go.id = dot.ref_order_id and dot.ref_order_category = 0
             where 1=1
             and dot.ref_order_category = 0
             and go.now_service_category_id >= 0
             group by 1,2,3
             ) s on s.ref_order_id = ns.order_id and (ns.order_type = 0 and ns.order_type = dot.ref_order_category)

left join
            (select ogm.group_id, ogm.ref_order_category--,go.now_service_category_id, ogm.ref_order_id
                   ,case when go.now_service_category_id = 1 then 'Food'
                         when go.now_service_category_id > 0 then 'Fresh/Market'
                         else 'Others' end as food_service
             from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm
             left join (select id, now_service_category_id from shopeefood.foody_mart__fact_gross_order_join_detail where grass_region = 'VN' GROUP BY 1,2) go on go.id = ogm.ref_order_id and ogm.ref_order_category = 0
             where 1=1
             and ogm.ref_order_category = 0
             and coalesce(ogm.group_id,0) > 0
             and go.now_service_category_id >= 0
             group by 1,2,3
             ) g on g.group_id = ns.order_id and ns.order_type = 200 and (case when ns.order_type <> 200 then ns.order_type else ogi.ref_order_category end  = 0)


WHERE 1=1
and date(from_unixtime(ns.create_time - 3600)) between date'2023-01-01' and current_date - interval '1' day
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)ns
WHERE 1=1
GROUP BY 1,2,3,3,4,5,6,7,8
)base
GROUP BY 1,2,3
)

SELECT
     p.period
    ,p.days AS days
    ,order_source
    -- , SUM(IF(order_source = '1. Food/Market', sum_total_assign, 0)) / p.days AS nowfood_assign
    -- , SUM(IF(order_source = '1. Food/Market', coalesce(sum_total_incharged, 0) + coalesce(sum_total_deny, 0), 0)) / p.days AS _nowfood_acceptance
    -- , SUM(IF(order_source = '1. Food/Market', sum_total_incharged, 0)) / p.days AS _nowfood_completed
    -- , SUM(IF(order_source = '1. Food/Market', sum_total_deny, 0)) / p.days AS _nowfood_denied
    -- , SUM(IF(order_source = '1. Food/Market', sum_total_ignored, 0)) / p.days AS _nowfood_ignored
    , SUM(IF(order_source != '1. Food/Market', sum_total_assign, 0)) / p.days AS nowship_assign
    , SUM(IF(order_source != '1. Food/Market', coalesce(sum_total_incharged, 0) + coalesce(sum_total_deny, 0), 0)) / p.days AS _nowship_acceptance
    , SUM(IF(order_source != '1. Food/Market', sum_total_incharged, 0)) / p.days AS _nowship_completed
    , SUM(IF(order_source != '1. Food/Market', sum_total_deny, 0)) / p.days AS _nowship_denied
    , SUM(IF(order_source != '1. Food/Market', sum_total_ignored, 0)) / p.days AS _nowship_ignored
FROM daily d
INNER JOIN params_date p ON d.date_ BETWEEN p.start_date AND p.end_date
WHERE order_source != '1. Food/Market'
GROUP BY 1,2,3