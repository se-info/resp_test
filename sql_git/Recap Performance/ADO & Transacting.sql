with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2021-12-01',current_date - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,report_date
        ,report_date

from raw_date

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,date_trunc('week',report_date) 
        ,max(report_date)

from raw_date

group by 1,2,3

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)

from raw_date

group by 1,2,3
)
,raw as 
(SELECT 
base.created_date as inflow_date
,base.shipper_id
,base.city_tier
,base.city_name
,rp.total_online_seconds/cast(3600 as double) as online_time 
,rp.total_work_seconds/cast(3600 as double) as working_time 
,case when tier.current_driver_tier = 'Hub' and count(distinct case when is_inshift_order = 1 then ref_order_code else null end) = 0 then 'T1'
            else coalesce(tier.current_driver_tier,'Others') end as tier_  
-- ,case when sm.shipper_type_id = 12 and sm.city_id != 220 and count(distinct case when is_inshift_order = 1 then ref_order_code else null end) = 0 then 'T1'
--       when sm.shipper_type_id = 12 and sm.city_id = 220 and count(distinct case when is_inshift_order = 1 then ref_order_code else null end) = 0 then 'Others'
--       when sm.shipper_type_id = 12 and count(distinct case when is_inshift_order = 1 then ref_order_code else null end) > 0 then 'Hub'  
--       else coalesce(tier.current_driver_tier,'Others') end as tier_  
---
,count(distinct ref_order_code) as delivered_orders_all
,count(distinct case when ref_order_category = 0 and service = 'Food' then ref_order_code else null end) as delivered_orders_food
,count(distinct case when ref_order_category = 0 and service != 'Food' then ref_order_code else null end) as delivered_orders_fresh
,count(distinct case when ref_order_category != 0 then ref_order_code else null end) as delivered_orders_spxi
---
,count(distinct case when is_hub_qualified = 1 then ref_order_code else null end) as hub_orders_all
,count(distinct case when is_hub_qualified = 1 and ref_order_category = 0 then ref_order_code else null end) as hub_orders_food
,count(distinct case when is_hub_qualified = 1 and ref_order_category != 0 then ref_order_code else null end) as hub_orders_spxi
--
,count(distinct case when is_inshift_order = 1 then ref_order_code else null end) as inshift_orders_all
,count(distinct case when is_inshift_order = 1 and ref_order_category = 0 and service = 'Food' then ref_order_code else null end) as inshift_orders_food
,count(distinct case when is_inshift_order = 1 and ref_order_category = 0 and service != 'Food' then ref_order_code else null end) as inshift_orders_fresh
,count(distinct case when is_inshift_order = 1 and ref_order_category != 0 then ref_order_code else null end) as inshift_orders_spxi



from
(
SELECT 
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
       ,case when dot.ref_order_category = 0 and oct.id is not null and oct.foody_service_id = 1 then 'Food' 
             when dot.ref_order_category = 0 and oct.id is not null and oct.foody_service_id != 1 then 'Market' 
             else 'SPXI' end as service 
       ,CASE WHEN cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_inshift_order
       ,case when cast(json_extract(dotet.order_data,'$.hub_id') as bigint) > 0 then 1 else 0 end as is_hub_qualified 
    --    ,dot.ref_order_category


from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

-- foody service
left join shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live oct on oct.id = dot.ref_order_id and dot.ref_order_category = 0


where 1 = 1 
-- and dot.ref_order_category = 0
and dot.order_status = 400
)base

left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot on slot.uid = base.shipper_id 
                                                                                             and date(from_unixtime(slot.date_ts - 3600)) = base.created_date

---tier 
LEFT JOIN
(SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
,bonus.uid as shipper_id
,case when hub.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else null end as current_driver_tier
,bonus.total_point
,bonus.daily_point

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

LEFT JOIN
(SELECT shipper_id
,shipper_type_id
,case when grass_date = 'current' then date(current_date)
else cast(grass_date as date) end as report_date

from shopeefood.foody_mart__profile_shipper_master

where 1=1
and (grass_date = 'current' OR cast(grass_date as date) >= date('2019-01-01'))
GROUP BY 1,2,3
)hub on hub.shipper_id = bonus.uid and hub.report_date = cast(from_unixtime(bonus.report_date - 60*60) as date)

where cast(from_unixtime(bonus.report_date - 60*60) as date) >=  date'2021-12-01'
)tier on tier.shipper_id = base.shipper_id and base.created_date = tier.report_date 

--Performance
LEFT JOIN  shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp on rp.uid = base.shipper_id and date(from_unixtime(rp.report_date - 3600)) = base.created_date

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = base.shipper_id and try_cast(sm.grass_date as date) = base.created_date

where 1 = 1 
and created_date >= date'2021-12-01'
group by 1,2,3,4,5,6,tier.current_driver_tier,sm.shipper_type_id,sm.city_id
)

,ado as 
(select 
       p.period_group
      ,p.period
      ,p.start_date
      ,p.end_date
      ,case when a.tier_ = 'Hub' and inshift_orders_food = 0 then 'T1'
            else coalesce(a.tier_,'Others') end as tier_  
    --   ,a.city_tier
    --   ,a.city_name
      ,sum(delivered_orders_all)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as delivered_orders_all
      ,sum(delivered_orders_food)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as delivered_orders_food
      ,sum(delivered_orders_fresh)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as delivered_orders_fresh
      ,sum(delivered_orders_spxi)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as delivered_orders_spxi
    --   ,sum(hub_orders_food)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as hub_orders_food 
      ,sum(inshift_orders_food)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as inshift_orders_food 
      ,count(shipper_id)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as active_drivers
      ,sum(online_time)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as online_time_active 
      ,sum(working_time)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as working_time 
      ,date_diff('day',p.start_date,p.end_date) +1 as days

from raw a 

inner join params_date p on a.inflow_date between cast(p.start_date as date) and cast(p.end_date as date)

where p.period_group = '3. Monthly'

group by 1,2,3,4,5,date_diff('day',p.start_date,p.end_date)
)

,online as 
(select  p.period_group
        ,period
        ,count(uid)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as total_online_driver 
        ,sum(total_online_seconds/cast(3600 as double))/cast((date_diff('day',p.start_date,p.end_date) +1) as double) as total_online_time

FROM shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp 


inner join params_date p on date(from_unixtime(rp.report_date - 3600)) between p.start_date and p.end_date

where p.period_group = '3. Monthly'
group by 1,2,date_diff('day',p.start_date,p.end_date)
)

,assign_raw as 
(select 
        date_ 
       ,sum(case when metrics = 'Assign' then value else null end) as total_assign 
       ,sum(case when metrics = 'Incharged' then value else null end) as total_incharged 
       ,sum(case when metrics = 'Ignore' then value else null end) as total_ignored 
       ,sum(case when metrics = 'Denied' then value else null end) as total_denied 

FROM 
(SELECT 
        date(from_unixtime(sa.create_time - 3600)) as date_
       ,case when sa.status in (3,4) then 'Incharged' when sa.status in (8,9) then 'Ignore' end as metrics
       ,count(order_id) as value

    --    ,count(case when sa.status in (3,4) then order_id else null end) as total_incharged 
    --    ,count(case when sa.status in (8,9) then order_id else null end) as total_ignore

FROM
    (SELECT
        CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
        , order_id, city_id, assign_type, update_time, create_time, status, order_type
        , experiment_group, shipper_uid AS shipper_id

        FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        WHERE status IN (3,4,8,9) -- shipper incharge + ignore
 UNION ALL

    SELECT
        CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
        , order_id, city_id, assign_type, update_time, create_time, status, order_type
        , experiment_group, shipper_uid AS shipper_id

        FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
        WHERE status IN (3,4,8,9) -- shipper incharge + ignore
    ) sa
group by 1,2

UNION ALL 

SELECT 
        date(from_unixtime(dod.create_time - 3600)) as date_ 
        ,'Denied' as metrics
        ,count(order_id) as value

FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod 
group by 1,2

UNION ALL 

select 
        date(from_unixtime(sa.create_time - 3600)) as date_
       ,'Assign' as metrics 
       ,count(order_id) as value

FROM
    (SELECT
        CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
        , order_id, city_id, assign_type, update_time, create_time, status, order_type
        , experiment_group, shipper_uid AS shipper_id

        FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
 UNION ALL

    SELECT
        CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
        , order_id, city_id, assign_type, update_time, create_time, status, order_type
        , experiment_group, shipper_uid AS shipper_id

        FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
    ) sa
group by 1,2
)
group by 1    
)
,assign_final as 
(SELECT
        p.period_group
       ,p.period 
       ,sum(total_assign)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) total_assigned
       ,sum(total_incharged)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) total_incharged
       ,sum(total_ignored)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) total_ignored
       ,sum(total_denied)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) total_denied

from assign_raw sa 

inner join params_date p on sa.date_ between p.start_date and p.end_date

where p.period_group = '3. Monthly'

GROUP BY 1,2,date_diff('day',p.start_date,p.end_date)
)
select 
        a.* 
---
       ,sum(total_online_driver) as total_online_driver
       ,sum(b.total_online_time) as total_online_time
       ,sum(b.total_online_time)/cast(sum(b.total_online_driver) as double) as avg_online_time
---
       ,sum(c.total_assigned) as total_assigned
       ,sum(c.total_incharged) as total_incharged 
       ,sum(c.total_ignored) as total_ignore
       ,sum(c.total_denied) as total_denied 
from 
(select        
        a.period_group
       ,a.period
       ,sum(a.delivered_orders_all) as total_delivered_all
       ,sum(a.delivered_orders_food) as total_delivered_food
       ,sum(a.delivered_orders_fresh) as total_delivered_fresh
       ,sum(a.delivered_orders_spxi) as total_delivered_spxi
       ,sum(a.active_drivers) as all_transacting
       ,sum(case when tier_ = 'Hub' then a.active_drivers else null end) as hub_transacting
       ,sum(case when tier_ = 'T1' then a.active_drivers else null end) as t1_transacting 
       ,sum(case when tier_ = 'T2' then a.active_drivers else null end) as t2_transacting 
       ,sum(case when tier_ = 'T3' then a.active_drivers else null end) as t3_transacting 
       ,sum(case when tier_ = 'T4' then a.active_drivers else null end) as t4_transacting                      
       ,sum(case when tier_ = 'T5' then a.active_drivers else null end) as t5_transacting                      
       ,sum(case when tier_ = 'Others' then a.active_drivers else null end) as oth_transacting                      
---
       ,sum(case when tier_ = 'Hub' then a.delivered_orders_all else null end) as hub_ado
       ,sum(case when tier_ = 'T1' then a.delivered_orders_all else null end) as t1_ado 
       ,sum(case when tier_ = 'T2' then a.delivered_orders_all else null end) as t2_ado 
       ,sum(case when tier_ = 'T3' then a.delivered_orders_all else null end) as t3_ado 
       ,sum(case when tier_ = 'T4' then a.delivered_orders_all else null end) as t4_ado                      
       ,sum(case when tier_ = 'T5' then a.delivered_orders_all else null end) as t5_ado                      
       ,sum(case when tier_ = 'Others' then delivered_orders_all else null end) as oth_ado       
---
       ,sum(case when tier_ = 'Hub' then a.delivered_orders_food else null end)/cast(sum(a.delivered_orders_food) as double) as hub_coverage 
       ,sum(a.working_time) as total_working_time
       ,sum(a.working_time)/cast(sum(a.active_drivers) as double) as avg_working_time       

       
from ado a 



group by 1,2) a 

left join online b on b.period = a.period       

left join assign_final c on c.period = a.period

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
