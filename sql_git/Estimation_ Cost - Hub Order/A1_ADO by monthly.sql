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
,params_date(period_group,period,start_date,end_date,days) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,CAST(report_date as date)
        ,CAST(report_date as date)
        ,CAST(1 as double)

from raw_date

UNION 

SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,CAST(date_trunc('week',report_date) as date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('week',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
)
,raw as 
(SELECT 
base.created_date as inflow_date
,base.shipper_id
,base.city_tier
,base.city_name
,case when sm.shipper_type_id = 12 then 1 else 0 end as is_hub_driver
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
,count(distinct case when is_inshift_order = 1 and ref_order_category = 0 then ref_order_code else null end) as inshift_orders_food
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
       ,case when oct.foody_service_id = 1 then 'Food' when oct.foody_service_id != 1 and oct.id is not null then 'Market' else 'SPXI' end as service 
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

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = base.shipper_id and try_cast(sm.grass_date as date) = base.created_date                                                                                              

where 1 = 1 
and created_date >= date'2021-12-01'
group by 1,2,3,4,5
)

select 
       p.period_group
      ,p.period
      ,p.start_date
      ,p.end_date
      ,'VN' as city
    --   ,a.city_tier
    --   ,a.city_name
      ,sum(delivered_orders_all)/cast(p.days as double) as delivered_orders
      ,sum(delivered_orders_food)/cast(p.days as double) as delivered_orders_food
      ,sum(delivered_orders_fresh)/cast(p.days as double) as delivered_orders_fresh
      ,sum(delivered_orders_spxi)/cast(p.days as double) as delivered_orders_spxi
      ,sum(hub_orders_food)/cast(p.days as double) as hub_orders_food 
      ,sum(inshift_orders_food)/cast(p.days as double) as inshift_orders_food 
      ,count(shipper_id)/cast(p.days as double) as all_a1 
      ,count(case when is_hub_driver = 1 and inshift_orders_food > 0 then shipper_id else null end)/cast(p.days as double) as hub_a1 


from raw a 

inner join params_date p on a.inflow_date between cast(p.start_date as date) and cast(p.end_date as date)

where 1 = 1 


group by 1,2,3,4,5,p.days


