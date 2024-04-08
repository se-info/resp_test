with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(current_date - interval '14' day,current_date) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(
SELECT 
         '1. Realtime'
        ,CAST(report_date as varchar)
        ,report_date
        ,report_date
        ,CAST(1 as double)

from raw_date
where report_date >= current_date 
group by 1,2,3,4,5

UNION 

SELECT 
         '2. Historical'
        ,CAST(report_date as varchar)
        ,report_date
        ,report_date
        ,CAST(1 as double)

from raw_date
where report_date < current_date 
group by 1,2,3,4,5

)

,data_realtime as 
(SELECT date_ 
      ,hour_
      ,city_group
    --   ,hub_type
      ,sum(case when shipper_type_id = 12 then total_stack else null end) as stack_order
      ,sum(total_stack_overall) as total_stack
      ,sum(total_order) as total_order
      ,sum(case when shipper_type_id = 12 then inshift_order else null end) as total_hub_order
      ,count(distinct  uid ) as total_active
      ,count(distinct case when inshift_order > 0 and shipper_type_id = 12 then uid else null end) as hub_a1
      
FROM       
(SELECT      a.uid
        --   ,concat(cast((slot.end_time - slot.start_time)/3600 as varchar),'-','hour shift') as hub_type    
        --   ,sm.shipper_name
          ,sm.shipper_type_id 
        --   ,sm.city_name
          ,date(from_unixtime(real_drop_time - 3600)) as date_
          ,date_format(from_unixtime(real_drop_time - 3600),'%H') as hour_
          ,case when a.pick_city_id = 217 then '2. HCM'
                when a.pick_city_id = 218 then '3. HN'
                else 'OTH' end as city_group
          ,count(case when cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then ref_order_id else null end) inshift_order      
          ,count(ref_order_id ) as total_order
          ,count(case when group_id > 0 AND cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then ref_order_id else null end) as total_stack
          ,count(case when group_id > 0 then ref_order_id else null end) as total_stack_overall
          
    FROM shopeefood.foody_partner_db__driver_order_tab__reg_continuous_s0_live  a 
    
    LEFT JOIN 
    (select 
            *,
            case when grass_date = 'current' then current_date else cast(grass_date as date) end as report_date
    from shopeefood.foody_mart__profile_shipper_master 
    ) sm on sm.shipper_id = a.uid 
         and date(from_unixtime(real_drop_time - 3600)) = sm.report_date
    
    LEFT JOIN shopeefood.foody_partner_db__driver_order_extra_tab__reg_continuous_s0_live doet on a.id = doet.order_id

    -- LEFT JOIN shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot on slot.uid = a.uid 
                                                                            -- and date(from_unixtime(date_ts - 3600)) = date(from_unixtime(real_drop_time - 3600))
    where 1 = 1 

    and date(from_unixtime(real_drop_time - 3600)) between current_date - interval '30' day and current_date 
    
    and a.pick_city_id in (217,218)
    
    and ref_order_category = 0
    
    and order_status = 400
        
    group by 1,2,3,4,5
    )
  
GROUP BY 1,2,3)



select 
       p.period_group
      ,p.period
      ,a.hour_
      ,a.city_group
      ,a.total_order
      ,a.stack_order
      ,a.total_hub_order
      ,a.total_active
      ,a.hub_a1
      ,a.total_stack as stack_overall


from data_realtime a 


inner join params_date p on a.date_ between p.start_date and p.end_date

-- UNION 


-- select 
--        p.period_group
--       ,p.period
--       ,a.hour_
--       ,'All' as city_group
--       ,sum(a.total_order) as total_order
--       ,sum(a.stack_order) as stack_order 
--       ,sum(a.total_hub_order) as total_hub_order
--       ,sum(a.total_active) as total_active
--       ,sum(a.hub_a1) as hub_a1
--       ,sum(a.total_stack) as stack_overall


-- from data_realtime a 


-- inner join params_date p on a.date_ between p.start_date and p.end_date


-- group by 1,2,3,4
