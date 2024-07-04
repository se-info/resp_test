with raw as 
(SELECT date(from_unixtime(real_drop_time - 3600)) as report_date
      ,extract(hour from from_unixtime(real_drop_time - 3600)) as report_hour 
      ,pick_city_id
      ,pick_district_id
      ,drop_district_id
      ,order_status
      ,delivery_distance*1.0000/1000 as delivery_distance
      ,ref_order_id
      ,case when ref_order_category = 0 then '1. Food'
            else '2. Ship' end as source
FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live
)
,final as
(
SELECT a.report_date 
       ,a.report_hour    
       ,case when a.pick_district_id = a.drop_district_id  and a.delivery_distance <= 2.3 then '1. P & D same District'
             when a.pick_district_id <> a.drop_district_id and a.delivery_distance <= 2.3 then '2. Pick <> Drop District'
             else '3. Other route' end as qualified_group
        ,case when a.pick_city_id = 217 then '2. HCM'
              when a.pick_city_id = 218 then '3. HN'    
              else '4. OTH' end as city_group
        ,a.source      
        ,count(distinct ref_order_id) as total_order
FROM raw a     
where a.order_status = 400
and a.report_date between date('2021-02-04') and date('2021-02-16')
group by 1,2,3,4,5)


SELECT a.* 

from final a 

UNION ALL 

SELECT b.report_date
      ,b.report_hour    
      ,b.qualified_group 
      ,'1. All' as city_group
      ,b.source 
      ,sum(b.total_order) as total_order

from final b 
group by 1,2,3,4,5

