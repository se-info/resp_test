with data as 
(SELECT 
a.uid 
,date(from_unixtime(a.real_drop_time - 3600)) as date_ 
,'1. Real-time' as period 
,case 
                when sm.shipper_type_id = 12 then '3. Hub'
                else '2. PT-16' end as working_group
,case 
                when a.ref_order_category = 0 then '2. Food'
                else '3. Ship' end as source
,case           
                when a.pick_city_id = 217 then '2. HCM'
                when a.pick_city_id = 218 then '3. HN'
                when a.pick_city_id = 222 then 'Đồng Nai'
                when a.pick_city_id = 220 then 'Hải Phòng'
                when a.pick_city_id = 230 then 'Bình Dương'
                when a.pick_city_id = 257 then 'Nghệ An'
                when a.pick_city_id = 219 then '4. Đà Nẵng'
                when a.pick_city_id = 248 then 'Khánh Hòa'
                when a.pick_city_id = 221 then 'Cần Thơ'
                when a.pick_city_id = 265 then 'Quảng Ninh'
                when a.pick_city_id = 223 then 'Vũng Tàu'
                when a.pick_city_id = 273 then 'Huế'
                when a.pick_city_id = 228 then 'Bắc Ninh'
                when a.pick_city_id = 271 then 'Thái Nguyên' 
                when a.pick_city_id = 254 then 'Lâm Đồng'
                when a.pick_city_id = 263 then 'Quảng Nam'
                end as city_name
,case when a.group_id > 0 then 1 else 0 end as is_group
,a.ref_order_id
,case when sm.shipper_type_id = 11 then -(delivery_cost *1.0000/100) 
      else -13500 end as cost 


from foody_vite.foody_partner_db__driver_order_tab a 
left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and sm.grass_date = 'current'
where 1 = 1  
and sm.shipper_type_id = 11
and order_status = 400 
and date(from_unixtime(a.real_drop_time - 3600)) = current_date
)


,extra as 
(SELECT '1. Real-time' as period 
        ,uid
        ,city_name
        ,case when total_order >= 15 and total_order < 25 and city_name in ('2. HCM','3. HN') and date(current_date) = date('2021-12-12') then 60000
              when total_order >= 25 and total_order < 35 and city_name in ('2. HCM','3. HN') and date(current_date) = date('2021-12-12') then 120000
              when total_order >= 35 and city_name in ('2. HCM','3. HN') and date(current_date) = date('2021-12-12') then 180000
              else 0 end as extra_bonus
from
(SELECT uid
,city_name
,count(ref_order_id) as total_order
from data 
group by 1, 2
) 
group by 1, 2, 3, 4
)


,final  as 
(SELECT 
 a.period 
,'1. All' as city_name 
,'1. Overall Performace' as kpi
,'1. All' as source
,(sum(a.cost)*1.0000/23000)*1.0000/count(a.ref_order_id)  as total_cost
,IF(date(current_date) =date('2021-12-12'),(sum(e.extra_bonus)*1.0000/23000)*1.0000/count(a.ref_order_id),0)  as extra_bonus
,count(distinct a.uid) as total_driver
,count(case when a.is_group = 1 then a.ref_order_id else null end) as total_group_stack
,count(a.ref_order_id) as total_order


from data a 
left join extra e on e.uid = a.uid 
where a.working_group = '2. PT-16'
group by 1,2,3,4

UNION ALL 

SELECT 
 b.period 
,b.city_name 
,'1. Overall Performace' as kpi
,'1. All' as source
,(sum(b.cost)*1.0000/23000)*1.0000/count(b.ref_order_id) as total_cost
,IF(date(current_date) =date('2021-12-12'),(sum(f.extra_bonus)*1.0000/23000)*1.0000/count(b.ref_order_id),0)  as extra_bonus
,count(distinct  b.uid) as total_driver
,count(case when b.is_group = 1 then b.ref_order_id else null end) as total_group_stack
,count(b.ref_order_id) as total_order

from data b
left join extra f on f.uid = b.uid 
where b.working_group = '2. PT-16'
group by 1,2,3,4

UNION ALL 

SELECT 
 c.period 
,c.city_name  
,'1. Overall Performace' as kpi
,c.source
,(sum(c.cost)*1.0000/23000)*1.0000/count(c.ref_order_id) as total_cost
,IF(date(current_date) =date('2021-12-12'),(sum(g.extra_bonus)*1.0000/23000)*1.0000/count(c.ref_order_id),0)  as extra_bonus
,count(distinct  c.uid) as total_driver
,count(case when c.is_group = 1 then c.ref_order_id else null end) as total_group_stack
,count(c.ref_order_id) as total_order

from data c
left join extra g on g.uid = c.uid 
where c.working_group = '2. PT-16'
group by 1,2,3,4

UNION ALL 

SELECT d.period 
      ,d.city_name
      ,'2. Extra Bonus Performace' as kpi
      ,case when d.extra_bonus = 60000 then 'L1. 60k'
            when d.extra_bonus = 120000 then 'L2. 120k'
            when d.extra_bonus = 180000 then 'L3. 180k'
            else '0'
            end as source
      ,0 as total_cost 
      ,sum(d.extra_bonus) as extra_bonus
      ,count(case when d.extra_bonus > 0 then uid else null end) as total_driver
      ,0 as total_group_stack
      ,0 as total_order
      
from extra d 
where d.city_name in ('2. HCM','3. HN')
group by 1,2,3,4,5,8,9
)
SELECT * 

from final 








