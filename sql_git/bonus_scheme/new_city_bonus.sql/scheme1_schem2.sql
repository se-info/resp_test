/*
Scheme 07 đơn đầu tiên (limit 30 tx đầu tiên/city)

https://shopee.vn/m/thuong-nong-50k-BMT?__mobile__=1&hidebar=1


https://shopee.vn/m/bi-quyet-hoat-dong-cho-bac-tai-moi-bmt-th?__mobile__=1&hidebar=1
Scheme thưởng tuần (tính theo mốc đơn hoàn thành/tuần)



Xét trên toàn tệp tx có onboard date trong tháng 3

Scheme 07 đơn đầu tiên (limit 30 tx đầu tiên/city): này là xét 30 mà từ lúc onboard > lúc complete đơn thứ 7 sớm nhất

Scheme thưởng tuần: xét mốc thưởng theo từng tuần nhé
*/
--Scheme1 : 
with raw as 
(select 
        dot.uid as shipper_id 
       ,dot.ref_order_code 
       ,from_unixtime(dot.real_drop_time - 3600) as last_delivered_timestamp
       ,date(from_unixtime(dot.real_drop_time - 3600)) as last_delivered_date       
       ,row_number()over(partition by dot.uid order by from_unixtime(dot.real_drop_time - 3600) asc) as rank   
        
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

WHERE 1 = 1 
AND dot.order_status = 400 
AND date(from_unixtime(dot.real_drop_time - 3600)) between date'2023-03-08' and date'2023-03-31'
)
,summary as 
(select 
        raw.last_delivered_date
       ,raw.shipper_id
       ,ROUND(sla.completed_rate/cast(100 as double),2) as sla_rate
       ,count(distinct ref_order_code) as total_order  

from raw 
left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live sla on sla.uid = raw.shipper_id and date(from_unixtime(sla.report_date - 3600)) = raw.last_delivered_date

group by 1,2,3
order by 1
)
select 
        raw.shipper_id
       ,spp.shopee_uid
       ,sm.shipper_name
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non hub' end as driver_type  
       ,sm.city_name
       ,case when sm.shipper_status_code = 1 then 'Normal' else 'Other' end as working_status 
       ,spp.main_phone
       ,raw.ref_order_code
       ,raw.last_delivered_timestamp
       ,raw.last_delivered_date
    --    ,raw.rank AS order_rank 
       ,row_number()over(order by last_delivered_timestamp asc) as rank_driver
       ,map_agg(cast(s.last_delivered_date as varchar),cast(s.total_order as varchar)) as order_ext 
       ,map_agg(cast(s.last_delivered_date as varchar),s.sla_rate) as sla_ext

from raw 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.shipper_id and try_cast(sm.grass_date as date) = raw.last_delivered_date

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp on spp.uid = raw.shipper_id

left join summary s on s.shipper_id = raw.shipper_id

WHERE 1 = 1 
AND raw.rank = ${order_threshold}
AND sm.city_name IN ('Thanh Hoa')
group by 1,2,3,4,5,6,7,8,9,10

LIMIT ${limit_driver}
;

-- Scheme 2:
WITH shipper_list AS 
(SELECT 
        sm.shipper_id
       ,sm.shipper_name
       ,sm.city_name
       ,DATE(FROM_UNIXTIME(sp.create_time - 3600)) AS onboard_date

FROM shopeefood.foody_mart__profile_shipper_master sm 

LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sp 
    on sp.uid = sm.shipper_id

WHERE 1 = 1
AND sm.city_name in ('Dak Lak','Thanh Hoa')
AND sm.grass_date = 'current'
AND DATE(FROM_UNIXTIME(sp.create_time - 3600)) BETWEEN DATE'2023-02-14' AND DATE'2023-03-31'
AND sm.shipper_status_code = 1
)
,driver_order AS 
(SELECT
         YEAR(DATE(FROM_UNIXTIME(dot.real_drop_time - 60*60)))*100 + WEEK(DATE(FROM_UNIXTIME(dot.real_drop_time - 60*60))) AS created_year_week
        ,dot.uid AS shipper_id 
        ,COUNT(DISTINCT dot.ref_order_code) AS total_order

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot


WHERE 1 = 1 
AND dot.order_status = 400
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 60*60)) BETWEEN DATE'2023-03-13' AND DATE'2023-03-26'
GROUP BY 1,2
)
SELECT 
         do.created_year_week
        ,do.shipper_id
        ,sl.shipper_name
        ,sl.city_name
        ,sl.onboard_date
        ,do.total_order
        ,CASE 
              WHEN do.total_order >= 35 THEN 200000
              WHEN do.total_order >= 20 THEN 100000
              ELSE 0 END AS bonus_value


FROM driver_order do 

INNER JOIN shipper_list sl 
    on do.shipper_id = sl.shipper_id


ORDER BY 1,2            