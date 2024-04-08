with raw as 
(SELECT   dot.uid
        ,psm.shipper_name
        ,psm.city_name
        ,psm.shipper_type_id 
        ,case
        WHEN dot.pick_city_id = 217 then 'HCM'
        WHEN dot.pick_city_id = 218 then 'HN'
        ELSE NULL end as city_group
        ,date(from_unixtime(dot.real_drop_time -3600)) as report_date
        ,YEAR(date(from_unixtime(dot.real_drop_time -3600)))*100 + WEEK(date(from_unixtime(dot.real_drop_time -3600))) as created_year_week
        ,(slot.end_time - slot.start_time)/3600 as shift_hour

        ,cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) policy
        ,dot.ref_order_code

        
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot on slot.uid = dot.uid and date(from_unixtime(slot.date_ts - 3600)) = date(from_unixtime(dot.real_drop_time - 3600))

LEFT JOIN
(
select *
,Case
when grass_date = 'current' then date(current_date)
else cast(grass_date AS DATE ) END as report_date
from shopeefood.foody_mart__profile_shipper_master
  WHERE grass_region = 'VN'
) psm on psm.shipper_id = dot.uid AND psm.report_date = date(from_unixtime(dot.real_drop_time -3600))
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on dot.id = doet.order_id



WHERE 1=1
--and cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 
AND dot.pick_city_id in (217,218)
AND psm.shipper_type_id in (12)
--and dot.ref_order_category <> 0
and psm.city_id in (217,218)
AND dot.order_status = 400
and date(from_unixtime(dot.real_drop_time -3600)) between date'2022-04-01' and date'2022-04-30'
)


SELECT   uid
        ,shipper_name 
        ,city_name
        ,shift_type
        ,case when inshift_order > 0 and shipper_type = 'Hub' then 'Hub' when shipper_type = 'Hub' and inshift_order = 0 then 'Hub Outshift' else 'Non Hub' end as working_group
        ,count(distinct report_date) as working_day


FROM 
(SELECT 
        uid
       ,report_date 
       ,shipper_name
       ,city_name
       ,concat(cast(shift_hour as varchar),' hour shift') as shift_type
       ,case when shipper_type_id = 12 then 'Hub' else 'Non Hub' end as shipper_type 
       ,count(distinct case when policy = 2 then ref_order_code else null end) as inshift_order
       ,count(distinct ref_order_code) as total_order
       
from raw 
group by 1,2,3,4,5,6
)

where 1 = 1 
--and uid = 2996387
group by 1,2,3,4,5



