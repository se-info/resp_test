SELECT   dot.uid
        ,psm.shipper_name
        ,psm.city_name
        ,case
        WHEN dot.pick_city_id = 217 then 'HCM'
        WHEN dot.pick_city_id = 218 then 'HN'
        ELSE NULL end as city_group
        ,date(from_unixtime(dot.real_drop_time -3600)) as report_date
        ,YEAR(date(from_unixtime(dot.real_drop_time -3600)))*100 + WEEK(date(from_unixtime(dot.real_drop_time -3600))) as created_year_week

        ,cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) policy_
        ,dot.ref_order_code

        
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

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
and cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 
AND dot.pick_city_id in (217,218)
AND psm.shipper_type_id in (12)
and dot.ref_order_category <> 0
and psm.city_id in (217,218)
AND dot.order_status = 405