with hub_info as 
(SELECT
    *
FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics
)
,raw as 
(SELECT 
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
,dot.ref_order_id as id 
,dot.ref_order_code
,dot.uid as shipper_id
,dot.is_asap
,dot.ref_order_category
,hi.hub_type_original AS shift_category_name
,hi.daily_bonus
,kpi AS is_qualified_kpi       
,row_number()over(partition by dot.uid,date(from_unixtime(dot.real_drop_time - 3600)),hi.slot_id order by from_unixtime(dot.real_drop_time - 3600) asc) as rank_order 

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

LEFT JOIN (SELECT * FROM dev_vnfdbi_opsndrivers.phong_raw_assignment_test WHERE status in (3,4) ) sa 
    on sa.ref_order_id = dot.ref_order_id
    and sa.order_category = dot.ref_order_category

LEFT JOIN (SELECT * FROM dev_vnfdbi_opsndrivers.phong_raw_assignment_test WHERE status in (3,4) ) sa_filter
    on  sa.ref_order_id = sa_filter.ref_order_id          
    and sa.order_category = sa_filter.order_category 
    and sa.created_timestamp < sa_filter.created_timestamp

LEFT JOIN hub_info hi 
on hi.uid = dot.uid 
and hi.date_ = date(sa.created_timestamp)
and sa.created_timestamp between hi.start_shift_time and hi.end_shift_time
and hi.registered_ = 1               

-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

where 1 = 1 
AND sa_filter.order_id is null
and cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) = 2
and (date(from_unixtime(dot.real_drop_time - 3600)) = date'${ref_date_1}'
or 
date(from_unixtime(dot.real_drop_time - 3600)) = date'${ref_date_2}')    
and dot.order_status = 400
and dot.ref_order_code = '16043-444651893'
-- and dot.ref_order_category = 0
)
select * from raw 
,final_metrics as 
(select 
    date(raw.last_delivered_timestamp) as report_date
,raw.last_delivered_timestamp
,raw.rank_order
,raw.shipper_id
,raw.ref_order_code
,raw.is_qualified_kpi
,raw.shift_category_name
,raw.city_name
--    ,total_bonus as current_bonus

,case /*when shift_category_name = '10 hour shift' and rank_order between 31 and 40 then 8000*/
        when shift_category_name = '10 hour shift' and rank_order > 30 then 6000
        
        when shift_category_name = '8 hour shift' and rank_order between 26 and 30 then 4000
        when shift_category_name = '8 hour shift' and rank_order > 30 then 6000

        when shift_category_name = '5 hour shift' and rank_order between 14 and 24 then 4000
        when shift_category_name = '5 hour shift' and rank_order > 24 then 6000             

        when shift_category_name = '3 hour shift' and rank_order between 7 and 14 then 2000
        when shift_category_name = '3 hour shift' and rank_order > 14 then 3000                             
        
        else 0 end as current_bonus

--    ,case when shift_category_name = '10 hour shift' and rank_order between 31 and 40 then 8000
--          when shift_category_name = '10 hour shift' and rank_order > 40 then 10000
        
--          when shift_category_name = '8 hour shift' and rank_order between 26 and 30 then 6000
--          when shift_category_name = '8 hour shift' and rank_order > 30 then 8000

--          when shift_category_name = '5 hour shift' and rank_order between 14 and 24 then 6000
--          when shift_category_name = '5 hour shift' and rank_order > 24 then 8000             

--          when shift_category_name = '3 hour shift' and rank_order between 7 and 14 then 4000
--          when shift_category_name = '3 hour shift' and rank_order > 14 then 5000                             
        
--          else 0 end as estimate_bonus_v1


,case when shift_category_name = '10 hour shift' and rank_order between 31 and 35 then 6000
        when shift_category_name = '10 hour shift' and rank_order > 35 then 8000
        
        when shift_category_name = '8 hour shift' and rank_order between 26 and 30 then 4000
        when shift_category_name = '8 hour shift' and rank_order > 30 then 8000

        when shift_category_name = '5 hour shift' and rank_order between 14 and 19 then 4000
        when shift_category_name = '5 hour shift' and rank_order > 19 then 7000             

        when shift_category_name = '3 hour shift' and rank_order between 7 and 12 then 2000
        when shift_category_name = '3 hour shift' and rank_order > 12 then 4000                             
        
        else 0 end as estimate_bonus_v2

from raw


-- where shipper_id = 40026477 
)

select * from final_metrics where shift_category_name is null
    report_date
    ,city_name
    ,shift_category_name
    ,count(distinct ref_order_code) as total_orders
    ,count(distinct shipper_id) as a1_drivers
    ,count(distinct case when is_qualified_kpi = 1 then shipper_id else null end) as driver_passed_kpi 
    ,sum(case when is_qualified_kpi = 1 then current_bonus else 0 end) as current_bonus
    --    ,sum(case when is_qualified_kpi = 1 then estimate_bonus_v1 else 0 end) as estimate_bonus_v1
    ,sum(case when is_qualified_kpi = 1 then estimate_bonus_v2 else 0 end) as estimate_bonus_v2
    --    ,count(distinct case when estimate_bonus_v1 > 0 and is_qualified_kpi = 1 then shipper_id else null end) as eligible_bonus_v1
    ,count(distinct case when estimate_bonus_v2 > 0 and is_qualified_kpi = 1 then shipper_id else null end) as eligible_bonus_v2




from final_metrics



group by 1,2,3