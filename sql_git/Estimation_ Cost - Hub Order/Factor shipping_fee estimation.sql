with rank_group as 
(select 
        dot.group_id
       ,ref_order_category 
       ,count(distinct ref_order_code) as total_order_in_group 

from     
(select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day ) dot
where dot.order_status = 400
and dot.group_id > 0
and date(from_unixtime(dot.real_drop_time - 3600)) between current_date - interval '60' day and current_date - interval '1' day
group by 1,2
)
,raw as
    (SELECT      dot.uid as shipper_id
                ,sm.city_name
                ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as shipper_type
                ,dot.ref_order_id as order_id
                ,dot.group_id
                --   ,pick_latitude,pick_longitude,drop_latitude,drop_longitude
                ,dot.ref_order_code as order_code
                ,dot.ref_order_category
                ,case when dot.ref_order_category = 0 then 'order_delivery'
                        when dot.ref_order_category = 3 then 'now_moto'
                        when dot.ref_order_category = 4 then 'now_ship'
                        when dot.ref_order_category = 5 then 'now_ship'
                        when dot.ref_order_category = 6 then 'now_ship_shopee'
                        when dot.ref_order_category = 7 then 'now_ship_sameday'
                        else null end source
                ,dot.ref_order_status
                ,dot.order_status
                ,case when dot.order_status = 1 then 'Pending'
                        when dot.order_status in (100,101,102) then 'Assigning'
                        when dot.order_status in (200,201,202,203,204) then 'Processing'
                        when dot.order_status in (300,301) then 'Error'
                        when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
                        else null end as order_status_group
                ,dot.is_asap
                ,date(from_unixtime(dot.real_drop_time - 3600)) as report_date                     
                ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
                ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
                ,case when dot.pick_city_id = 217 then 'HCM'
                        when dot.pick_city_id = 218 then 'HN'
                        when dot.pick_city_id = 219 then 'DN'
                        ELSE 'OTH' end as city_group
                ,CASE WHEN COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) > 0 then 1 else 0 end as is_hub_qualified
                ,dot.delivery_distance/cast(1000 as double) as distance_
                ,dot.delivery_cost/cast(100 as double) as delivery_cost
                ,dotet.policy_driver
                ,row_number()over(partition by dot.group_id order by real_drop_time asc) as rank                        
                -- ,case when dot.group_id > 0 then row_number()over(partition by dot.group_id) else 1 end as rank
                ,case when dot.group_id > 0 then rg.total_order_in_group else 1 end as total_order_in_group

                        

            FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
            
            LEFT JOIN rank_group rg on rg.group_id = dot.group_id and rg.ref_order_category = dot.ref_order_category

            LEFT JOIN (select *,cast(json_extract(order_data,'$.shipper_policy.type') as bigint ) as policy_driver
                        from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da 
                        where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
            -- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live info on info.id = COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0)
            LEFT JOIN  shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(from_unixtime(dot.real_drop_time - 60*60))

            where 1 = 1 
            and dot.order_status = 400
            -- and dot.pick_city_id = 217        
            and date(from_unixtime(dot.real_drop_time - 3600)) between current_date - interval '60' day and current_date - interval '1' day
) 
,hub_metrics as 
(select 
    date(from_unixtime(hub.report_date - 3600)) as report_date
    ,uid as shipper_id
    ,cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) as shift_category_name
    ,cast(json_extract(hub.extra_data,'$.total_order') as bigint) as total_order_inshift
    ,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
    else 0 end as extra_ship
    ,cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) as is_apply_fixed_amount -- check driver has order <= threshold and pass all kpi >> dieu kien de duoc bu min
    ,cast(json_extract(hub.extra_data,'$.total_income') as bigint) as total_income
    ,cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint) as calculated_shipping_shared
    -- ,hub.extra_data
    ,cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) as city_id
    ,case 
    	when cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) not in (217,218,220) then 999 
    	else cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) end as dummy_city_id
    ,cast(json_extract(hub.extra_data,'$.hub_ids') as array<int>) as hub_id
    ,cast(json_extract(hub.extra_data,'$.total_bonus') as bigint) as total_bonus
        , CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) AS hub_shift
    , CAST(json_extract(hub.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hub.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count
    , CAST(json_extract(hub.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 AS online_in_shift
    , CAST(json_extract(hub.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour
    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) start_shift
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) end_shift
    ,date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.00 as time_in_shift
    ,case 
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '10 hour shift' then 2
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '8 hour shift' then 2
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift' 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) > 6 then 1
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift' then 0             
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) > 6 
             AND hour(from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)) < 20 then 1
        WHEN CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) = '3 hour shift' then 0
        else null end as kpi_peak_hour


from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
)
,final_metrics as 
(select 
        raw.report_date
       ,raw.order_id 
       ,raw.order_code
       ,raw.group_id
       ,raw.ref_order_category 
       ,raw.policy_driver
       ,raw.rank
       ,raw.distance_
       ,raw.shipper_id
       ,raw.city_name
       ,raw.shipper_type
       ,ogi.distance as group_distance
       ,case when raw.policy_driver = 2 and raw.group_id = 0 then 13500
             when raw.policy_driver = 2 and raw.group_id > 0 and rank = 1 then 13500 * total_order_in_group
             when raw.policy_driver = 2 and raw.group_id > 0 and rank != 1 then 0 
             when raw.policy_driver != 2 and raw.group_id = 0 then raw.delivery_cost
             when raw.policy_driver != 2 and raw.group_id > 0 then coalesce(ogi.group_fee,0)
             else 0 end as shipping_fee_current

        ,case 
                    -- Hub 
            when raw.distance_ <= 2 and raw.policy_driver = 2 and raw.ref_order_category = 0 then 12500
            when raw.distance_ > 2 and raw.policy_driver = 2  and raw.ref_order_category = 0 then 13500
                    
                    -- Group/Stack order -- Non Hub                                    
            when raw.group_id > 0 and raw.policy_driver != 2 and rank = 1 and  coalesce(ogi.distance,0) <= 2 and ogi.ref_order_category = 0
                 and coalesce(ogi.group_fee,0) - (1000 * total_order_in_group) <= 13500 
                 then 12500 + (total_order_in_group * 1000)

            when raw.group_id > 0 and raw.policy_driver != 2 and rank = 1 and  coalesce(ogi.distance,0) <= 2 and ogi.ref_order_category = 0 
                 and coalesce(ogi.group_fee,0) - (1000 * total_order_in_group) > 13500 
                 then (((coalesce(ogi.group_fee,0) - (1000 * total_order_in_group))/cast(13500 as double)) * 12500) + (total_order_in_group * 1000)  

            when raw.group_id > 0 and raw.policy_driver != 2 and rank = 1 and  coalesce(ogi.distance,0) > 2 and ogi.ref_order_category = 0 then coalesce(ogi.group_fee,0)   

            when raw.group_id > 0 and raw.policy_driver != 2 and rank != 1 /*and  coalesce(ogi.distance,0) <= 2 and ogi.ref_order_category = 0*/ then 0    
            
            when raw.distance_ <= 2 and raw.policy_driver != 2 and raw.group_id = 0 and raw.ref_order_category = 0 and raw.delivery_cost <= 13500 then 12500
            when raw.distance_ <= 2 and raw.policy_driver != 2 and raw.group_id = 0 and raw.ref_order_category = 0 and raw.delivery_cost > 13500    
                 then (raw.delivery_cost/cast(13500 as double))*12500
            when raw.ref_order_category != 0 and group_id > 0 then coalesce(ogi.group_fee,0)
            else raw.delivery_cost end as shipping_fee_estimate_delivery_only
        --All service 
        ,case 
                    -- Hub 
            when raw.distance_ <= 2 and raw.policy_driver = 2  then 12500
            when raw.distance_ > 2 and raw.policy_driver = 2   then 13500
                    
                    -- Group/Stack order -- Non Hub                                    
            when raw.group_id > 0 and raw.policy_driver != 2 and rank = 1 and  coalesce(ogi.distance,0) <= 2
                 and coalesce(ogi.group_fee,0) - (1000 * total_order_in_group) <= 13500 
                 then 12500 + (total_order_in_group * 1000)

            when raw.group_id > 0 and raw.policy_driver != 2 and rank = 1 and  coalesce(ogi.distance,0) <= 2 
                 and coalesce(ogi.group_fee,0) - (1000 * total_order_in_group) > 13500 
                 then (((coalesce(ogi.group_fee,0) - (1000 * total_order_in_group))/cast(13500 as double)) * 12500) + (total_order_in_group * 1000)  

            when raw.group_id > 0 and raw.policy_driver != 2 and rank = 1 and  coalesce(ogi.distance,0) > 2 then coalesce(ogi.group_fee,0)   

            when raw.group_id > 0 and raw.policy_driver != 2 and rank != 1 /*and  coalesce(ogi.distance,0) <= 2 and ogi.ref_order_category = 0*/ then 0    
            
            when raw.distance_ <= 2 and raw.policy_driver != 2 and raw.group_id = 0 and raw.delivery_cost <= 13500 then 12500
            when raw.distance_ <= 2 and raw.policy_driver != 2 and raw.group_id = 0 and raw.delivery_cost > 13500    
                 then (raw.delivery_cost/cast(13500 as double))*12500
            else raw.delivery_cost end as shipping_fee_estimate_all_service                        
        ,total_order_in_group                    

from raw 

LEFT JOIN (select 
                    id
                    ,ref_order_category
                    ,distance/cast(100000 as double) as distance
                    -- ,distance
                    ,ship_fee/cast(100 as double) as group_fee 

            from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day)
            where 1 = 1
            ) ogi on ogi.id = (case when raw.group_id > 0 then raw.group_id else null end) and ogi.ref_order_category = raw.ref_order_category and raw.rank = 1   

where 1 = 1 
-- and raw.report_date = current_date - interval '1' day 
)
-- select * from final_metrics where shipper_id = 15532793
,summary as 
(select 
        fm.report_date 
       ,fm.shipper_id
       ,case when fm.city_name in ('HCM City','Ha Noi City','Da Nang City') then fm.city_name else 'Others' end as city_group
       ,coalesce(tier.current_driver_tier,fm.shipper_type) as tier_type
    --    ,map_agg(order_code,distance_) as ext_info
       ,hm.extra_ship as current_extra_ship  
       ,case when hm.shipper_id is not null and hm.extra_ship > 0 then 
                  (case when hm.hub_shift = '10 hour shift' then 13500*30
                        when hm.hub_shift = '8 hour shift' then 13500*25
                        else 0 end) - sum(fm.shipping_fee_estimate_delivery_only) else 0 end as extra_ship_opt1
       ,case when hm.shipper_id is not null and hm.extra_ship > 0 then 
                  (case when hm.hub_shift = '10 hour shift' then 13500*30
                        when hm.hub_shift = '8 hour shift' then 13500*25
                        else 0 end) - sum(fm.shipping_fee_estimate_all_service) else 0 end as extra_ship_opt2         
       ,count(distinct fm.order_code) as total_order 
       ,sum(fm.shipping_fee_current) as total_ship_fee 
       ,sum(fm.shipping_fee_estimate_delivery_only) as shipping_fee_estimate_delivery_only_opt1  
       ,sum(fm.shipping_fee_estimate_all_service) as shipping_fee_estimate_all_service_opt2
-- 
       ,1 - (sum(fm.shipping_fee_estimate_delivery_only)/cast(sum(fm.shipping_fee_current) as double)) as ship_fee_gap_opt1  
       ,1 - (sum(fm.shipping_fee_estimate_all_service)/cast(sum(fm.shipping_fee_current) as double)) as ship_fee_gap_opt2
-- 
                           

from final_metrics fm 

LEFT JOIN hub_metrics hm on hm.shipper_id = fm.shipper_id and hm.report_date = fm.report_date

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
,bonus.bonus_value/cast(100 as double)

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
)tier on fm.shipper_id = tier.shipper_id and fm.report_date = tier.report_date

group by 1,2,3,4,5,hm.shipper_id,hm.hub_shift
)
select 
-- * from summary where tier_type = 'Hub'
        report_date
       ,city_group
       ,tier_type 
       ,case when ship_fee_gap_opt1 <= 0.05 then '1. 0 - 5%'
             when ship_fee_gap_opt1 <= 0.1 then '2. 5 - 10%'
             when ship_fee_gap_opt1 <= 0.3 then '3. 10 - 30%'
             when ship_fee_gap_opt1 <= 0.5 then '4. 30 - 50%' 
             when ship_fee_gap_opt1 <= 0.7 then '5. 50 - 70%' 
             when ship_fee_gap_opt1 > 0.7 then '6. >70%' end as gap_opt1_range
-- 
       ,case when ship_fee_gap_opt2 <= 0.05 then '1. 0 - 5%'
             when ship_fee_gap_opt2 <= 0.1 then '2. 5 - 10%'
             when ship_fee_gap_opt2 <= 0.3 then '3. 10 - 30%'
             when ship_fee_gap_opt2 <= 0.5 then '4. 30 - 50%' 
             when ship_fee_gap_opt2 <= 0.7 then '5. 50 - 70%' 
             when ship_fee_gap_opt2 > 0.7 then '6. >70%' end as gap_opt2_range
        --
        ,count(distinct shipper_id) as total_drivers
        ,sum(total_ship_fee)/cast(count(distinct shipper_id) as double) as avg_current_shipping_fee                          
        ,sum(shipping_fee_estimate_delivery_only_opt1)/cast(count(distinct shipper_id) as double) as avg_opt1_shipping_fee_delivery_only
        ,sum(shipping_fee_estimate_all_service_opt2)/cast(count(distinct shipper_id) as double) as avg_opt2_shipping_fee_all_service
        --
        ,sum(current_extra_ship)/cast(count(distinct case when current_extra_ship > 0 then shipper_id else null end) as double) as avg_current_extra_ship
        ,sum(extra_ship_opt1)/cast(count(distinct case when extra_ship_opt1 > 0 then shipper_id else null end) as double) as avg_extra_ship_opt1
        ,sum(extra_ship_opt2)/cast(count(distinct case when extra_ship_opt2 > 0 then shipper_id else null end) as double) as avg_extra_ship_opt2
        

from summary 


where report_date between current_date - interval '14' day and current_date - interval '1' day 
and total_ship_fee > 0

group by 1,2,3,4,5
