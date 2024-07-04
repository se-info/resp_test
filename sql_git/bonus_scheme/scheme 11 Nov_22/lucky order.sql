with raw as 
(SELECT
         id
        ,group_code
        ,group_status  
        ,from_unixtime(create_time - 3600) as create_time 
        ,from_unixtime(update_time - 3600) as update_time 

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day)
WHERE grass_schema = 'foody_partner_db'
and group_code in 
('D33431831691'
,'D88597456845'
,'D87048787771'
,'D67705995109'
,'D49024591714'
,'D48641572751'
,'D43591030617'
,'D41451720049')
)
select 
-- Group search 
         raw.group_code
        ,od.ref_order_code
        ,case when real_drop_time > 0 then from_unixtime(dot.real_drop_time - 3600) else null end as delivered_timestamp
        ,case when dot.order_status  = 400 then 'delivered' else 'others' end as order_staus
        ,dot.uid as shipper_id
        ,sp.shopee_uid 
        ,sm.shipper_name
        ,sm.city_name
        ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as driver_type
        ,rp.total_completed_order 
-- Single search
        --  dot.ref_order_code           
        -- ,case when real_drop_time > 0 then from_unixtime(dot.real_drop_time - 3600) else null end as delivered_timestamp
        -- ,case when dot.order_status  = 400 then 'delivered' else 'others' end as order_staus
        -- ,dot.uid as shipper_id
        -- ,sp.shopee_uid 
        -- ,sm.shipper_name
        -- ,sm.city_name
        -- ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as driver_type
        -- ,rp.total_completed_order             


-- from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

from raw 

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) od on od.group_id = raw.id 

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
            where date(dt) = current_date - interval '1' day) dot on od.ref_order_id = dot.ref_order_id
                                                                  and od.ref_order_category = dot.ref_order_category

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(from_unixtime(dot.submitted_time - 3600))                                                                  

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sp on sp.uid = dot.uid 

left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp on rp.uid = dot.uid and date(from_unixtime(rp.report_date - 3600)) = date'2022-11-11'

where 1 = 1 

