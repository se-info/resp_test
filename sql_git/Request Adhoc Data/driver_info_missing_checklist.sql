with driver_checklist as 
(select 
        uid,
        -- case 
        -- when template_id = 1 then ' Received supporting documents'
        -- when template_id = 2 then ' Received deposit'
        -- when template_id = 3 then ' Shipper received uniform'
        -- when template_id = 4 then ' 01 Criminal record check (photo of the front of the record)'
        -- when template_id = 5 then ' Identity card / Passport (Notarized)'
        -- when template_id = 6 then ' 01 Driving license (photo of the front and back of license)'
        -- when template_id = 7 then ' 01 Vehicle registration certificate (photo of the front and back of license)'
        -- when template_id = 8 then ' Sacombank ATM card (Internet Banking)'
        -- when template_id = 9 then ' 01 Family register or Temporary Residence Card (photo of householder information + photo of driver information)'
        -- when template_id = 10 then ' Curriculum vitae (Notarized)'
        -- when template_id = 11 then ' Health certificate (last 6 months)'
        -- when template_id = 12 then ' Have AirPay account with GIRO' end as template_name,
        array_agg(distinct template_id) as template_id


from shopeefood.foody_internal_db__shipper_check_list_tab__reg_daily_s0_live
where template_id in (6,7)

-- change binary to varchar
and from_utf8(is_checked) = '1'

-- and from_utf8(has_document) = '0'

group by 1 
)
,agg_driver AS 
(SELECT
        shipper_id,
        max_by(city_name,report_date) as city_name,
        ARRAY_AGG( DISTINCT
        CASE
        WHEN total_order > 0 THEN report_date ELSE NULL END            
        ) AS agg_delivered_date
FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab

WHERE total_order > 0
GROUP BY 1 
)
select * from 
(select 
        raw.uid as shipper_id,
        raw.shopee_uid,
        sm.city_name,
        case 
        when sm.shipper_status_code = 1 then 'working' else 'off' end as working_status,
        coalesce(cardinality(filter(dc.template_id,x->x = 6)),0) as no_of_driving_license,
        coalesce(cardinality(filter(dc.template_id,x->x = 7)),0) as no_of_vehicle_registration,        
        coalesce(cardinality(filter(agg.agg_delivered_date,x->x = current_date - interval '1' day)),0) as no_of_a1,
        coalesce(cardinality(filter(agg.agg_delivered_date,x->x between current_date - interval '7' day and current_date - interval '1' day)),0) as no_of_a7,
        coalesce(cardinality(filter(agg.agg_delivered_date,x->x between current_date - interval '30' day and current_date - interval '1' day)),0) as no_of_a30,
        coalesce(cardinality(filter(agg.agg_delivered_date,x->x between current_date - interval '120' day and current_date - interval '1' day)),0) as no_of_a120


from shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live  raw 
left join driver_checklist dc on dc.uid = raw.uid
left join agg_driver agg on agg.shipper_id = raw.uid
left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.uid and sm.grass_date = 'current'
)
where (no_of_driving_license = 0 or no_of_vehicle_registration = 0)
and working_status != 'off'
and regexp_like(city_name,'Dien Bien|Test|test|live|stress|Stress') = false 