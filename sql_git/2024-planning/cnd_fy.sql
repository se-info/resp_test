--overall
with completed_tab_continuous as
(SELECT 
        oct.*
        ,date(cast(from_unixtime(submit_time - 60*60) as timestamp)) as order_created_date 
        ,CAST(JSON_EXTRACT(oct.extra_data, '$.is_merchant_acknowledged') as varchar ) is_acknowledged
        ,HOUR(from_unixtime(submit_time - 60*60)) AS created_hour
        ,distance
        ,sub_total*1.00/100 AS sub_total
        ,CASE 
         WHEN merchant_paid_method = 6 THEN 'non_cod' else 'cod' end as merchant_paid_type
        ,YEAR(DATE(from_unixtime(submit_time - 60*60))) AS year_
        ,YEAR(DATE(from_unixtime(submit_time - 60*60)))*100 + MONTH(DATE(from_unixtime(submit_time - 60*60))) AS year_month
        ,city.name_en AS city_name
        ,case when oct.city_id in (217,218,219) then 'T1'
                 when oct.city_id in (220,221,222,223,230,273) then 'T2'
                 else 'T3' end as city_tier
        ,case 
                when oct.cancel_type in (0,5) then 'System'
                when oct.cancel_type = 1 then 'CS BPO'
                when oct.cancel_type = 2 then 'User'
                when oct.cancel_type in (3,4) then 'Merchant'
                when oct.cancel_type = 6 then 'Fraud'
                end as cancel_actor


    FROM shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct 
    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
        on city.id = oct.city_id 
        and city.country_id = 86

    WHERE 1=1     
    and date(cast(from_unixtime(submit_time - 3600) as timestamp)) between current_date - interval '7' day and current_date - interval '1' day
    AND city_id NOT IN  (0,238,468,469,470,471,472)
)
,cancel_reason_tab AS
(select distinct 
        a.id, 
        a.note_id
        , case  when trim(coalesce(cr.message_en, a.cancel_note)) in ('Out of stock of all order items','Hết tẩt cả món trong đơn hàng') then 1 else 0 end as is_oos_all_item
        , case  when a.status not in (8) then null 
                when a.note_id = 12 then 'Other reason - Merchant'
                when a.note_id = 69 then 'Other reason - Buyer'
                when a.note_id = 78 then 'Other reason - Driver'
                when a.note_id = 88 then 'Other reason - Merchant'
                when cast(crm.is_cancel_reason as bigint) = 1 then   
                    case when crm.cancel_reason = 'Shop closed' and is_pre_order> 0 then 'Preorder'
                            when crm.cancel_reason = 'Shop closed' then crm.cancel_reason
                            else crm.cancel_reason end

                when trim(coalesce(cr.message_en, a.cancel_note)) = '' then 'Missing reason'                                                                                             
                when trim(coalesce(cr.message_en, a.cancel_note)) is null then 'Missing reason'    
                else 'Others' end as cancel_reason
        ,trim(coalesce(cr.message_en, a.cancel_note)) as raw_cancel_reason
        ,a.cancel_actor

FROM (
select oct.id, oct.status
, try_cast(JSON_EXTRACT(oct.extra_data, '$.note_ids') AS bigint) note_id
, cast(JSON_EXTRACT(oct.extra_data, '$.cancel_note') as varchar) cancel_note
, oct.cancel_actor
, least(1,sum(CASE WHEN TRIM(element_at(split(bo.note_content, ':', 2), 2)) = 'Wrong Pre-order' THEN 1 ELSE 0 END)) is_pre_order

from (select id, status, extra_data,cancel_actor from completed_tab_continuous where 1 = 1 and status = 8) oct

LEFT JOIN 
(   
SELECT distinct order_id
,cast(JSON_EXTRACT(note_content, '$.default') AS varchar) as note_content
FROM shopeefood.foody_mart__fact_order_note
WHERE note_type_id = 2 -- note_type_id = 2 --> bo reason
AND COALESCE(cast(JSON_EXTRACT(note_content, '$.default') AS varchar), cast(JSON_EXTRACT(note_content, '$.en') AS varchar), extra_note) != ''
AND grass_region ='VN'
) bo on bo.order_id = oct.id
group by 1,2,3,4,5
) a 
LEFT JOIN shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr ON cr.id = a.note_id 
LEFT JOIN vnfdbi_opsndrivers.shopeefood_vn_cancel_reason_mapping_tab crm 
    on regexp_like(trim(coalesce(cr.message_en, a.cancel_note)),crm.actual_reason) = true
)
,final AS 
(SELECT      
        raw.*, 
        case 
         when city_name IN ('HCM City','Ha Noi City','Da Nang City') THEN city_name 
         when regexp_like(lower(city_name),'dak lak|thanh hoa|binh thuan|binh dinh') = true THEN 'new cities' 
         ELSE city_tier END AS cities,         
        CASE
        WHEN raw.status = 8 THEN 1 ELSE 0 END AS is_cancelled,
        COALESCE(crt.cancel_reason,NULL) AS cancel_reason

FROM completed_tab_continuous raw 
LEFT JOIN cancel_reason_tab crt 
    on crt.id = (CASE WHEN raw.status = 8 THEN raw.id ELSE 0 END) 
)
SELECT
        year_ AS period,
        COALESCE(cities,'VN') AS cities,
        count(distinct id)/CAST(COUNT(DISTINCT order_created_date) AS DOUBLE) as gross_order,
        count(distinct case when is_cancelled = 1 and cancel_reason = 'No driver' then id else null end )/CAST(COUNT(DISTINCT order_created_date) AS DOUBLE) AS cnd,
        count(distinct case when is_cancelled = 1 and cancel_reason = 'No driver' then id else null end )/CAST(count(distinct id) AS DOUBLE) AS pct_cnd

FROM final 

GROUP BY 1, grouping sets(cities,())
;
--segment

--overall
with completed_tab_continuous as
(SELECT 
        oct.*
        ,date(cast(from_unixtime(submit_time - 60*60) as timestamp)) as order_created_date 
        ,CAST(JSON_EXTRACT(oct.extra_data, '$.is_merchant_acknowledged') as varchar ) is_acknowledged
        ,HOUR(from_unixtime(submit_time - 60*60)) AS created_hour
        ,distance
        ,sub_total*1.00/100 AS sub_total
        ,CASE 
         WHEN merchant_paid_method = 6 THEN 'non_cod' else 'cod' end as merchant_paid_type
        ,YEAR(DATE(from_unixtime(submit_time - 60*60))) AS year_
        ,YEAR(DATE(from_unixtime(submit_time - 60*60)))*100 + MONTH(DATE(from_unixtime(submit_time - 60*60))) AS year_month
        ,city.name_en AS city_name
        ,case when oct.city_id in (217,218,219) then 'T1'
                 when oct.city_id in (220,221,222,223,230,273) then 'T2'
                 else 'T3' end as city_tier
        ,case 
                when oct.cancel_type in (0,5) then 'System'
                when oct.cancel_type = 1 then 'CS BPO'
                when oct.cancel_type = 2 then 'User'
                when oct.cancel_type in (3,4) then 'Merchant'
                when oct.cancel_type = 6 then 'Fraud'
                end as cancel_actor


    FROM shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct 
    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
        on city.id = oct.city_id 
        and city.country_id = 86

    WHERE 1=1     
    and date(cast(from_unixtime(submit_time - 3600) as timestamp)) between current_date - interval '7' day and current_date - interval '1' day
    AND city_id NOT IN  (0,238,468,469,470,471,472)
)
,cancel_reason_tab AS
(select distinct 
        a.id, 
        a.note_id
        , case  when trim(coalesce(cr.message_en, a.cancel_note)) in ('Out of stock of all order items','Hết tẩt cả món trong đơn hàng') then 1 else 0 end as is_oos_all_item
        , case  when a.status not in (8) then null 
                when a.note_id = 12 then 'Other reason - Merchant'
                when a.note_id = 69 then 'Other reason - Buyer'
                when a.note_id = 78 then 'Other reason - Driver'
                when a.note_id = 88 then 'Other reason - Merchant'
                when cast(crm.is_cancel_reason as bigint) = 1 then   
                    case when crm.cancel_reason = 'Shop closed' and is_pre_order> 0 then 'Preorder'
                            when crm.cancel_reason = 'Shop closed' then crm.cancel_reason
                            else crm.cancel_reason end

                when trim(coalesce(cr.message_en, a.cancel_note)) = '' then 'Missing reason'                                                                                             
                when trim(coalesce(cr.message_en, a.cancel_note)) is null then 'Missing reason'    
                else 'Others' end as cancel_reason
        ,trim(coalesce(cr.message_en, a.cancel_note)) as raw_cancel_reason
        ,a.cancel_actor

FROM (
select oct.id, oct.status
, try_cast(JSON_EXTRACT(oct.extra_data, '$.note_ids') AS bigint) note_id
, cast(JSON_EXTRACT(oct.extra_data, '$.cancel_note') as varchar) cancel_note
, oct.cancel_actor
, least(1,sum(CASE WHEN TRIM(element_at(split(bo.note_content, ':', 2), 2)) = 'Wrong Pre-order' THEN 1 ELSE 0 END)) is_pre_order

from (select id, status, extra_data,cancel_actor from completed_tab_continuous where 1 = 1 and status = 8) oct

LEFT JOIN 
(   
SELECT distinct order_id
,cast(JSON_EXTRACT(note_content, '$.default') AS varchar) as note_content
FROM shopeefood.foody_mart__fact_order_note
WHERE note_type_id = 2 -- note_type_id = 2 --> bo reason
AND COALESCE(cast(JSON_EXTRACT(note_content, '$.default') AS varchar), cast(JSON_EXTRACT(note_content, '$.en') AS varchar), extra_note) != ''
AND grass_region ='VN'
) bo on bo.order_id = oct.id
group by 1,2,3,4,5
) a  shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr ON cr.id = a.note_id 
LEFT JOIN vnfdbi_opsndrivers.shopeefood_vn_cancel_reason_mapping_tab crm 
    on regexp_like(trim(coalesce(cr.message_en, a.cancel_note)),crm.actual_reason) = true
)
,raw AS 
(SELECT      
        raw.*, 
        case 
         when city_name IN ('HCM City','Ha Noi City','Da Nang City') THEN city_name 
         when regexp_like(lower(city_name),'dak lak|thanh hoa|binh thuan|binh dinh') = true THEN 'new cities' 
         ELSE city_tier END AS cities,         
        CASE
        WHEN raw.status = 8 THEN 1 ELSE 0 END AS is_cancelled,
        COALESCE(crt.cancel_reason,NULL) AS cancel_reason

FROM completed_tab_continuous raw 
LEFT JOIN cancel_reason_tab crt 
    on crt.id = (CASE WHEN raw.status = 8 THEN raw.id ELSE 0 END) 
)
SELECT 
        year_month,
        '0. timeslot' AS segment,
        CASE
        WHEN created_hour < 11 THEN 'early morning'
        WHEN created_hour < 14 THEN 'lunch'
        WHEN created_hour < 18 THEN 'off peak'
        WHEN created_hour < 21 THEN 'dinner'
        WHEN created_hour >= 21 THEN 'late night' END AS range_,
        COUNT(DISTINCT order_id)/CAST(COUNT(DISTINCT created_date) AS DOUBLE) AS ado,
        COUNT(DISTINCT CASE WHEN is_cancelled = 1 AND cancel_reason = 'No driver' THEN order_id  ELSE NULL END)
        /CAST(COUNT(DISTINCT created_date) AS DOUBLE) AS cnd


FROM raw 
GROUP BY 1,2,3
UNION ALL 
SELECT 
        year_month,
        '1. distance' AS segment,
        CASE
        WHEN distance < 1 THEN '1. 0-1km'
        WHEN distance < 3 THEN '2. 1-3km'
        WHEN distance < 5 THEN '3. 3-5km'
        WHEN distance < 7 THEN '4. 5-7km'
        WHEN distance < 10 THEN '5. 7-10km'
        WHEN distance >= 10 THEN '6. 10km++' END AS range_,
        COUNT(DISTINCT order_id)/CAST(COUNT(DISTINCT created_date) AS DOUBLE) AS ado,
        COUNT(DISTINCT CASE WHEN is_cancelled = 1 AND cancel_reason = 'No driver' THEN order_id  ELSE NULL END)
        /CAST(COUNT(DISTINCT created_date) AS DOUBLE) AS cnd


FROM raw 
GROUP BY 1,2,3
UNION ALL
SELECT 
        year_month,
        '2. order_value' AS segment,
        CASE 
        WHEN sub_total_new < 500000 THEN '1. 0-500k'
        WHEN sub_total_new < 1000000 THEN '2. 500-1000k'
        WHEN sub_total_new < 1500000 THEN '3. 1000-1500k'
        WHEN sub_total_new < 2000000 THEN '4. 1500-2000k'
        WHEN sub_total_new >= 2000000 THEN '5. 2000k++' END AS range_, 
        -- COALESCE(cities,'VN') AS cities,
        COUNT(DISTINCT order_id)/CAST(COUNT(DISTINCT created_date) AS DOUBLE) AS ado,
        COUNT(DISTINCT CASE WHEN is_cancelled = 1 AND cancel_reason = 'No driver' THEN order_id  ELSE NULL END)
        /CAST(COUNT(DISTINCT created_date) AS DOUBLE) AS cnd


FROM raw 
GROUP BY 1,2,3