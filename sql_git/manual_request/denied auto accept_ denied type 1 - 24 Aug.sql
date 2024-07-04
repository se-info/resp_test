with quit as 
(select  
        oct.id 
       ,oct.shipper_uid as shipper_id  
    --    ,CASE WHEN CAST(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') AS integer) = 1 THEN 'Now' 
    --          ELSE 'Driver' END AS risk_bearer
       ,date(quit.quit_timestamp) as date_ts                
       ,COALESCE(CAST(json_extract(bo.note_content, '$.default') AS varchar), bo.extra_note) quit_reason
       ,case when  oct.status = 9 
                   then (case when cast(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') as INTEGER) = 1 then 'Now' else 'Driver' end )
             else '-' end as risk_bearer      


from shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live oct 

LEFT JOIN (select * from shopeefood.foody_mart__fact_order_note
                    where grass_region = 'VN' ) bo 
                    ON bo.order_id = oct.id AND bo.note_type_id = 3
                    AND COALESCE(CAST(json_extract(bo.note_content, '$.default') AS varchar), CAST(json_extract(bo.note_content, '$.en') AS varchar), bo.extra_note) <> ''

LEFT JOIN (
            SELECT
              osl.order_id
            , osl.create_uid
            , max(from_unixtime(osl.create_time - 3600)) quit_timestamp
            FROM
              shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live osl
            WHERE 1 = 1 
            AND status = 9
            GROUP BY 1, 2
         )  quit ON quit.order_id = oct.id


where date(quit.quit_timestamp) between current_date - interval '30' day and current_date - interval '1' day

-- group by 1,2
)
,base as 
(SELECT
              dod.deny_date as date_
            , dod.shipper_id
            , dod.deny_type
            , dod.deny_reason
            , dod.ref_order_id as order_id
            , dod.order_source as order_type
            , dod.ref_order_category
            , dod.deny_timestamp
            , raw.order_status
            , coalesce(is_manual_loss,0) as is_manual_loss
            , coalesce(manual_compensation_loss,0) as compensation
            , rank_ 
            -- , case when rank_ = 1 and dod.ref_order_category = 0 then coalesce(manual_compensation_loss,0) else 0 end as compensation                    
            , dod.city_name 
            , case when dod.ref_order_category = 0 then coalesce(raw.cancel_reason,null) 
                   when dod.ref_order_category = 6 and raw.order_status = 'Assigning Timeout' then 'No Driver'
                   else null end as cancel_reason
            , case when dod.ref_order_category = 0 and order_status = 'Quit' then quit.quit_reason
                   else null end as quit_reason
            , case when dod.ref_order_category = 0 and order_status = 'Quit' then quit.risk_bearer
                   else null end as risk_bearer
            -- ,row_number()over(partition by ref_order_id order by deny_timestamp asc) as rank
            
FROM
            (
            SELECT
                dod.uid AS shipper_id
                , DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_date
                , FROM_UNIXTIME(dod.create_time - 3600) AS deny_timestamp
                , HOUR(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_hour
                , MINUTE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_minute
                , dot.ref_order_id
                , dot.ref_order_code
                , dot.ref_order_category
                , rea.content_en as deny_reason
                , city.name_en as city_name 
                -- , dot.ref_order_category
                , CASE
                    WHEN dot.ref_order_category = 0 THEN 'Food/Market'
                    WHEN dot.ref_order_category = 4 THEN 'NS Instant'
                    WHEN dot.ref_order_category = 5 THEN 'NS Food Mex'
                    WHEN dot.ref_order_category = 6 THEN 'NS Shopee'
                    WHEN dot.ref_order_category = 7 THEN 'NS Same Day'
                    WHEN dot.ref_order_category = 8 THEN 'NS Multi Drop'
                ELSE NULL END AS order_source
                , CASE
                    WHEN dod.deny_type = 0 THEN 'NA'
                    WHEN dod.deny_type = 1 THEN 'Driver_Fault'
                    WHEN dod.deny_type = 10 THEN 'Order_Fault'
                    WHEN dod.deny_type = 11 THEN 'Order_Pending'
                    WHEN dod.deny_type = 20 THEN 'System_Fault'
                END AS deny_type
                , reason_text
                , row_number()over(partition by dod.order_id order by dod.create_time desc ) as rank_ 

            FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod
            LEFT JOIN shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on dod.order_id = dot.id
            LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

            left  join shopeefood.foody_internal_db__deny_reason_template_tab__reg_daily_s0_live rea on rea.id = dod.reason_id

            WHERE DATE(FROM_UNIXTIME(dod.create_time - 3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day

            ) dod

left join dev_vnfdbi_opsndrivers.food_raw_phong raw on raw.id = dod.ref_order_id and dod.ref_order_category = raw.order_type

LEFT JOIN dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_ops_order_detail_ext cps on cps.id = dod.ref_order_id and dod.ref_order_category = 0 

left join quit on quit.id = dod.ref_order_id and dod.ref_order_category = 0

-- select * from dev_vnfdbi_opsndrivers.food_raw_phong


where 1 = 1 
-- and dod.deny_type = 'Driver_Fault'
and dod.deny_reason like '%Auto%'

)


select 
         date_ 
       ,cancel_reason
       ,city_name
       ,count(order_id) as total_denied_turn
       ,count(distinct order_id) unique_order
       ,count(case when compensation = 1 then order_id else null end) as total_denied_turn_have_compensated
       ,count(distinct case when compensation = 1 then order_id else null end) as unique_order_have_compensated
       ,sum(case when rank_ = 1 then compensation else null end) as total_manual_compensated



from base 

where 1 = 1 
-- and compensation > 0 
group by 1,2,3



-- select * from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_ops_order_detail_ext


-- where grass_date = date'2022-08-20'
-- and manual_compensation_loss > 0 
-- -- group by



-- select max(report_date)
-- from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_ops_order_detail_ext




