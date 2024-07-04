select  
        oct.id 
       ,oct.shipper_uid as shipper_id  
       ,CASE WHEN CAST(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') AS integer) = 1 THEN 'Now' 
             ELSE 'Driver' END AS risk_bearer
       ,date(quit.quit_timestamp) as date_ts             
       ,COALESCE(CAST(json_extract(bo.note_content, '$.default') AS varchar), bo.extra_note) quit_reason      


from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct 

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