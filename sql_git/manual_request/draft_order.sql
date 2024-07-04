select  date(create_time) as create_date
       ,merchant_name
       ,count(case when merchant_action is not null then order_id else null end) as total_merchant_action

from  

(SELECT t4.*
              ,date_diff('second',create_time,update_time) as lt_edit_to_user_approve
              ,substr( concat(case when driver_edit is not null then ', ' || driver_edit else '' end,
                              case when driver_add is not null then ', ' || driver_add else '' end,
                              case when driver_remove is not null then ', ' || driver_remove else '' end
                             ),3) as driver_action
                             
              ,substr( concat(case when merchant_edit is not null then ', ' || merchant_edit else '' end,
                              case when merchant_add is not null then ', ' || merchant_add else '' end,
                              case when merchant_remove is not null then ', ' || merchant_remove else '' end
                             ),3) as merchant_action   
               , CASE WHEN go.order_status_id in (7,9) and (go.is_now_merchant_order_flag = 1 or COALESCE(apm.is_airpay_merchant_active_flag,0) = 1) THEN 1 ELSE 0 END as is_merchant_tool             
        FROM 
                (
                SELECT t3.order_id 
                      ,t3.merchant_name
                      ,min(t3.create_time) create_time
                      ,max(t3.update_time) update_time
                      ,least(sum(t3.is_edit_by_driver),1) is_edit_by_driver
                      ,least(sum(t3.is_edit_by_merchant),1) is_edit_by_merchant
                      ,case when least(sum(case when is_edit_by_driver = 1 then is_edit else 0 end),1) = 1 then 'Edit' else null end as driver_edit 
                      ,case when least(sum(case when is_edit_by_driver = 1 then is_add else 0 end),1) = 1 then 'Add' else null end as driver_add
                      ,case when least(sum(case when is_edit_by_driver = 1 then is_remove else 0 end),1) = 1 then 'Remove' else null end as driver_remove
                
                      ,case when least(sum(case when is_edit_by_merchant = 1 then is_edit else 0 end),1) = 1 then 'Edit' else null end as merchant_edit 
                      ,case when least(sum(case when is_edit_by_merchant = 1 then is_add else 0 end),1) = 1 then 'Add' else null end as merchant_add
                      ,case when least(sum(case when is_edit_by_merchant = 1 then is_remove else 0 end),1) = 1 then 'Remove' else null end as merchant_remove
                
                FROM 
                        (
                        SELECT t1.order_id
                             , t1.merchant_name
                             , t1.create_time
                             , t1.update_time
                             , case when t1.edit_by = 'Driver' then 1 else 0 end as is_edit_by_driver 
                             , case when t1.edit_by = 'Merchant' then 1 else 0 end as is_edit_by_merchant
                             , t2.old_dish_qty
                             , t2.new_dish_qty
                             , t2.old_dish_price
                             , t2.new_dish_price       
                             , t2.dish_name   
                             , case when ((t2.old_dish_qty > 0 and t2.old_dish_qty > t2.new_dish_qty and t2.new_dish_qty <> 0 ) or  (t2.old_dish_qty > 0 and t2.old_dish_qty < t2.new_dish_qty)
                                            or (t2.old_dish_price > 0 and t2.old_dish_price > t2.new_dish_price and t2.new_dish_price <> 0) or  (t2.old_dish_price > 0 and t2.old_dish_price < t2.new_dish_price)
                                         ) then 1 else 0 end as is_edit 
                                         
                             , case when ((t2.old_dish_qty = 0 and t2.old_dish_qty < t2.new_dish_qty) or (t2.old_dish_price = 0 and t2.old_dish_price < t2.new_dish_price)
                                         ) then 1 else 0 end as is_add                  
                        
                             , case when ((t2.old_dish_qty > 0 and t2.new_dish_qty = 0) or (t2.old_dish_price > 0 and t2.new_dish_price = 0)
                                         ) then 1 else 0 end as is_remove
                        
                        FROM
                                    (
                                    SELECT order_id
                                         , do.merchant_id
                                         , mm.merchant_name
                                         , from_unixtime(create_time -60*60) create_time
                                         , from_unixtime(update_time -60*60) update_time
                                         , case when create_source = 1 then 'Merchant'
                                                when create_source = 2 then 'Driver'
                                                else 'Others' end as edit_by
                                         , JSON_EXTRACT(do.extra_data, '$.draft_change.change_logs') AS change_logs
                                         
                                    FROM shopeefood.foody_order_db__order_draft_log_tab__reg_daily_s0_live do
                                    LEFT JOIN shopeefood.foody_mart__profile_merchant_master mm on mm.merchant_id = do.merchant_id and mm.grass_date = 'current' 
                                    WHERE JSON_EXTRACT(do.extra_data, '$.draft_change.change_logs') IS NOT NULL 
                                    --and create_source = 2
                                    ) t1 
                                    
                        CROSS JOIN UNNEST( CAST(change_logs AS ARRAY(ROW("old_dish_qty" DECIMAL, "new_dish_qty" DECIMAL, "old_dish_price" DECIMAL, "new_dish_price" DECIMAL , "dish_name" VARCHAR)))) AS t2(old_dish_qty, new_dish_qty, old_dish_price,new_dish_price, dish_name)
                        
                        WHERE t2.dish_name is not null 
                        and t1.edit_by in ('Driver','Merchant')
                        and date(t1.create_time) between date'2022-04-01' and date'2022-04-30'
                     --   and t1.order_id = 192450209 --195376771
                        )t3
                
                GROUP BY 1,2
                )t4

left join shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = t4.order_id 
LEFT JOIN
         (SELECT m.merchant_id
                 ,CASE WHEN m.grass_date = 'current' THEN date(current_date)
                       ELSE TRY_CAST(m.grass_date as date) END AS report_date
                ,m.is_airpay_merchant_active_flag
          FROM shopeefood.foody_mart__profile_merchant_master m 
          WHERE grass_region = 'VN'
          GROUP BY 1,2,3
         ) apm 
         ON apm.merchant_id = go.merchant_id and apm.report_date = date(from_unixtime(go.create_timestamp))

where 1=1 
and date(create_time) between date'2022-04-01' and date'2022-04-30'
and CASE WHEN go.order_status_id in (7,9) and (go.is_now_merchant_order_flag = 1 or COALESCE(apm.is_airpay_merchant_active_flag,0) = 1) THEN 1 ELSE 0 END = 1
and merchant_name = 'Mỹ Đức Bình Điền - Hải Sản & Rau Củ Quả Sạch'

)



group by 1,2 