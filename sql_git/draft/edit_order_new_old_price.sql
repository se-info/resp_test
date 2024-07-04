with base as 
(SELECT t1.order_id
                             , t1.merchant_name
                             , t1.create_uid
                             , t1.create_time
                             , t1.update_time
                             , case when t1.edit_by = 'Driver' then 1 else 0 end as is_edit_by_driver 
                             , case when t1.edit_by = 'Merchant' then 1 else 0 end as is_edit_by_merchant
                             , t2.old_dish_qty
                             , t2.new_dish_qty
                             , t2.old_dish_price
                             , t2.new_dish_price
                             , t2.old_object_value
                             , t2.new_object_value       
                             , t2.dish_name   
                             , t2.object_name
                             , case when ((t2.old_dish_qty > 0 and t2.old_dish_qty > t2.new_dish_qty and t2.new_dish_qty <> 0 ) or  (t2.old_dish_qty > 0 and t2.old_dish_qty < t2.new_dish_qty)
                                            or (t2.old_dish_price > 0 and t2.old_dish_price > t2.new_dish_price and t2.new_dish_price <> 0) or  (t2.old_dish_price > 0 and t2.old_dish_price < t2.new_dish_price)
                                         ) then 1 else 0 end as is_edit 
                                         
                             , case when ((t2.old_dish_qty = 0 and t2.old_dish_qty < t2.new_dish_qty) or (t2.old_dish_price = 0 and t2.old_dish_price < t2.new_dish_price)
                                         ) then 1 else 0 end as is_add                  
                        
                             , case when ((t2.old_dish_qty > 0 and t2.new_dish_qty = 0) or (t2.old_dish_price > 0 and t2.new_dish_price = 0)
                                         ) then 1 else 0 end as is_remove
                             ,row_number()over(partition by order_id order by update_time desc) as rank_new_value
                             ,row_number()over(partition by order_id order by update_time asc) as rank_old_value
                                         
                        
                        FROM
                                    (
                                    SELECT order_id
                                         , do.merchant_id
                                         , do.create_uid
                                         , mm.merchant_name
                                         , from_unixtime(create_time -60*60) create_time
                                         , from_unixtime(update_time -60*60) update_time
                                         , case when create_source = 1 then 'Merchant'
                                                when create_source = 2 then 'Driver'
                                                else 'Others' end as edit_by
                                        --  , do.extra_data       
                                         , JSON_EXTRACT(do.extra_data, '$.draft_change.change_logs') AS change_logs
                                         
                                    FROM shopeefood.foody_order_db__order_draft_log_tab__reg_daily_s0_live do
                                    LEFT JOIN shopeefood.foody_mart__profile_merchant_master mm on mm.merchant_id = do.merchant_id and mm.grass_date = 'current' 
                                    WHERE JSON_EXTRACT(do.extra_data, '$.draft_change.change_logs') IS NOT NULL 
                                    --and create_source = 2
                                    ) t1 
                                    
                        CROSS JOIN UNNEST( CAST(change_logs AS ARRAY(ROW("old_dish_qty" DECIMAL, "new_dish_qty" DECIMAL, "old_dish_price" DECIMAL, "new_dish_price" DECIMAL , "dish_name" VARCHAR,"old_object_value" DECIMAL,"new_object_value" DECIMAL,"object_name" VARCHAR)))) 
                                            AS t2(old_dish_qty, new_dish_qty, old_dish_price,new_dish_price, dish_name,old_object_value,new_object_value,object_name)
                        
                        WHERE 1 = 1 
                        and (old_object_value is not null or new_object_value is not null) 
                        -- and t2.dish_name is not null 
                        -- and t1.edit_by in ('Driver')
                        -- ,'Merchant')
                        and date(t1.create_time) between current_date - interval '7' day and current_date - interval '1' day
                    --    and t1.order_id = 301611549
                       and t2.object_name = 'total amount'




                     order by update_time desc )




                     select  a.order_id
                            ,a.create_uid
                            ,a.old_object_value as old_price 
                            ,b.new_object_value as new_price
                            ,case when payment_method_id = 1 then 'COD' else 'Non COD' end as payment_method
                            ,case when apm.is_airpay_merchant_active_flag = 1 then 'Non COD' else 'COD' end as merchant_method
                            ,b.update_time as last_update_time
                            ,from_unixtime(pick_timestamp - 3600) as pickup_time
                            ,from_unixtime(final_status_timestamp - 3600) as final_status_timestamp
                            ,case when go.order_status_id = 7 then 'Delivered'
                                  when go.order_status_id = 8 then 'Cancelled'
                                  when go.order_status_id = 9 then 'Quit'
                                  else null end as order_status
 



                     from base a 
                     left join base b on b.order_id = a.order_id and a.create_uid = b.create_uid

                     left join shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = a.order_id

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


                     where 1 = 1  
                     and a.rank_old_value = 1 
                     and b.rank_new_value = 1 
                     



