with raw as 
(SELECT order_id
    , do.merchant_id
    , do.create_uid
    , mm.merchant_name
    , from_unixtime(create_time -60*60) create_time
    , from_unixtime(update_time -60*60) update_time
    , case when create_source = 1 then 'Merchant'
           when create_source = 2 then 'Driver' 
           else 'Others' end as edit_by
    , JSON_EXTRACT(do.extra_data, '$.draft_change.change_logs') AS change_logs
    -- , do.extra_data
                                         
FROM shopeefood.foody_order_db__order_draft_log_tab__reg_daily_s0_live do

LEFT JOIN shopeefood.foody_mart__profile_merchant_master mm on mm.merchant_id = do.merchant_id and try_cast(mm.grass_date as date) = date(from_unixtime(create_time -60*60)) 

WHERE JSON_EXTRACT(do.extra_data, '$.draft_change.change_logs') IS NOT NULL 
--and create_source = 2
)
,final_metrics as 
(SELECT 
          t1.order_id
        , t1.create_uid  
        , t1.merchant_name
        , t1.create_time
        , t1.update_time
        , case when t1.edit_by = 'Driver' then 1 else 0 end as is_edit_by_driver 
        , case when t1.edit_by = 'Merchant' then 1 else 0 end as is_edit_by_merchant
        , cast(t2.old_dish_qty as varchar) as old_dish_qty
        , cast(t2.new_dish_qty as varchar) as new_dish_qty
        , cast(t2.old_dish_price as varchar) as old_dish_price
        , cast(t2.new_dish_price as varchar) as new_dish_price       
        , cast(t2.dish_name as varchar) as dish_name   
        , case when ((t2.old_dish_qty > 0 and t2.old_dish_qty > t2.new_dish_qty and t2.new_dish_qty <> 0 ) or  (t2.old_dish_qty > 0 and t2.old_dish_qty < t2.new_dish_qty)
                or (t2.old_dish_price > 0 and t2.old_dish_price > t2.new_dish_price and t2.new_dish_price <> 0) or  (t2.old_dish_price > 0 and t2.old_dish_price < t2.new_dish_price)
                    ) then 1 else 0 end as is_edit 
        , case when ((t2.old_dish_qty = 0 and t2.old_dish_qty < t2.new_dish_qty) or (t2.old_dish_price = 0 and t2.old_dish_price < t2.new_dish_price)
                    ) then 1 else 0 end as is_add                  
        , case when ((t2.old_dish_qty > 0 and t2.new_dish_qty = 0) or (t2.old_dish_price > 0 and t2.new_dish_price = 0)
                    ) then 1 else 0 end as is_remove

FROM raw t1
                                   
CROSS JOIN UNNEST( CAST(change_logs AS ARRAY(ROW("old_dish_qty" DECIMAL, "new_dish_qty" DECIMAL, "old_dish_price" DECIMAL, "new_dish_price" DECIMAL , "dish_name" VARCHAR)))) AS t2(old_dish_qty, new_dish_qty, old_dish_price,new_dish_price, dish_name)
                        
WHERE t2.dish_name is not null 
and t1.edit_by in ('Driver'/*,'Merchant'*/)
and date(t1.create_time) between current_date - interval '1' day and  current_date - interval '1' day
)
-- select * from final_metrics where order_id = 417079102

select
        fm.order_id
       ,fm.create_uid as driver_id
       ,sm.shipper_name
       ,sm.city_name
       ,IF(sm.shipper_type_id = 12,'Hub','Non hub') as shipper_type 
       ,fm.is_remove 
       ,fm.is_edit 
       ,fm.is_add
       ,date(fm.create_time) as created_date
       ,oct.total_amount/cast(100 as double) as actual_order_value
       ,row_number()over(partition by fm.create_uid,date(fm.create_time) order by fm.create_time) as count_turn_adjust 
       ,case when fm.is_remove = 1 then sum(cast(old_dish_price as bigint)) 
             when fm.is_edit = 1 then sum(cast(new_dish_price as bigint) - cast(old_dish_price as bigint)) 
             when fm.is_add = 1 then sum(cast(new_dish_price as bigint)) end as gap_price


       ,array_agg( map(array['type','old_price','update_price','item_name'],array[(case when is_remove = 1 then 'Removed'
                                                                                        when is_add = 1 then 'Added'
                                                                                        when is_edit = 1 then 'Edited' end),old_dish_price,new_dish_price,dish_name])) as ext_info
       ,count(case when is_remove = 1 then dish_name else null end) as total_remove                                                                                         
       ,count(case when is_add = 1 then dish_name else null end) as total_add                                                                                         
       ,count(case when is_edit = 1 then dish_name else null end) as total_edit                                                                                         
                                                                                                



    --    ,map(array[dish_name],array[''])
    --    ,sum(gap_price) as gap_price   
    --    ,case when is_edit = 1 then map_agg(dish_name,map(array['old_price','update_price'],array[old_dish_price,new_dish_price]))
    --          when is_remove = 1 then map_agg('Removed',map(array['dish_price','dish_name'],array[old_dish_price,dish_name]))
    --          when is_add = 1 then map_agg('Added',map(array[dish_name],array['']))  end as order_change_log 
    --    ,case when is_edit = 1 then map_agg(dish_name,map(cast(old_dish_price as varchar),cast(new_dish_price as varchar))) else 0 end as t_1


from final_metrics fm 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = fm.create_uid and try_cast(sm.grass_date as date) = date(fm.create_time)

left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = fm.order_id

where 1 = 1 

group by 1,2,3,4,5,6,7,8,9,10,fm.create_time
;
--#Combine cancel OOS and draft edit
with base as 
(select 
        oct.id as order_id 
       ,date(from_unixtime(oct.submit_time - 3600)) as created_order_date 
       ,oct.shipper_uid as shipper_id      
       ,sm.shipper_name
       ,sm.city_name
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as shipper_type
       ,coalesce(cr.message_en,json_extract_scalar(oct.extra_data, '$.cancel_note')) as cancel_note  
       ,case 
                when oct.cancel_type in (0,5) then 'System'
                when oct.cancel_type = 1 then 'CS BPO'
                when oct.cancel_type = 2 then 'User'
                when oct.cancel_type in (3,4) then 'Merchant'
                when oct.cancel_type = 6 then 'Fraud'
                end as cancel_actor
        ,case when oct.status = 7 then 'Delivered' when oct.status = 8 then 'Cancelled' when oct.status = 9 then 'Quit' end as order_status 
        ,dr.ext_info as draft_order_log
        ,dr.total_remove
        ,dr.total_edit
        ,dr.total_add
        ,dr.count_turn_adjust
        ,dr.gap_price
        ,case when oct.merchant_paid_method = 1 then 'Cash'
              else 'Online payment' end as merchant_paid_type --1 is cod; 6 is online
        ,case when oct.payment_method = 1 then 'Cash'
              else 'Online payment' end as user_paid_type
        ,oct.commission_amount/cast(100 as double) as commission_fee 
        ,oct.total_discount_amount/cast(100 as double) as total_discount_amount   
        ,oct.merchant_paid_amount/cast(100 as double) as merchant_paid_amount
        ,oct.total_amount/cast(100 as double) as user_paid_amount

from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct 

left join shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int)

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = oct.shipper_uid and try_cast(sm.grass_date as date) = date(from_unixtime(oct.submit_time - 3600))

left join dev_vnfdbi_opsndrivers.phong_draft_order_checking dr on dr.order_id = oct.id 

where date(from_unixtime(oct.submit_time - 3600)) between current_date - interval '45' day and  current_date - interval '1' day
)
select 


* from base 


where 1 = 1 

and (case when order_status = 'Cancelled' and merchant_paid_type = 'Cash' and user_paid_type = 'Cash' then cancel_note = 'Out of stock of all order items' else draft_order_log is not null end) 

