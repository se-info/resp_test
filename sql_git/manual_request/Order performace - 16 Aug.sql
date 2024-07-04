with base as 
(
select * from dev_vnfdbi_opsndrivers.food_raw_phong
        )
,final as 
(
SELECT   base1.id
        ,base1.uid
        ,base1.cancel_date
        -- ,base1.inflow_date
        ,base1.created_date
        ,base1.created_hour
        -- ,base1.inflow_hour
        ,base1.shipper_id
        ,base1.city_group
        -- ,base1.district_name
        ,base1.city_name
        ,base1.is_asap
        ,base1.order_status
        ,base1.order_type
        -- ,base1.foody_service
        -- ,base1.item_range
        -- ,base1.total_amount_range
        -- ,base1.distance_range
        ,case when trim(base1.cancel_reason) = 'Shop closed' then (case when po.is_pre_order> 0 then 'Pre-order' else 'Shop closed' end)
              else base1.cancel_reason end as cancel_reason

        ,case when order_status = 'Cancelled' then 1 else 0 end as is_canceled
        ,case when order_status = 'Delivered' then 1 else 0 end as is_del
        ,case when order_status = 'Quit' then 1 else 0 end as is_quit
        ,case when sa.total_assign_turn >= 1 then 1 else 0 end as total_assign_turn
        ,case when ign.total_ignore >= 1 then 1 else 0 end as total_ignore 
        ,case when ign.total_denied >= 1 then 1 else 0 end as total_denied 
FROM
(
SELECT
         base.id
        ,base.uid
        -- ,case when total_item <= 10 then '1. 0 - 10 items'
        --       when total_item <= 20 then '2. 10 - 20 items'
        --       when total_item <= 30 then '3. 20 - 30 items'
        --       when total_item <= 40 then '4. 30 - 40 items'
        --       when total_item > 40 then '5. > 40 items'

        --       end as item_range

        -- ,case when total_amount <= 100000 then '1. 0 - 100,000 vnd'
        --       when total_amount <= 200000 then '2. 100,000 - 200,000 vnd' 
        --       when total_amount <= 400000 then '3. 200,000 - 400,000 vnd'
        --       when total_amount <= 500000 then '4. 400,000 - 500,000 vnd' 
        --       when total_amount > 500000 then '5. > 500,000 vnd'   
        --       end as total_amount_range

        -- ,case when distance <= 3 then '1. 0 - 3km'
        --       when distance <= 5 then '2. 3 - 5km'
        --       when distance <= 10 then '3. 5 - 10km'
        --       when distance > 10 then '4. > 10km' end as distance_range

        ,base.cancel_date
        ,base.created_date
        ,base.created_hour
        -- ,case when order_type = 0 then 'order-delivery' else 'order-spxi' end as order_type
        ,base.order_type
        ,base.shipper_id
        ,base.city_group
        ,base.city_name
        ,base.is_asap
        ,base.order_status
        ,base.cancel_by as cancel_reason
FROM base 

)base1
LEFT JOIN
            (SELECT
                    base.id
                    ,sum(base.is_pre_order) as is_pre_order
            FROM
                    (select oct.id
                            ,date(from_unixtime(oct.submit_time - 3600)) as submit_date
                            ,case when trim(split(cast(json_extract(bo.note_content, '$.default') as VARCHAR),':',2)[2]) = 'Wrong Pre-order' then 1 else 0 end is_pre_order

                    from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
                    left join
                             (SELECT *

                              FROM shopeefood.foody_mart__fact_order_note

                              WHERE 1=1
                              and grass_region ='VN'
                              )bo on bo.order_id = oct.id and bo.note_type_id = 2 -- note_type_id = 2 --> bo reason
                                                         and COALESCE(cast(json_extract(bo.note_content, '$.default') as VARCHAR),cast(json_extract(bo.note_content, '$.en') as VARCHAR), bo.extra_note) != ''

                    where 1=1


                    --and oct.foody_service_id = 1
                    ) base
            group by 1
            ) po 
ON base1.id = po.id and base1.order_type = 0

LEFT JOIN
            (SELECT 
                     a.order_id
                    ,a.order_type
                    ,count(a.order_id) as total_assign_turn
            
            from
                (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type
        
                    from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        
                UNION
            
                SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type
        
                    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                )a
                
                GROUP By 1,2
            )sa on sa.order_id = base1.id and sa.order_type = base1.order_type

LEFT JOIN
(       select 
                order_code  
                ,order_id 
                ,count(case when issue_category = 'Ignore' then order_id else null end) as total_ignore
                ,count(case when issue_category != 'Ignore' then order_id else null end) as total_denied


        from dev_vnfdbi_opsndrivers.phong_raw_assignment_test  

        where 1 = 1 
        -- and order_type = '1. Food/Market'

    group by 1,2            
)ign on ign.order_id = base1.id and base1.order_type = ign.order_code

)


,daily as (
SELECT   base2.created_date
        ,base2.cancel_date
        ,base2.created_hour
        ,base2.city_group
        ,base2.city_name
        ,base2.is_asap
        ,base2.cancel_reason
        -- ,base2.cancel_by
        ,base2.is_canceled
        ,base2.is_del
        ,base2.is_quit
        ,case when order_type = 0 then 'order-delivery' else 'order-spxi' end as order_type
        ,count(distinct base2.uid) cnt_total_order
        ,count(distinct case when base2.order_status = 'Delivered' then base2.uid else null end) as total_net
        ,count(distinct case when base2.shipper_id > 0 and base2.is_del = 1 then base2.uid else null end) cnt_total_order_for_late_calculation
        ,sum(total_assign_turn) as total_assign_turn
        ,sum(total_ignore) as total_ignore 
        ,sum(total_denied) as total_denied

FROM final base2

WHERE 1=1

GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)


SELECT
     created_date AS date_
    ,order_type
    -- ,created_hour
    ,city_group
    -- ,item_range
    ,coalesce(cancel_reason,'non cancel') as cancel_reason
    -- ,if(foody_service = 'Food', 'Food', 'Fresh/Market') as service
    ,sum(if(is_canceled = 1, cnt_total_order, 0))/cast(count(distinct created_date) as double) as total_cancel
    ,sum(if(cancel_reason = 'No Driver', cnt_total_order, 0))/cast(count(distinct created_date) as double) as cnt_cancel_no_driver
    ,sum(cnt_total_order)/cast(count(distinct created_date) as double) as total_submit
    ,sum(total_net)/cast(count(distinct created_date) as double) as total_net
    ,sum(total_assign_turn)/cast(count(distinct created_date) as double) as total_assign_turn
    ,sum(total_ignore)/cast(count(distinct created_date) as double) as total_ignore
    ,sum(total_denied)/cast(count(distinct created_date) as double) as total_denied


FROM daily d


where d.created_date between current_date - interval '14' day and current_date - interval '1' day



group by 1,2,3,4

