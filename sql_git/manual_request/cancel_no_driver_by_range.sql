with base as 
(select * from dev_vnfdbi_opsndrivers.food_raw_phong
        )
,final as 
(
SELECT   base1.id
        ,base1.uid
        ,base1.cancel_date
        ,base1.inflow_date
        ,base1.created_date
        ,base1.inflow_hour
        ,base1.shipper_id
        ,base1.city_group
        ,base1.district_name
        ,base1.city_name
        ,base1.is_asap
        ,base1.order_status
        ,base1.foody_service
        ,base1.item_range
        ,base1.total_amount_range
        ,base1.distance_range
        ,case when trim(base1.cancel_reason) = 'Shop closed' then (case when po.is_pre_order> 0 then 'Pre-order' else 'Shop closed' end)
              else base1.cancel_reason end as cancel_reason

        ,case when order_status = 'Cancelled' then 1 else 0 end as is_canceled
        ,case when order_status = 'Delivered' then 1 else 0 end as is_del
        ,case when order_status = 'Quit' then 1 else 0 end as is_quit
FROM
(
SELECT
         base.id
        ,base.uid
        ,case when total_item <= 10 then '1. 0 - 10 items'
              when total_item <= 20 then '2. 10 - 20 items'
              when total_item <= 30 then '3. 20 - 30 items'
              when total_item <= 40 then '4. 30 - 40 items'
              when total_item > 40 then '5. > 40 items'

              end as item_range

        ,case when total_amount <= 100000 then '1. 0 - 100,000 vnd'
              when total_amount <= 200000 then '2. 100,000 - 200,000 vnd' 
              when total_amount <= 400000 then '3. 200,000 - 400,000 vnd'
              when total_amount <= 500000 then '4. 400,000 - 500,000 vnd' 
              when total_amount > 500000 then '5. > 500,000 vnd'   
              end as total_amount_range

        ,case when distance <= 3 then '1. 0 - 3km'
              when distance <= 5 then '2. 3 - 5km'
              when distance <= 10 then '3. 5 - 10km'
              when distance > 10 then '4. > 10km' end as distance_range

        ,base.cancel_date
        ,base.created_date
        ,date(inflow_timestamp) inflow_date
        ,extract(hour from inflow_timestamp) as inflow_hour
        ,base.shipper_id
        ,base.city_group
        ,base.city_name
        ,base.is_asap
        ,base.order_status
        ,base.foody_service
        ,base.cancel_reason
        ,base.district_name
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
ON base1.id = po.id




)




,daily as (
SELECT   base2.created_date
        ,base2.cancel_date
        ,base2.inflow_date
        ,base2.inflow_hour as created_hour
        ,base2.city_group
        ,base2.item_range
        ,base2.total_amount_range
        ,base2.distance_range
        ,base2.district_name
        ,base2.city_name
        ,base2.is_asap
        ,base2.foody_service
        ,base2.cancel_reason
        ,case when base2.cancel_reason is null then null
              when base2.cancel_reason in ('No driver') then '4. Driver'
              when base2.cancel_reason in ('Out of stock', 'Shop closed','Shop busy','Shop did not confirm','Wrong price') then '3. Merchant'
              when base2.cancel_reason in ('Pending status from bank') then '5. System'
              when base2.cancel_reason in ('Payment failed') then '2. Buyer System'
              when base2.cancel_reason in ('Affected by quarantine area','Order limit due to Covid') then '6. Others'
              else '1. Buyer Voluntary' end as cancel_by
        ,case when base2.cancel_reason is null then null
              when base2.cancel_reason in ('No driver') then '2. Buyer Non-voluntary Cancellation'
              when base2.cancel_reason in ('Out of stock', 'Shop closed','Shop busy','Shop did not confirm','Wrong price') then '2. Buyer Non-voluntary Cancellation'
              when base2.cancel_reason in ('Pending status from bank') then '2. Buyer Non-voluntary Cancellation'
              when base2.cancel_reason in ('Payment failed') then '2. Buyer Non-voluntary Cancellation'
              when base2.cancel_reason in ('Affected by quarantine area','Order limit due to Covid') then '2. Buyer Non-voluntary Cancellation'
              else '1. Buyer Voluntary Cancellation' end as cancel_type
        ,base2.is_canceled
        ,base2.is_del
        ,count(distinct base2.uid) cnt_total_order
        ,count(distinct case when base2.order_status = 'Delivered' then base2.uid else null end) as total_net
        ,count(distinct case when base2.shipper_id > 0 and base2.is_del = 1 then base2.uid else null end) cnt_total_order_for_late_calculation

FROM final base2

WHERE 1=1

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)


SELECT
    -- inflow_date AS date_
    -- ,created_hour
     city_group
    ,item_range
    ,total_amount_range
    ,distance_range
    -- ,if(foody_service = 'Food', 'Food', 'Fresh/Market') as service
    ,sum(if(cancel_reason = 'No driver' and is_canceled = 1, cnt_total_order, 0))/cast(count(distinct inflow_date) as double) as cnt_cancel_no_driver
    ,sum(cnt_total_order)/cast(count(distinct inflow_date) as double) as total_submit
    ,sum(total_net)/cast(count(distinct inflow_date) as double) as total_net


FROM daily d


where d.inflow_date between current_date - interval '7' day and current_date - interval '1' day



group by 1,2,3,4

