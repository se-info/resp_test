with order_raw as 
(SELECT 
         id
        ,ref_order_id
        ,ref_order_code
        ,ref_order_category 
        ,delivery_distance/cast(1000 as double) as distance
        ,hub_id 
        ,pick_hub_id
        ,drop_hub_id
        ,real_pick_hub_id
        ,real_drop_hub_id

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

left join 
            (
            SELECT 
                    order_id
                    ,cast(json_extract(order_data,'$.shipper_policy.type') as bigint) AS driver_payment_policy 
                    ,cast(json_extract(order_data,'$.hub_id') as bigint) as hub_id 
                    ,cast(json_extract(order_data,'$.pick_hub_id') as bigint) as pick_hub_id 
                    ,cast(json_extract(order_data,'$.drop_hub_id') as bigint) as drop_hub_id 
                    ,cast(json_extract(order_data,'$.real_pick_hub_id') as bigint) as real_pick_hub_id 
                    ,cast(json_extract(order_data,'$.real_drop_hub_id') as bigint) as real_drop_hub_id 

            FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day)

            )dotet ON dot.id = dotet.order_id

WHERE date(from_unixtime(submitted_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day

-- and ref_order_code = '21092-454437473'
)
,metrics as 
(select 
        ref_id as order_id 
       ,issue_reason
       ,case when od.hub_id > 0 then 1 else 0 end as is_hub_qualified
       ,case when (od.pick_hub_id > 0  and pick_hub_id = drop_hub_id  or od.real_pick_hub_id > 0  and real_pick_hub_id = real_drop_hub_id )
            then '1. Pick and Drop in Hub'
            when (od.pick_hub_id > 0  and pick_hub_id <> coalesce(drop_hub_id,0) or od.real_pick_hub_id > 0  and real_pick_hub_id <> coalesce(real_drop_hub_id,0)) 
            then  '2. Pick in Hub and Drop out Hub '
            else null end as pick_drop_rule
        ,case when distance <= 3 then 1 else 0 end as is_less_than_3km    
        ,case when sm.shipper_type_id = 12 then 1 else 0 end as is_hub_driver
        ,now_uid
        ,date(start_at) as report_date

from dev_vnfdbi_opsndrivers.snp_foody_sf_chat_new a 

left join order_raw od on od.ref_order_code = a.ref_id and od.ref_order_category = 0

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = cast(a.now_uid as bigint) and try_cast(sm.grass_date as date) = date(start_at)


where issue_reason like '%Wrong assigned rule for HUB Driver%'
and service in ('Food','NowMarket')

and date(start_at) between current_date - interval '1' day and current_date - interval '1' day)

select 
        report_date 
       ,issue_reason
       ,pick_drop_rule
       ,is_less_than_3km
       ,is_hub_qualified
       ,is_hub_driver
       ,count(order_id) as total_cases  


from metrics 


group by 1,2,3,4,5,6
