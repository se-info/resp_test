with group_order_base as
(select 
    group_id
    ,count(distinct order_id) as order_in_group
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 
group by 1
)
,order_ab as 
(select  a.id 
        ,'order-A' as type_order
        ,cast(json_extract(extra_data,'$.route[0].order_id') as bigint) as order_id
        ,dot.ref_order_id
        ,cast(json_extract(extra_data,'$.re') as bigint) as re_

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) a 

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.id = cast(json_extract(extra_data,'$.route[0].order_id') as bigint)

UNION ALL

select  a.id 
        ,'order-B' as type_order
        ,cast(json_extract(extra_data,'$.route[1].order_id') as bigint) as order_id
        ,dot.ref_order_id
        ,cast(json_extract(extra_data,'$.re') as bigint) as re_

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) a
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.id = cast(json_extract(extra_data,'$.route[1].order_id') as bigint)


)
,raw as 
(select 
       cast(bf.order_id as bigint) as order_id
      ,2000 as extra_fee
      ,bf.group_id
      ,re.group_code
      ,bf.date_      
      ,bf.city_name_full
      ,bf.is_stack_group_order
      ,bf.sub_source_v2
      ,bf.distance 
      ,coalesce(bf.distance_all,bf.distance) as sum_single_distance       
      ,bf.distance_grp as stack_distance
      ,bf.status
      ,bf.exchange_rate
      ,(bf.total_shipping_fee_basic + bf.total_shipping_fee_surge) as single_fee


      ,case when coalesce(bf.distance_all,bf.distance) <= 1 then '1. 0 - 1km'
            when coalesce(bf.distance_all,bf.distance) <= 2 then '2. 1 - 2km'
            when coalesce(bf.distance_all,bf.distance) <= 3 then '3. 2 - 3km'
            when coalesce(bf.distance_all,bf.distance) <= 4 then '4. 3 - 4km'
            when coalesce(bf.distance_all,bf.distance) <= 5 then '5. 4 - 5km'
            when coalesce(bf.distance_all,bf.distance) <= 6 then '6. 5 - 6km'
            when coalesce(bf.distance_all,bf.distance) <= 7 then '7. 6 - 7km'
            when coalesce(bf.distance_all,bf.distance) <= 8 then '8. 7 - 8km'
            when coalesce(bf.distance_all,bf.distance) <= 9 then '9. 8 - 9km'
            when coalesce(bf.distance_all,bf.distance) <= 10 then '10. 9 - 10km'
            when coalesce(bf.distance_all,bf.distance) > 10 then '11. > 10km'
            end as distance_range_before_stack

      ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as bigint)*bf.distance_grp*cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as bigint) as spf_by_stacked_distance
      ,cast(json_extract(re.extra_data,'$.re') as double) as RE
      ,case when grp.order_in_group = 1 then 0 else 1 end as is_qualified_stack 

      ,coalesce(max.type_order,'order-single') as order_type
      
      ,extract(hour from from_unixtime(dot.submitted_time - 3600)) as created_hour

      ,sum(case when grp.order_in_group = 1 then total_shipping_fee else driver_cost_base_n_surge end) over(partition by bf.group_id order by bf.date_ desc ) as stack_fee_current

      ,row_number()over(partition by bf.group_id order by bf.order_id desc ) as rank

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 

left join order_ab max on max.ref_order_id = cast(bf.order_id as bigint)

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) re on re.id = bf.group_id

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = bf.order_id and dot.ref_order_category = 0
left join group_order_base grp
    on bf.group_id = grp.group_id


where 1 = 1 
and bf.sub_source_v2 in ('Food','Market')

and bf.status in (7) --order completed

and bf.is_stack_group_order <> 1 

and bf.date_ between current_date - interval '1' day and current_date - interval '1' day
)

-- input rate A & B
,rate(order_type,rate) 
as (
values 
 ('order-A',1)
,('order-B',1)

)
,metrics as 
(select
          coalesce(group_code,'order-single') as group_code
         ,date_ 
         ,city_name_full as city_name  
         ,is_stack_group_order
         ,is_qualified_stack          

         ,count(distinct order_id) as total_del

         ,sum(case when rank = 1 then RE else null end) as RE         

         ,sum( case when a.order_type = 'order-A' then single_fee/cast(exchange_rate as double) else 0 end) as single_fee_a 
         ,sum( case when a.order_type = 'order-B' then single_fee/cast(exchange_rate as double) else 0 end) as single_fee_b

         ,sum(case when a.order_type = 'order-A' then rate.rate else 0 end) as rate_a
         ,sum(case when a.order_type = 'order-B' then rate.rate else 0 end) as rate_b

         ,sum(single_fee/cast(exchange_rate as double)) as single_shipping_fee    
         ,sum(case when rank = 1 then spf_by_stacked_distance/cast(exchange_rate as double) else null end) as spf_by_stacked_distance
         ,sum(case when rank = 1 then stack_fee_current/cast(exchange_rate as double) else null end) as stack_fee_current 

from raw a 

left join rate on rate.order_type = a.order_type

-- where group_code = 'D97301497661'

group by 1,2,3,4,5
)

,final as 
(select 
        group_code
       ,date_ 
       ,city_name  
       ,case when group_code <> 'order-single' then greatest(fee_A,fee_B) else 0 end as min_group_shipping_fee
       ,spf_by_stacked_distance
       ,case when city_name in ('Quang Nam','Quang Ninh','Lam Dong') and group_code <> 'order-single'
        then 
        greatest(greatest(fee_A,fee_B),spf_by_stacked_distance) 
        when city_name not in ('Quang Nam','Quang Ninh','Lam Dong') and group_code <> 'order-single'
        then 
        stack_fee_current 
        else 
        0
        end as stack_fee_est
       ,case when group_code = 'order-single' then single_shipping_fee else 0 end as single_shipping_fee
       ,stack_fee_current
       ,total_del
    
        
from
    (select  
        group_code
       ,date_
       ,city_name
       ,RE 
       ,IF(group_code <> 'order-single',(single_fee_a + (single_fee_b/RE)) * rate_a , 0) as fee_A
       ,IF(group_code <> 'order-single',(single_fee_b + (single_fee_a/RE)) * rate_b , 0) as fee_B
       ,case when is_qualified_stack = 1 and is_stack_group_order = 2 then spf_by_stacked_distance else 0 end as spf_by_stacked_distance
       ,case when is_qualified_stack = 1 and is_stack_group_order = 2 then stack_fee_current else 0 end as stack_fee_current
       ,case when is_stack_group_order <> 2 then single_shipping_fee else 0 end as single_shipping_fee
       ,total_del




from metrics)



)
-- select * from final where city_name in ('Quang Nam','Quang Ninh','Lam Dong')

select   
        -- date_ 
         city_name  
        -- order
        ,sum(total_del)/cast(count(distinct date_) as double) as total_del 
        ,sum(case when group_code <> 'order-single' then total_del else 0 end )/cast(count(distinct date_) as double) as total_stack_del
        -- current fee 
        ,sum(single_shipping_fee + stack_fee_current)/cast(count(distinct date_) as double) as total_shipping_fee_usd_current  
        ,sum(case when group_code <> 'order-single' then stack_fee_current else 0 end)/cast(count(distinct date_) as double) as total_stack_fee_usd_current
        -- estimate fee 
        ,sum(single_shipping_fee + stack_fee_est)/cast(count(distinct date_) as double) as total_shipping_fee_usd_est
        ,sum(case when group_code <> 'order-single' then stack_fee_est else 0 end)/cast(count(distinct date_) as double) as total_stack_fee_usd_est



from final 

where city_name in ('Quang Nam','Quang Ninh','Lam Dong')

group by 1

