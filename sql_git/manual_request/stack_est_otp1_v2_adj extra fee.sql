with group_order_base as
(select 
    group_id
    ,count(distinct order_id) as order_in_group
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 
group by 1
)
,merchant_id as 
(select group_id
        ,count(distinct restaurant_id) as count_mex
    
from    
(select oct.id 
       ,oct.restaurant_id
       ,grp.group_id 



from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct 

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) grp on oct.id = grp.ref_order_id and grp.ref_order_category = 0 

where date(from_unixtime(oct.submit_time - 3600)) between current_date - interval '45' day and current_date - interval '1' day 
)
group by 1

)

,raw as 
(select 
       cast(bf.order_id as bigint) as order_id
      ,2000 as extra_fee
      ,bf.group_id
      ,mi.count_mex 
      ,re.group_code
      ,bf.city_name_full
      ,bf.is_stack_group_order
      ,bf.sub_source_v2
      ,coalesce(bf.distance_all,bf.distance) as sum_single_distance
      ,bf.distance 
      ,bf.distance_grp as stack_distance
      ,bf.status
      ,bf.exchange_rate
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
      ,delivered_by
      ,case when is_stack_group_order = 2 then try(round(distance_all,1)*1.00/round(distance_grp,0)) else 0 end as re_stack_manual
      ,case when is_stack_group_order = 2 then json_extract(re.extra_data,'$.re') else null end as re_stack_system 
      ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as bigint) as surge_rate
      ,cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as bigint) as unit_fee
      ,bf.distance_grp
        ,cast(json_extract(re.extra_data,'$.ship_fee_info.min_fee') as bigint) as min_group_shipping_fee
      ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as bigint)*bf.distance_grp*cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as bigint) as spf_by_stacked_distance
      ,bf.date_
      ,extract(hour from from_unixtime(dot.submitted_time - 3600)) as created_hour
      ,bf.total_shipping_fee_basic as single_shipping_fee
      ,sum(case when grp.order_in_group = 1 then total_shipping_fee else driver_cost_base_n_surge end) over(partition by bf.group_id order by bf.date_ desc ) as total_after_stack_fee
      ,sum(bf.total_shipping_fee_surge + bf.total_shipping_fee_basic) over(partition by bf.group_id order by bf.date_ desc ) as total_before_stack_fee
      ,row_number()over(partition by bf.group_id order by bf.order_id desc ) as rank
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) re on re.id = bf.group_id

-- select * from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day)

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = bf.order_id and dot.ref_order_category = 0
left join group_order_base grp
    on bf.group_id = grp.group_id

left join merchant_id mi on mi.group_id = bf.group_id

where 1 = 1 
and bf.sub_source_v2 in ('Food','Market')

and bf.status in (7) --order completed

and bf.is_stack_group_order <> 1 

and bf.date_ between current_date - interval '30' day and current_date - interval '1' day 

-- and bf.group_id = 24979951
-- and re.group_code = 'D94646476940'
-- group by 1,2,3,4,5,6,7
)
-- select 
--     *
-- from raw
,final as 
(select 


         date_
        ,group_code 
        ,exchange_rate
        ,distance_range_before_stack
        ,delivered_by
        ,city_name_full

        ,case when is_stack_group_order = 2  and rank  = 1 and delivered_by != 'hub' 
              then total_before_stack_fee else 0 end as sum_single_fee 

        ,case when is_stack_group_order = 2  and rank  = 1 and delivered_by != 'hub' 
              then total_after_stack_fee  else 0 end as current_stack_fee

        ,case when is_stack_group_order = 2  and rank  = 1 and delivered_by != 'hub' 
              then total_before_stack_fee - total_after_stack_fee else 0 end as current_saving_stack

        -- ,case when is_stack_group_order = 2  and rank  = 1 and delivered_by != 'hub'  
        --       then least(cast(total_before_stack_fee as bigint),cast(total_ship_fee_ab as bigint)) else 0 end as new_stack_fee_v1

        ,spf_by_stacked_distance
        ,total_before_stack_fee
        ,min_group_shipping_fee
        --new formula
        ,case when is_stack_group_order = 2  and rank  = 1 and delivered_by != 'hub'  
            --   then greatest( cast(total_before_stack_fee as bigint),greatest(cast(total_ship_fee_ab as bigint),cast(max_a_b as bigint)) ) 
              then greatest(least(spf_by_stacked_distance,total_before_stack_fee),min_group_shipping_fee) + IF(count_mex = 1,0,count_mex*1000)      
              else 0 
              end as new_stack_fee_v2            

        ,rank
        -- ,count(distinct restaurant_id) as count_mex
        ,count(distinct order_id) as total_order
        ,count(distinct case when is_stack_group_order = 2 then order_id else null end) as stack_order
        ,sum(distinct case when is_stack_group_order = 2 and rank = 1 then IF(count_mex = 1,0,count_mex*1000) else null end) as extra_fee




        from raw 

-- where rank = 1  
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

-- select * from final

select date_
    --   ,group_code
      ,distance_range_before_stack
    --   ,delivered_by
      
      ,sum(total_order) as total_del 
      ,sum(stack_order) as stack_del  
    --   fee
      ,sum(sum_single_fee)*1.00/exchange_rate as sum_single_fee
      ,sum(current_stack_fee)*1.00/exchange_rate as current_stack_fee
    --   ,sum(new_stack_fee_v1)*1.00/exchange_rate as new_stack_fee_v1
      ,sum(new_stack_fee_v2)*1.00/exchange_rate as new_stack_fee_v2
    --   saving
      ,sum(current_saving_stack)*1.00/exchange_rate as current_saving_stack
    --   ,sum(sum_single_fee - new_stack_fee_v1)*1.00/23181 as estimate_saving_stack_v1
      ,(sum(sum_single_fee - new_stack_fee_v2))*1.00/exchange_rate as estimate_saving_stack_v2
    --   ,sum(extra_fee) as extra_fee


from final 

where 1 = 1 

-- and date_ between current_date - interval '30' day and current_date - interval '1' day 

group by 1,2,exchange_rate

