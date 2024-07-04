with raw as 
(select bf.order_id
      ,bf.group_id
      ,re.group_code
      ,bf.is_stack_group_order
      ,bf.sub_source_v2
      ,coalesce(bf.distance_all,bf.distance) as sum_single_distance
      ,bf.distance 
      ,bf.distance_grp as stack_distance
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
      ,case when is_stack_group_order = 2 then cast(json_extract(re.extra_data,'$.re') as double) else null end as re_stack_system            

    --   ,case when distance_grp > distance_all 
    --              and sum(driver_cost_base) over(partition by group_id order by date_ desc) > sum(total_shipping_fee_basic) over(partition by group_id order by date_ desc)
    --              and shipper_type_id <> 12 
    --                  then 1 
    --                  else 0 
    --                  end as need_adjust

      ,bf.date_
      ,extract(hour from from_unixtime(dot.submitted_time - 3600)) as created_hour
    --   ,driver_cost_base
      ,bf.total_shipping_fee_basic as single_shipping_fee
      ,sum(bf.driver_cost_base_n_surge) over(partition by bf.group_id order by bf.date_ desc ) as total_after_stack_fee_manual
    --   ,re.ship_fee*1.00/100 as total_after_stack_fee
      ,sum(bf.total_shipping_fee_surge + bf.total_shipping_fee_basic) over(partition by bf.group_id order by bf.date_ desc ) as total_before_stack_fee
      ,re.ship_fee/cast(100 as double) as total_after_stack_fee
      ,ROUND(cast(json_extract(re.extra_data,'$.ship_fee_info.min_fee') as double),0) as minfee_system
      ,row_number()over(partition by bf.group_id order by bf.order_id desc ) as rank
      ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)* bf.distance_grp * cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as double) as a_spf_by_stacked_distance



-- select * 
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) re on re.id = bf.group_id
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = bf.order_id and dot.ref_order_category = 0

where 1 = 1 
and bf.sub_source_v2 in ('Food','Market')

and bf.status in (7) --order completed

and bf.is_stack_group_order <> 1 

and bf.date_  between current_date - interval '30' day and current_date - interval '1' day

)
,final_metrics as 
(select
        date_
       ,group_id
       ,group_code 
       ,delivered_by
       ,case when group_id is not null then 'Stacked' else 'Single' end as order_type
       ,sum(case when rank = 1 then re_stack_system else null end) re_stack_system 
       ,sum(case when rank = 1 then total_before_stack_fee else null end) as total_before_stack_fee
       ,sum(case when rank = 1 then total_after_stack_fee else null end) as total_after_stack_fee
       ,sum(case when rank = 1 then total_after_stack_fee - 2000 else null end) as total_stack_fee_excl_extra
       ,sum(case when rank = 1 then total_before_stack_fee - total_after_stack_fee else null end) as saving_stack
       ,sum(case when rank = 1 then minfee_system else null end) as minfee_system
       ,sum(case when rank = 1 then round(a_spf_by_stacked_distance,0) else null end) as ship_fee 
       ,count(distinct order_id ) as total_order


from raw 


where 1 = 1 

-- and ( group_id is null or (case when group_id is not null then rank = 1 end) )
and is_stack_group_order = 2 

group by 1,2,3,4
)
-- select * from final_metrics 	
select 
        date_
       ,order_type 
       ,case when re_stack_system <= 1.05 then '1. <= 1.05'
             when re_stack_system <= 1.1 then '2. <= 1.1'
             when re_stack_system <= 1.15 then '3. <= 1.15'
             when re_stack_system <= 1.2 then '4. <= 1.2'
             when re_stack_system <= 1.25 then '5. <= 1.25'
             when re_stack_system <= 1.3 then '6. <= 1.3'
             when re_stack_system <= 1.35 then '7. <= 1.35'
             when re_stack_system <= 1.4 then '8. <= 1.4'
             when re_stack_system <= 1.45 then '9. <= 1.45'
             when re_stack_system <= 1.5 then '10. <= 1.5'
             when re_stack_system > 1.5 then '11. > 1.5'
             end as re_range   
       ,delivered_by
       ,case when (total_after_stack_fee - 2000) = ship_fee then 'ship_fee'
                             else 'min_fee' end as dist_stack_fee
    --    ,case when is_stack_group_order = 2 and rank = 1 
    --               then (case when (total_after_stack_fee - 2000) = round(a_spf_by_stacked_distance,0) then 'ship_fee'
    --                          else 'min_fee' end) else null end as dist_stack_fee
       ,sum(total_order) as total_orders
       ,sum(re_stack_system*total_order)/cast(sum(total_order) as double) as avg_re        
       ,case when delivered_by = 'hub' then 0
             else 1 - (sum(total_after_stack_fee)/cast(sum(total_before_stack_fee) as double)) end as pct_saving 

FROM final_metrics


group by 1,2,3,4,5

UNION ALL

select 
        date_
       ,order_type 
       ,'All' re_range   
       ,delivered_by
       ,case when (total_after_stack_fee - 2000) = ship_fee then 'ship_fee'
                             else 'min_fee' end as dist_stack_fee
    --    ,case when is_stack_group_order = 2 and rank = 1 
    --               then (case when (total_after_stack_fee - 2000) = round(a_spf_by_stacked_distance,0) then 'ship_fee'
    --                          else 'min_fee' end) else null end as dist_stack_fee
       ,sum(total_order) as total_orders
       ,sum(re_stack_system*total_order)/cast(sum(total_order) as double) as avg_re        
       ,case when delivered_by = 'hub' then 0
             else 1 - (sum(total_after_stack_fee)/cast(sum(total_before_stack_fee) as double)) end as pct_saving 

FROM final_metrics

group by 1,2,3,4,5
