with raw as 
(select bf.order_id
      ,bf.group_id
      ,bf.is_stack_group_order
      ,bf.sub_source_v2
      ,coalesce(bf.distance_all,distance) as sum_single_distance
      ,bf.distance_grp as stack_distance
      ,case when coalesce(bf.distance_all,distance) <= 1 then '1. 0 - 1km'
            when coalesce(bf.distance_all,distance) <= 2 then '2. 1 - 2km'
            when coalesce(bf.distance_all,distance) <= 3 then '3. 2 - 3km'
            when coalesce(bf.distance_all,distance) <= 4 then '4. 3 - 4km'
            when coalesce(bf.distance_all,distance) <= 5 then '5. 4 - 5km'
            when coalesce(bf.distance_all,distance) <= 6 then '6. 5 - 6km'
            when coalesce(bf.distance_all,distance) <= 7 then '7. 6 - 7km'
            when coalesce(bf.distance_all,distance) <= 8 then '8. 7 - 8km'
            when coalesce(bf.distance_all,distance) <= 9 then '9. 8 - 9km'
            when coalesce(bf.distance_all,distance) <= 10 then '10. 9 - 10km'
            when coalesce(bf.distance_all,distance) > 10 then '11. > 10km'
            end as distance_range_before_stack
      ,delivered_by            

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
      ,sum(bf.driver_cost_base) over(partition by bf.group_id order by bf.date_ desc ) as total_after_stack_fee
      ,sum(bf.total_shipping_fee_basic) over(partition by bf.group_id order by bf.date_ desc ) as total_before_stack_fee
      ,row_number()over(partition by bf.group_id order by bf.order_id desc ) as rank



-- select * 
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = bf.order_id and dot.ref_order_category = 0

where 1 = 1 
and bf.sub_source_v2 in ('Food','Market')

and bf.status in (7) --order completed

and bf.is_stack_group_order <> 1  

and bf.date_ between current_date - interval '30' day and current_date - interval '1' day

-- and bf.group_id = 314801090

-- group by 1,2,3,4,5,6,7
)

,final as 
(
select  
        date_ 
       ,year(date_)*100 + week(date_) as year_week
       ,created_hour
       ,order_id
       ,distance_range_before_stack
       ,sum_single_distance
       ,is_stack_group_order
       ,case when is_stack_group_order = 2 and rank = 1 and delivered_by != 'hub' and (total_before_stack_fee - total_after_stack_fee) < 0 then 1 else 0 end as over_single_fee
       ,case when is_stack_group_order = 2  and rank  = 1 and delivered_by != 'hub' and distance_range_before_stack not in ('3. 2 - 3km','2. 1 - 2km','1. 0 - 1km')
                  then total_before_stack_fee - total_after_stack_fee 
             else 0 end as gap_stack_single

from raw

-- where distance_range_before_stack is null  

)


select  year_week
    --    ,created_hour 
       ,distance_range_before_stack
    --    ,over_single_fee
       ,count(distinct order_id) as total_del 
       ,count(distinct case when is_stack_group_order = 2 then order_id else null end) as total_stack_del
       ,sum(gap_stack_single)*1.00/23181 as gap_stack_single
       ,over_single_fee

 from final 


 group by 1,2,6