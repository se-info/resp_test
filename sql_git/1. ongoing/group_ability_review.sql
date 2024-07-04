with group_info_tab as (
      select 
            id as group_id
            , group_code
            , ref_order_category
            , if(ref_order_category=0,'Delivery','SPXI') as source
            , distance * 1.00 / 100000 as group_distance
            , ship_fee * 1.00 / 100 as group_fee
            , uid as shipper_uid
            , group_status
            , create_time
            , cast(json_extract(extra_data, '$.re') as double) AS re 
            , cast(json_extract(extra_data, '$.pick_city_id') as int) AS city_id
            , cast(json_extract(extra_data, '$.distance_matrix.data') as array<array<double>>) as distance_matrix
            -- if group_id is stack, 
            , cast(json_extract(extra_data, '$.distance_matrix.data') as array<array<double>>)[1][3] as stack_merchant_distance
            --if group_id is group, 
            , cast(json_extract(extra_data, '$.distance_matrix.data') as array<array<double>>)[1][2] as group_merchant_distance
            , JSON_ARRAY_LENGTH(json_extract(extra_data, '$.distance_matrix.data'))/2 as total_order_in_group

      from shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live
      -- where date(from_unixtime(create_time - 3600)) = current_date - interval '1' day
)
,f as 
(select 
      g.*,
      r.order_assign_type,
      r.sum_weight,
      r.total_cod

from group_info_tab as g 

left join (select group_id,max_by(order_assign_type,id) as order_assign_type,sum(weight) as sum_weight,sum(cod_value) as total_cod
            from
            (select r.*,doet.cod_value  
            from driver_ops_raw_order_tab r 
            left join (select order_id,cast(json_extract(order_data,'$.shopee.cod_value') as bigint) as cod_value 
            from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on doet.order_id = r.delivery_id
            )
            where group_id > 0 
            group by 1 ) r 
      on g.group_id = r.group_id
      and r.group_id > 0 
where 1 = 1
and r.group_id is not null 
)
-- select * from f where date(from_unixtime(create_time - 3600)) = current_date - interval '1' day
select
      date_trunc('month',date(from_unixtime(create_time - 3600))) as "created",
      "source",
      avg(case when order_assign_type = 'Group' then "group_distance" else null end) as "avg_group_disance",
      avg(case when order_assign_type != 'Group' then "group_distance" else null end) as "avg_stack_distance",
      avg(case when order_assign_type = 'Group' then "group_merchant_distance"*1.00/1000 else null end) as  "avg_group_merchant_distance",
      avg(case when order_assign_type != 'Group' then "stack_merchant_distance"*1.00/1000 else null end) as  "avg_stack_merchant_distance",
      avg(case when order_assign_type = 'Group' then "total_order_in_group" else null end) as "avg_parcel_in_group",
      avg(case when order_assign_type = 'Group' then "sum_weight"*1.00/1000 else null end) as "avg_weight_in_group",
      avg(case when order_assign_type = 'Group' then "re" else null end)      as "avg_re_in_group",
      avg(case when order_assign_type != 'Group' then "re" else null end) as "avg_re_in_stack"

from f 
where date(from_unixtime(create_time - 3600)) between date'2024-03-01' and date'2024-03-31'
group by 1,2 

