with merchant_id as 
(select 
    group_id
    ,count(distinct merchant_id) as count_mex
  from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 
  group by 1
)
,drop_info as 
(SELECT 
        group_id
       ,COUNT(DISTINCT address) as count_drop
FROM 
(SELECT group_id
      ,t.json_
      ,json_extract(t.json_,'$.is_pick') as is_pick 
      ,TRIM(LOWER(CAST(json_extract(t.json_,'$.address') AS VARCHAR))) as address 
FROM
(SELECT 
         id as group_id
        ,CAST(json_extract(extra_data,'$.route') AS array(json))as check
        ,group_status
FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day
) t 
CROSS JOIN UNNEST (check) AS t(json_)
)
WHERE CAST(is_pick AS VARCHAR) = 'false'
GROUP BY 1
)
,check_group as
(SELECT 
         group_id
        ,COUNT(DISTINCT order_code) as total_order_in_group
FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da
WHERE DATE(dt)= current_date - interval '1' day
AND mapping_status = 11 
GROUP BY 1
)
,raw as 
(select bf.order_id
      ,bf.group_id
      ,re.group_code
      ,bf.is_stack_group_order
      ,bf.source as sub_source_v2
      ,coalesce(bf.distance_all,bf.distance) as sum_single_distance
      ,bf.distance 
      ,bf.distance_grp
      ,re.distance/CAST(100000 as DOUBLE) as group_distance
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
      ,cast(json_extract(re.extra_data,'$.re') as double) as re_system
      ,try(round(distance_all,1)*1.00/round(distance_grp,0)) as re_manual
    --   ,case when is_stack_group_order = 2 then try(round(distance_all,1)*1.00/round(distance_grp,0)) else 0 end as re_stack_manual
    --   ,case when is_stack_group_order = 2 then cast(json_extract(re.extra_data,'$.re') as double) else null end as re_stack_system            
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
      ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)* re.distance/CAST(100000 AS DOUBLE) * cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as double) as a_spf_by_stacked_distance
      ,re.extra_data
      ,mi.count_mex
      ,di.count_drop


-- select * 
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) re 
    on re.id = bf.group_id

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = bf.order_id and dot.ref_order_category = 0

left join merchant_id mi 
    on mi.group_id = bf.group_id

left join drop_info di 
    on di.group_id = bf.group_id

where 1 = 1 
and bf.is_stack_group_order = 2 
-- and bf.sub_source_v2 in ('Food','Market')
and bf.status in (7,11)
and bf.date_  between DATE'2023-02-01' AND DATE'2023-02-24'
)
-- SELECT * FROM raw
,metrics as 
(SELECT 
         date_   
        ,raw.group_id 
        ,raw.group_code
        ,sub_source_v2
        ,is_stack_group_order
        ,group_distance
        ,CASE
              WHEN group_distance <= 2 then '1. 0 - 2km'
              WHEN group_distance <= 3 then '2. 2 - 3km'
              WHEN group_distance <= 4 then '3. 3 - 4km'
              WHEN group_distance <= 5 then '4. 4 - 5km'
              WHEN group_distance > 5 then '5. 5km+'
              END AS distance_range
        ,delivered_by
        ,re_system
        ,minfee_system
        ,a_spf_by_stacked_distance as total_group_shipping_fee
        ,total_after_stack_fee as final_group_fee 
        ,GREATEST(a_spf_by_stacked_distance,minfee_system) AS group_fee_exclude_extra
        ,rank
        ,count_mex
        ,count_drop
        ,cg.total_order_in_group
        ,CASE 
              WHEN sub_source_v2 in ('Food','Market') then cg.total_order_in_group *1000
              WHEN sub_source_v2 not in ('Food','Market') then cg.total_order_in_group *500
              END AS extra_fee_current 
        ,(CASE 
              WHEN sub_source_v2 in ('Food','Market') and count_mex <= 1 then 0
              WHEN sub_source_v2 in ('Food','Market') and count_mex > 1 then 1000
              WHEN sub_source_v2 not in ('Food','Market') and count_mex <= 1 then 0
              WHEN sub_source_v2 not in ('Food','Market') and count_mex > 1 then 500
              END) + 
         (CASE 
              WHEN sub_source_v2 in ('Food','Market') and count_drop <= 1 then 0
              WHEN sub_source_v2 in ('Food','Market') and count_drop > 1 then 1000
              WHEN sub_source_v2 not in ('Food','Market') and count_drop <= 1 then 0
              WHEN sub_source_v2 not in ('Food','Market') and count_drop > 1 then 500
              END) AS extra_fee_v2       

FROM raw

LEFT JOIN check_group cg on cg.group_id = raw.group_id

WHERE rank = 1 
)


SELECT
         '1-24Feb' AS period 
        ,sub_source_v2 AS source 
        ,CASE 
              WHEN is_stack_group_order = 1 then 'Group'
              WHEN is_stack_group_order = 2 then 'Stack'
              END AS order_type
        -- ,distance_range
        ,total_order_in_group
        ,COUNT(DISTINCT group_code)/CAST(COUNT(DISTINCT date_) AS DOUBLE) as total_group 
        ,SUM(group_distance)/CAST(COUNT(DISTINCT group_code) AS DOUBLE) AS avg_distance
        ,SUM(re_system)/CAST(COUNT(DISTINCT group_code) AS DOUBLE) AS avg_re
        ,SUM(group_fee_exclude_extra) AS group_fee_exclude_extra
        ,SUM(group_fee_exclude_extra + extra_fee_current)/CAST(COUNT(DISTINCT date_) AS DOUBLE) AS current_group_fee 
        ,SUM(group_fee_exclude_extra + extra_fee_v2)/CAST(COUNT(DISTINCT date_) AS DOUBLE) AS v2_group_fee
        ,SUM(extra_fee_current)/CAST(COUNT(DISTINCT date_) AS DOUBLE) AS extra_fee_current
        ,SUM(extra_fee_v2)/CAST(COUNT(DISTINCT date_) AS DOUBLE) AS extra_fee_v2
        ,COUNT(DISTINCT date_) AS days
        ,SUM(group_fee_exclude_extra)/CAST(SUM(group_fee_exclude_extra + extra_fee_current) AS DOUBLE) as gap_opt1_remove_extra
        ,SUM(group_fee_exclude_extra + extra_fee_v2)/CAST(SUM(group_fee_exclude_extra + extra_fee_current) AS DOUBLE) as gap_opt2_adjust_extra

FROM metrics
WHERE sub_source_v2 != 'Market'
GROUP BY 1,2,3,4