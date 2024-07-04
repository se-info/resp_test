with group_info as 
(SELECT 
         group_id 
        ,group_code
        ,ref_order_category
        ,MAX(final_stack_fee) AS current_group_fee
        ,MAX(final_stack_fee)/MAX(rank_order) AS group_fee_allocate_current 
        ,MAX(rank_order) AS total_order_in_group
        ,ROUND(CASE WHEN MAX(rank_order) >= 2 THEN 
        MAX(final_stack_fee) - (CASE WHEN MAX(rank_order) = 2 THEN 
         ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  ))
              WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))   
              ELSE MAX(final_stack_fee) END) END) AS extra_fee
        
        ,ROUND(CASE WHEN MAX(rank_order) >= 2 THEN 
        MAX(final_stack_fee) - (CASE WHEN MAX(rank_order) = 2 THEN 
         ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  ))
              WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))   
              ELSE MAX(final_stack_fee) END) END)/MAX(rank_order) AS extra_fee_allocate

        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_cal) + (MAX(fee_2_cal)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_cal) + (MAX(fee_1_cal)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_fee),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_current

        ,ROUND(CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_cal) + (MAX(fee_2_cal)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_cal) + (MAX(fee_1_cal)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_fee),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END )/MAX(rank_order) AS group_fee_current_allocate         

        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_est) + (MAX(fee_2_est)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_est) + (MAX(fee_1_est)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_est),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_est)*IF(group_category=0,1,0.7),LEAST(SUM(single_est),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_est

        ,ROUND(CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_est) + (MAX(fee_2_est)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_est) + (MAX(fee_1_est)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_est),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_est)*IF(group_category=0,1,0.7),LEAST(SUM(single_est),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END)/MAX(rank_order) AS group_fee_est_allocate

FROM dev_vnfdbi_opsndrivers.group_order_info_raw 
-- where group_code = 'D37594799808'
GROUP BY 1,2,3,group_category
)
,bill_fee_order_base as
(select 
    *
    ,first_value(city_name_full) over (partition by grass_date,partner_id order by order_id asc) as first_city_name_full
    

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level
where 1=1
and source in ('Food','Market')
-- and status = 7
and grass_date between date '2023-10-01' and date '2023-10-25'
and coalesce(city_name_full,'na') not in ('na','TestCity','Dien Bien')
)
,driver_cost_raw as 
(select 
    case 
        when first_city_name_full in ('HCM City','Ha Noi City' ,'Da Nang City') then first_city_name_full
        when first_city_name_full IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') then 'T2'
        else 'T3' end as city_group
    ,(CASE 
        -- WHEN is_nan(bfo.bonus) = true THEN 0.00 
        WHEN bfo.delivered_by = 'hub' THEN bonus_hub
        WHEN bfo.delivered_by != 'hub' THEN bonus_non_hub
        ELSE null end)*1.000000   as bonus_usd_all

        , (driver_cost_base + bfo.return_fee_share_basic)*1.000000   as total_driver_cost_base_all
        , (driver_cost_surge + bfo.return_fee_share_surge)*1.000000   as total_driver_cost_surge_all
        ,case 
        when bfo.delivered_by != 'hub' and bfo.group_id > 0 then (gi.group_fee_est_allocate)    
        when bfo.delivered_by != 'hub' and bfo.group_id is null then   
            (greatest(13500,(case 
            when bfo.distance <= 3 then 3650
            when bfo.distance <= 4 then 3850
            when bfo.distance <= 5 then 3950
            when bfo.distance > 5 then 4000 end) * distance) + (driver_cost_surge + bfo.return_fee_share_surge)) *1.000000
        else driver_cost_base end             
        as total_driver_cost_base_surge_est
        
        ,(CASE 
        -- WHEN is_nan(bfo.bonus_v2) = true THEN 0.00 
        WHEN bfo.delivered_by = 'hub' THEN bonus_hub_v2
        WHEN bfo.delivered_by != 'hub' THEN bonus_non_hub_v2
        ELSE null end)*1.000000   as bonus_usd_all_v2
        , (driver_cost_base_v2 + bfo.return_fee_share_basic)*1.000000   as total_driver_cost_base_all_v2
        , (driver_cost_surge_v2 + bfo.return_fee_share_surge)*1.000000   as total_driver_cost_surge_all_v2
            -- ,delivered_by
        ,case 
             
            when city_name_full in ('HCM City', 'Ha Noi City') then 
            (case
            when delivered_by = 'hub' and (new_driver_tier_v2 is null or new_driver_tier_v2 = 'HUB_OTH') then 'HUB03'
            when delivered_by = 'hub' and new_driver_tier_v2 in ('HUB10','HUB08') then 'HUB8|HUB10'
            when delivered_by = 'hub' and new_driver_tier_v2 is not null then new_driver_tier_v2
            when delivered_by != 'hub' and lower(new_driver_tier_v2) like '%hub%' then 'T1' --- outshift
            when delivered_by != 'hub' and new_driver_tier_v2 is null then 'T1'
            when delivered_by != 'hub' and new_driver_tier_v2 = 'part_time' then 'T1'
            else new_driver_tier_v2 end) 
            when city_name_full not in ('HCM City', 'Ha Noi City') then  'part_time' else null end
            driver_tier
        ,bfo.order_id
        ,bfo.group_id
        ,bfo.partner_id
        ,bfo.grass_date
        ,bfo.delivered_by
        ,bfo.distance


from bill_fee_order_base bfo
left join group_info gi 
 on gi.group_id = bfo.group_id
-- where bfo.order_id = 629600616
)
select
    date_trunc('month',grass_date) as month_
    -- ,exchange_rate
    -- ,case 
    --      when city_name IN ('HCM','HN','DN') THEN city_name_full 
    --      when city_name_full IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') then 'T2'
    --      else 'T3'
    --      END AS cities
    ,delivered_by
    -- ,city_group
    ,driver_tier
    -- ,sum(bonus_usd_all + total_driver_cost_base_all + total_driver_cost_surge_all) as driver_cost_v1
    -- ,sum(bonus_usd_all_v2 + total_driver_cost_base_all_v2 + total_driver_cost_surge_all_v2) as driver_cost_v2
    ,sum(total_driver_cost_base_all + total_driver_cost_surge_all)/count(distinct order_id) as base_v1
    ,sum(bonus_usd_all)/count(distinct order_id) as bonus_v1
    ,sum(total_driver_cost_base_surge_est)/count(distinct order_id) as base_est
    ,sum(bonus_usd_all_v2)/count(distinct order_id) as bonus_v2
    ,count(distinct order_id) as ado
    ,count(distinct (partner_id,grass_date)) as unique_a1
    ,count(distinct case when delivered_by = 'hub' then order_id else null end)*1.0000/count(distinct order_id) as pp_hub
    ,count(distinct grass_date) as days
    
    
from driver_cost_raw
where driver_tier not like '%HUB%'
and delivered_by != 'hub'
group by 1,2,3
