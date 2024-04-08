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
                ROUND(MAX(fee_1_cal) + (MAX(fee_2_cal)/1.27)*0.2),
                ROUND(MAX(fee_2_cal) + (MAX(fee_1_cal)/1.27)*0.2)
            ),LEAST(SUM(single_fee),ROUND((ROUND(SUM(single_distance)/1.27,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),ROUND(SUM(single_distance)/1.27,1)*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_2024

        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_cal)),
                ROUND(MAX(fee_2_cal))
            ),LEAST(SUM(single_fee),ROUND( (SUM(single_distance)*1.00/1.34) *MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),ROUND((SUM(single_distance)*1.00/1.34),1)*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_2025

        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_cal)),
                ROUND(MAX(fee_2_cal))
            ),LEAST(SUM(single_fee),ROUND( (SUM(single_distance)*1.00/1.37) *MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),ROUND((SUM(single_distance)*1.00/1.37),1)*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_2026

        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_cal)),
                ROUND(MAX(fee_2_cal))
            ),LEAST(SUM(single_fee),ROUND( (SUM(single_distance)*1.00/1.4) *MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),ROUND((SUM(single_distance)*1.00/1.4),1)*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_2027

FROM dev_vnfdbi_opsndrivers.group_order_info_raw 
-- where group_code = 'D00937319218'

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
and grass_date between date '2023-10-01' and date '2023-10-30'
and coalesce(city_name_full,'na') not in ('na','TestCity','Dien Bien')
)
select 
        date_trunc('month',grass_date) as month_,
        -- order_id,
        -- group_id,
        case 
        when city_name in ('HCM','HN') then delivered_by
        else 'small_city' end as type_,
        -- delivered_by,
        count(distinct order_id)*1.00000/count(distinct grass_date) net_order,
        sum(total_shipping_fee) as before_stack_shipping_fee,
        sum(case 
            when delivered_by = 'hub' then total_shipping_fee else driver_cost_base_n_surge end) as after_stack_shipping_fee_current,
        sum(case 
            when delivered_by = 'hub' then total_shipping_fee else gi.group_fee_2024/gi.total_order_in_group end) as after_stack_2024,
        sum(case 
            when delivered_by = 'hub' then total_shipping_fee else gi.group_fee_2025/gi.total_order_in_group end) as after_stack_2025,
        sum(case 
            when delivered_by = 'hub' then total_shipping_fee else gi.group_fee_2026/gi.total_order_in_group end) as after_stack_2026,
        sum(case 
            when delivered_by = 'hub' then total_shipping_fee else gi.group_fee_2027/gi.total_order_in_group end) as after_stack_2027
        -- ,(sum(driver_cost_base_n_surge) - sum(total_shipping_fee))/count(distinct order_id) vnd_saving_per_order

from bill_fee_order_base bfo
left join group_info gi 
 on gi.group_id = bfo.group_id

where 1=1
and bfo.group_id > 0
-- and delivered_by = 'non-hub'
and source in ('Food','Market')
and date_ >= date'2023-10-01'
group by 1,2
