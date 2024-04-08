with order_info as 
(SELECT *from driver_ops_temp_order 
)
,group_info as 
(select  
        group_id,
        order_type,
        ROUND(CASE WHEN MAX(rank_order) >= 2 THEN 
        MAX(final_stack_fee) - (CASE WHEN MAX(rank_order) = 2 THEN 
         ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  ))
              WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))   
              ELSE MAX(final_stack_fee) END) END) AS extra_fee,
        
        ROUND(CASE WHEN MAX(rank_order) >= 2 THEN 
        MAX(final_stack_fee) - (CASE WHEN MAX(rank_order) = 2 THEN 
         ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  ))
              WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))   
              ELSE MAX(final_stack_fee) END) END)/MAX(rank_order) AS extra_fee_allocate,

         MAX(final_stack_fee)/CAST(MAX(rank_order) AS DOUBLE) AS group_fee_current,
        ROUND(CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt1) + (MAX(fee_2_opt1)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt1) + (MAX(fee_1_opt1)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt1),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt1)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt1),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END)/MAX(rank_order) AS group_fee_opt1,

        ROUND(CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt2) + (MAX(fee_2_opt2)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt2) + (MAX(fee_1_opt2)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt2),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt2)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt2),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END)/MAX(rank_order) AS group_fee_opt2,

        ROUND(CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt3_4) + (MAX(fee_2_opt3_4)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt3_4) + (MAX(fee_1_opt3_4)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt3_4),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt3_4)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt3_4),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END)/MAX(rank_order) AS group_fee_opt3_4

from order_info

where group_id > 0 
and driver_policy != 2
group by 1,2,group_category
)
,metrics as 
(select 
        raw.report_date,
        raw.shipper_id,
        raw.is_hub,
        raw.cities,
        raw.shift_hour,
        raw.slot_id,
        coalesce(do.extra_ship,0) as extra_ship,
        count(distinct ref_order_id) as total_order,
        sum(current_fee) as current_fee,
        sum(fee_opt1) as fee_opt1,
        sum(fee_opt2) as fee_opt2,
        sum(fee_opt3) as fee_opt3,
        sum(fee_opt4) as fee_opt4     
from
(select 
        oi.report_date,
        oi.ref_order_id,
        oi.group_id,
        oi.shipper_id,
        oi.slot_id,
        shift_hour,
        case 
        when oi.city_name in ('HCM City','Ha Noi City','Da Nang City') then oi.city_name
        else 'Others' end as cities,
        oi.group_code,
        oi.order_code,
        case 
        when oi.slot_id > 0 then 1 else 0 end as is_hub,
        case 
        when oi.slot_id > 0 then oi.hub_current
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_current
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_current end as current_fee,

        case 
        when oi.slot_id > 0 then oi.hub_opt1
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_opt1
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_opt1 end as fee_opt1,

        case 
        when oi.slot_id > 0 then oi.hub_opt2
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_opt2
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_opt2 end as fee_opt2,

        case 
        when oi.slot_id > 0 then oi.hub_opt3
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_opt3_4
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_opt3_4 end as fee_opt3,

        case 
        when oi.slot_id > 0 then oi.hub_opt4
        when oi.slot_id is null and oi.group_id = 0 then oi.non_hub_opt3_4
        when oi.slot_id is null and oi.group_id > 0 then ogi.group_fee_opt3_4 end as fee_opt4

from order_info oi
left join group_info ogi 
        on ogi.group_id = oi.group_id 
        and oi.order_type = ogi.order_type
) raw 

left join driver_ops_hub_driver_performance_tab do 
    on do.uid = raw.shipper_id
    and do.date_ = raw.report_date
    and do.total_order > 0 
    and do.slot_id = raw.slot_id
where 
(fee_opt1 is not null 
or fee_opt2 is not null
or fee_opt3 is not null
or fee_opt4 is not null)
group by 1,2,3,4,5,6,7
)
,income as 
(select 
        date_trunc('month',m.report_date) as month_,
        m.shipper_id,
        m.report_date,
        m.is_hub,
        max_by(shift_hour,shift_hour) as shift_hour, 
        sum(current_fee) as current_fee,
        sum(current_fee + coalesce(extra_ship,0)) as current_income,
        sum(fee_opt1 + new_extra_opt1) as income_opt1,
        sum(fee_opt2 + new_extra_opt2) as income_opt2,
        sum(fee_opt3 + new_extra_opt3) as income_opt3,
        sum(fee_opt4 + new_extra_opt4) as income_opt4

from
(select  
        m.*,
        case 
        when extra_ship > 0 and shift_hour = 10 then (13500*30)-fee_opt1
        when extra_ship > 0 and shift_hour = 8 then (13500*25)-fee_opt1
        else 0 end as new_extra_opt1,
        case 
        when extra_ship > 0 and shift_hour = 10 then (13500*30)-fee_opt2
        when extra_ship > 0 and shift_hour = 8 then (13500*25)-fee_opt2
        else 0 end as new_extra_opt2,
        case 
        when extra_ship > 0 and shift_hour = 10 then (13500*30)-fee_opt3
        when extra_ship > 0 and shift_hour = 8 then (13500*25)-fee_opt3
        else 0 end as new_extra_opt3,
        case 
        when extra_ship > 0 and shift_hour = 10 then (13500*30)-fee_opt4
        when extra_ship > 0 and shift_hour = 8 then (13500*25)-fee_opt4
        else 0 end as new_extra_opt4

from metrics m
where cities in ('HCM City','Ha Noi City')

) m 
group by 1,2,3,4
)
,income_v2 as 
(select 
        m.*,
        case 
        when m.is_hub = 0 and dp.shipper_tier = 'Hub' then 'Level 1' 
        when m.is_hub = 1 then cast(shift_hour as varchar) else dp.shipper_tier 
        end as tier,
        dp.driver_daily_bonus,
        dp.driver_other_income,
        (dp.ship_shared - current_fee) as spxi_fee

from income m

left join 
(select 
        report_date,
        shipper_id,
        shipper_tier,
        driver_income - driver_daily_bonus - driver_other_income as ship_shared,
        driver_daily_bonus,
        -- (driver_daily_bonus/cast(total_order as double))*total_order_food as driver_daily_bonus ,
        driver_other_income 
from driver_ops_driver_performance_tab 
where total_order > 0 
-- and shipper_id = 41117346 and report_date = date'2023-10-20'
)dp  
    on m.report_date = dp.report_date
    and m.shipper_id = dp.shipper_id
)
-- select * from income_v2 where shipper_id = 41117346 and report_date=  date'2023-10-20';
select
        month_,
        coalesce(tier,'VN') as tier,
        count(shipper_id) as total_driver,
        avg(current_income + driver_daily_bonus + driver_other_income + spxi_fee) as current_income,
        sum(income_opt1 + driver_daily_bonus + driver_other_income + spxi_fee)/cast(count(shipper_id) as double) as opt1,
        sum(income_opt2 + driver_daily_bonus + driver_other_income + spxi_fee)/cast(count(shipper_id) as double) as opt2,
        sum(income_opt3 + driver_daily_bonus + driver_other_income + spxi_fee)/cast(count(shipper_id) as double) as opt3,
        sum(income_opt4 + driver_daily_bonus + driver_other_income + spxi_fee)/cast(count(shipper_id) as double) as opt4

from income_v2
where tier is not null
and tier != 'Other'
group by 1, grouping sets (tier,())
;