WITH raw AS 
(SELECT 
     raw.report_date
    ,raw.shipper_id
    ,raw.city_name 
    ,case
    when regexp_like(lower(raw.city_name),'dak lak|thanh hoa|binh thuan|binh dinh') = true THEN 'new cities' 
    when raw.city_id in (217,218,219) then raw.city_name
    when raw.city_id in (220,221,222,223,230,273) then 'T2'
    else 'T3' end as city_tier
    ,raw.sla_rate AS sla_system
    ,raw.total_order
    ,raw.total_order - raw.hub_order as non_hub_order
    ,raw.total_order_food
    ,raw.total_order_spxi
    ,raw.driver_daily_bonus
    ,raw.driver_income
    ,raw.shipper_tier
    ,case 
    when raw.shipper_tier = 'Hub' and hub.uid is not null then 'Hub'
    when raw.shipper_tier = 'Hub' and hub.uid is null then 'T1' else shipper_tier end as shipper_tier_v2
    ,COALESCE(sa.denied,0) AS denied
    ,COALESCE(sa.ignore_current,0) AS ignore_current
    ,COALESCE(sa.ignore_opt1,0) AS ignore_all_service
    ,COALESCE(sa.ignore_opt2,0) AS ignore_spxi_only
    ,ROUND((raw.total_order*1.00/CAST((COALESCE(sa.denied,0) + COALESCE(sa.ignore_current,0) + raw.total_order) AS DOUBLE) )*100,2) AS sla_cal

    ,ROUND((raw.total_order*1.00/CAST((COALESCE(sa.denied,0) + COALESCE(sa.ignore_opt1,0) + raw.total_order) AS DOUBLE) )*100,2) AS sla_opt1 -- #count ignore stack/group all service
    ,ROUND((raw.total_order*1.00/CAST((COALESCE(sa.denied,0) + COALESCE(sa.ignore_opt1,0)*0.5 + raw.total_order) AS DOUBLE) )*100,2) AS sla_opt2 -- #count ignore stack/group all service - weight = 0.5

    ,ROUND((raw.total_order*1.00/CAST((COALESCE(sa.denied,0) + COALESCE(sa.ignore_opt2,0) + raw.total_order) AS DOUBLE) )*100,2) AS sla_opt3 -- #count ignore stack/group spxi only

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab raw 

LEFT JOIN (select date_,uid,sum(total_order) as total_order 
        from dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab 
        where total_order > 0
        group by 1,2) hub 
        on hub.uid = raw.shipper_id and hub.date_ = raw.report_date

LEFT JOIN (SELECT 
                DATE(create_time) AS created_date
                ,driver_id
                ,COUNT(DISTINCT CASE WHEN status IN (8,9) AND TRIM(assign_type) != '6. New Stack Assign' AND order_type != 'Group' THEN (order_id,create_time) ELSE NULL END) AS ignore_current
                ,COUNT(DISTINCT CASE WHEN status IN (8,9,17,18) THEN (order_id,create_time) ELSE NULL END) AS ignore_opt1 -- #count ignore stack/group all service
                
                -- #count ignore stack/group spxi only
                ,COUNT(DISTINCT CASE WHEN (status IN (8,9,17,18) AND order_category != 0
                                           or 
                                           status IN (8,9) AND order_category = 0 AND TRIM(assign_type) != '6. New Stack Assign' AND order_type != 'Group' ) 
                                           THEN (order_id,create_time) ELSE NULL END) AS ignore_opt2 
                ,COUNT(DISTINCT CASE WHEN status IN (2) THEN (order_id,create_time) ELSE NULL END) AS denied

FROM dev_vnfdbi_opsndrivers.driver_ops_order_assign_log_tab 
WHERE 1 = 1 
AND status IN (2,3,8,9,17,18)
GROUP BY 1,2
) sa 
    on sa.driver_id = raw.shipper_id
    and sa.created_date = raw.report_date

WHERE raw.report_date BETWEEN date'2023-09-01' and date'2023-09-30'
-- AND raw.report_date != date'2023-09-09' and raw.report_date != date'2023-09-29'
-- AND raw.shipper_tier != 'Hub'
AND raw.total_order > 0 
)
,income_mapping_tab as
(select 
        report_date,
        impact_version,
        city_tier,
        shipper_id,         
        case when pp_dr >= 0.7 then bonus_saving else 0 end as bonus_add_on_30pp,
        case when pp_dr >= 0.5 then bonus_saving else 0 end as bonus_add_on_50pp,
        case when pp_dr >= 0.3 then bonus_saving else 0 end as bonus_add_on_70pp,
        case when pp_dr >= 0.1 then bonus_saving else 0 end as bonus_add_on_90pp,

        case when pp_dr >= 0.7 then 90 else sla_simulation end as sla_30pp,
        case when pp_dr >= 0.5 then 90 else sla_simulation end as sla_50pp,
        case when pp_dr >= 0.3 then 90 else sla_simulation end as sla_70pp,
        case when pp_dr >= 0.1 then 90 else sla_simulation end as sla_90pp
from
(select 
        date_trunc('month',report_date) as monthly,
        report_date,
        shipper_id,
        city_tier,
        'opt1' as impact_version,
        sla_system,
        sla_opt1 as sla_simulation,
        case 
        when sla_system > 90 and driver_daily_bonus > 0 and sla_opt1 < 90 then 1 else 0 end as is_impact,
        dense_rank()over(partition by report_date order by rand()) as random_rank,
        count(shipper_id)over(partition by report_date) as driver_impact,
        dense_rank()over(partition by report_date order by rand())/cast(count(shipper_id)over(partition by report_date) as double) as pp_dr,
        (driver_daily_bonus) as bonus_saving

from raw 

where shipper_tier_v2 != 'Hub'
and (sla_system > 90 and driver_daily_bonus > 0 and sla_opt1 < 90)
UNION ALL
select 
        date_trunc('month',report_date) as monthly,
        report_date,
        shipper_id,
        city_tier,
        'opt2' as impact_version,
        sla_system,
        sla_opt2 as sla_simulation,
        case 
        when sla_system > 90 and driver_daily_bonus > 0 and sla_opt2 < 90 then 1 else 0 end as is_impact,
        dense_rank()over(partition by report_date order by rand()) as random_rank,
        count(shipper_id)over(partition by report_date) as driver_impact,
        dense_rank()over(partition by report_date order by rand())/cast(count(shipper_id)over(partition by report_date) as double) as pp_dr,
        (driver_daily_bonus) as bonus_saving

from raw 

where shipper_tier_v2 != 'Hub'
and (sla_system > 90 and driver_daily_bonus > 0 and sla_opt2 < 90)
UNION ALL
select 
        date_trunc('month',report_date) as monthly,
        report_date,
        shipper_id,
        city_tier,
        'opt3' as impact_version,
        sla_system,
        sla_opt3 as sla_simulation,
        case 
        when sla_system > 90 and driver_daily_bonus > 0 and sla_opt3 < 90 then 1 else 0 end as is_impact,
        dense_rank()over(partition by report_date order by rand()) as random_rank,
        count(shipper_id)over(partition by report_date) as driver_impact,
        dense_rank()over(partition by report_date order by rand())/cast(count(shipper_id)over(partition by report_date) as double) as pp_dr,
        (driver_daily_bonus) as bonus_saving

from raw 

where shipper_tier_v2 != 'Hub'
and (sla_system > 90 and driver_daily_bonus > 0 and sla_opt3 < 90)
)
)
,metrics as 
(select 
        raw.report_date,
        raw.shipper_id,
        raw.city_tier,
        raw.total_order,
        raw.total_order_food,
        raw.total_order_spxi,
        raw.sla_system,
        raw.sla_opt1,
        raw.sla_opt2,
        raw.sla_opt3,
        raw.driver_daily_bonus as current_daily_bonus,
        raw.driver_income - raw.driver_daily_bonus as ship_shared,
        case 
        when raw.sla_opt1 < 90 then 0 else raw.driver_daily_bonus end as bonus_opt1,
        case 
        when raw.sla_opt2 < 90 then 0 else raw.driver_daily_bonus end as bonus_opt2,
        case 
        when raw.sla_opt3 < 90 then 0 else raw.driver_daily_bonus end as bonus_opt3,
        case 
        when im.shipper_id is not null then im.bonus_add_on_30pp 
        else (case 
        when raw.sla_opt3 < 90 then 0 else raw.driver_daily_bonus end) end as bonus_opt3_1,
        case 
        when im.shipper_id is not null then im.bonus_add_on_50pp 
        else (case 
        when raw.sla_opt3 < 90 then 0 else raw.driver_daily_bonus end) end as bonus_opt3_2,
        case 
        when im.shipper_id is not null then im.bonus_add_on_70pp 
        else (case 
        when raw.sla_opt3 < 90 then 0 else raw.driver_daily_bonus end) end as bonus_opt3_3,
        case 
        when im.shipper_id is not null then im.bonus_add_on_90pp 
        else (case 
        when raw.sla_opt3 < 90 then 0 else raw.driver_daily_bonus end) end as bonus_opt3_4,

        case 
        when im.shipper_id is not null then im.sla_30pp 
        else sla_opt3 end as sla_opt3_1,
        case 
        when im.shipper_id is not null then im.sla_50pp 
        else sla_opt3 end as sla_opt3_2,
        case 
        when im.shipper_id is not null then im.sla_70pp 
        else sla_opt3 end as sla_opt3_3,
        case 
        when im.shipper_id is not null then im.sla_90pp 
        else sla_opt3 end as sla_opt3_4

from raw 

left join income_mapping_tab im 
    on im.report_date = raw.report_date
    and im.shipper_id = raw.shipper_id
    and im.impact_version = 'opt3'


where shipper_tier_v2 != 'Hub'
)
select 
        coalesce(city_tier,'VN') as cities,
        (sum(ship_shared) + sum(current_daily_bonus))/count(shipper_id) as income_current,
        (sum(ship_shared) + sum(bonus_opt1))*1.000/count(shipper_id) as income_opt1,
        (sum(ship_shared) + sum(bonus_opt2))*1.000/count(shipper_id) as income_opt2,
        (sum(ship_shared) + sum(bonus_opt3))*1.000/count(shipper_id) as income_opt3,
        (sum(ship_shared) + sum(bonus_opt3_1))*1.000/count(shipper_id) as income_opt3_1,
        (sum(ship_shared) + sum(bonus_opt3_2))*1.000/count(shipper_id) as income_opt3_2,
        (sum(ship_shared) + sum(bonus_opt3_3))*1.000/count(shipper_id) as income_opt3_3,
        (sum(ship_shared) + sum(bonus_opt3_4))*1.000/count(shipper_id) as income_opt3_4,

        sum(sla_system)*1.000/count(shipper_id) as avg_sla_current,
        sum(sla_opt1)*1.000/count(shipper_id) as avg_sla_opt1,
        sum(sla_opt2)*1.000/count(shipper_id) as avg_sla_opt2,
        sum(sla_opt3)*1.000/count(shipper_id) as avg_sla_opt3,
        sum(sla_opt3_1)*1.000/count(shipper_id) as avg_sla_opt3_1,
        sum(sla_opt3_2)*1.000/count(shipper_id) as avg_sla_opt3_2,
        sum(sla_opt3_3)*1.000/count(shipper_id) as avg_sla_opt3_3,
        sum(sla_opt3_4)*1.000/count(shipper_id) as avg_sla_opt3_4
        
from metrics


group by grouping sets (city_tier,())






