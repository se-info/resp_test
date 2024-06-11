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

WHERE raw.report_date BETWEEN date'2024-05-01' and date'2024-05-31'
-- where raw.report_date = current_date - interval '1' day 
-- AND raw.report_date != date'2023-09-09' and raw.report_date != date'2023-09-29'
-- AND raw.shipper_tier != 'Hub'
AND raw.total_order > 0 
)
,summary as 
(select 
        date_trunc('month',report_date) as monthly,
        report_date,
        shipper_id,
        city_tier,
        IF((sla_system > 90 and driver_daily_bonus > 0 and sla_opt1 < 90),1,0) AS is_impact_opt1,
        IF((sla_system > 90 and driver_daily_bonus > 0 and sla_opt2 < 90),1,0) AS is_impact_opt2,
        case 
        when sla_system > 90 and driver_daily_bonus > 0 and sla_opt1 < 90 then 1 else 0 end as is_impact,
        rank()over(partition by report_date order by rand()) as random_rank,
        rank()over(partition by report_date order by sla_system,total_order desc) as fixed_rank,
        count(shipper_id)over(partition by report_date) as driver_impact,
        rank()over(partition by report_date order by ignore_all_service)/cast(count(shipper_id)over(partition by report_date) as double) as pp_dr,
        (driver_daily_bonus) as bonus_saving

from raw 

where shipper_tier_v2 != 'Hub'
and sla_system > 90 and driver_daily_bonus > 0 and sla_opt1 < 90
)

-- select * from summary where impact_version = 'opt1' and report_date = date'2024-05-11'
,f as 
(select
        s.report_date,
        s.city_tier,
        s.shipper_id,
        it.total_ship_shared,
        it.driver_daily_bonus,
        s.fixed_rank,
        s.driver_impact,
        s.pp_dr,
        is_impact_opt1,
        case when s.is_impact_opt1 = 1 and s.pp_dr >= 0.7 then 1 else 0 end as is_impacted_30pp,
        case when s.is_impact_opt1 = 1 and s.pp_dr >= 0.5 then 1 else 0 end as is_impacted_50pp,
        case when s.is_impact_opt1 = 1 and s.pp_dr >= 0.7 then 0 else it.driver_daily_bonus end as bonus_30pp,
        case when s.is_impact_opt1 = 1 and s.pp_dr >= 0.5 then 0 else it.driver_daily_bonus end as bonus_50pp
        -- count(distinct case when pp_dr >= 0.7 then shipper_id else null end) as driver_impact_30pp,
        -- count(distinct case when pp_dr >= 0.5 then shipper_id else null end) as driver_impact_50pp,

        -- sum(case when pp_dr >= 0.7 then bonus_saving else null end) as bonus_add_on_30pp,
        -- sum(case when pp_dr >= 0.5 then bonus_saving else null end) as bonus_add_on_50pp,

        -- COUNT(DISTINCT shipper_id) AS total_driver_impacted,
        -- SUM(bonus_saving) AS total_bonus_saving 

from summary s 

left join driver_ops_driver_income_tracking_tab it 
        ON it.shipper_id = s.shipper_id 
        AND s.report_date = it.report_date
)
select
        city_tier,
        -- (sum(total_ship_shared) + sum(driver_daily_bonus))/cast(count(distinct (shipper_id,report_date)) as double) AS current_income,
        -- (sum(total_ship_shared) + sum(bonus_30pp))/cast(count(distinct (shipper_id,report_date)) as double) AS overall_income_30pp,
        -- (sum(total_ship_shared) + sum(bonus_50pp))/cast(count(distinct (shipper_id,report_date)) as double) AS overall_income_50pp,

        (sum(case when is_impact_opt1 = 1 then total_ship_shared else null end) 
                + sum(case when is_impact_opt1 = 1 then driver_daily_bonus else null end))
                /cast(count(distinct case when is_impact_opt1 = 1 then (shipper_id,report_date) else null end) as double) 
                AS current_income_impacted,

        (sum(case when is_impacted_30pp = 1 then total_ship_shared else null end) 
                + sum(case when is_impacted_30pp = 1 then bonus_30pp else null end))
                /cast(count(distinct case when is_impacted_30pp = 1 then (shipper_id,report_date) else null end) as double) 
                AS impacted_income_30pp,

        (sum(case when is_impacted_50pp = 1 then total_ship_shared else null end) 
                + sum(case when is_impacted_50pp = 1 then bonus_50pp else null end)) 
                /cast(count(distinct case when is_impacted_50pp = 1 then (shipper_id,report_date) else null end) as double) AS impacted_income_50pp
   
from f 

group by 1

