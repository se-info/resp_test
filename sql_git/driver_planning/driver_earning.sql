with params(period_grp, period, start_date, end_date, days) AS (
    SELECT
        'Daily' as period_grp, 
         CAST(report_date AS VARCHAR)
        ,CAST(report_date AS DATE)
        ,CAST(report_date AS DATE)
        ,CAST(1 AS DOUBLE)
        -- ,CAST(first_day_of_month AS DATE) first_day_of_month
    FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim  
    WHERE report_date BETWEEN current_date - interval '15' day and current_date - interval'1' day
    -- where report_date between date '2022-10-16' and date '2022-10-17'

    UNION -- week

    SELECT 
        DISTINCT
        'Weekly' as period_grp
        ,CAST(week_of_year_name AS VARCHAR),  CAST(first_day_of_week AS DATE)
        ,CAST((CASE WHEN report_date = current_date - interval '1' day THEN report_date ELSE last_day_of_week END) AS DATE)
        ,CAST((CASE WHEN report_date = current_date - interval '1' day THEN day_of_week ELSE 7 END) AS DOUBLE)
        -- ,CAST(first_day_of_month AS DATE) first_day_of_month
    FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim  
    WHERE report_date = current_date - interval'1' day OR (report_date BETWEEN current_date - interval'28' day and current_date - interval'7' day)
    
    UNION -- month

    SELECT 
        'Monthly' as period_grp
        ,CAST(month_name AS VARCHAR),  CAST(first_day_of_month AS DATE)
        ,CAST((CASE WHEN report_date = current_date - interval '1' day THEN report_date ELSE last_day_of_month END) AS DATE)
        ,CAST((CASE WHEN report_date = current_date - interval '1' day THEN day ELSE num_day_in_month END) AS DOUBLE)
        -- ,CAST(first_day_of_month AS DATE) first_day_of_month
    FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim  
    WHERE report_date IN (current_date - interval'1' day, current_date - interval'1' day - interval'1' month, current_date - interval'1' day - interval'2' month)
    -- where report_date between date '2022-10-16' and date '2022-10-17'
)
,earning_tab as
(SELECT
    date_, 
    exchange_rate,
    case 
        when i.city_name_full in ('HCM City','Ha Noi City', 'Hai Phong City') and current_driver_tier = 'Hub' then 'Hub' 
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T1' then 'T1'
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T2' then 'T2'
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T3' then 'T3'
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T4' then 'T4'
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T5' then 'T5'
        -- else 'OTH'
        when i.city_name not in ('HCM','HN') then 'OTH'
        else 'full time' 
    end as driver_tier
    ,total_earning_before_tax
    ,partner_id
from vnfdbi_opsndrivers.snp_foody_shipper_income_tab i
LEFT JOIN (SELECT distinct
    grass_date,
    exchange_rate
    FROM 
    mp_order.dim_exchange_rate__reg_s0_live 
    WHERE
    currency='VND'
    and grass_date >= date('2020-12-28')
)xrate on xrate.grass_date = i.date_
left join shopeefood.foody_mart__profile_shipper_master sm
    on i.partner_id = sm.shipper_id and i.date_ = try_cast(sm.grass_date as date)
where sm.shipper_type_id != 3 and sm.shipper_status_code = 1

-- where coalesce(city_name_full,'n/a') not in ('n/a','TestCity','TestCity1','TestCity2','Dien Bien')
-- and shipper_type != 'tester'
-- where coalesce(city_name_full,'n/a') not in ('HCM City')

-- where total_bill >0 and date_ between date(current_date) - interval '70' day and date(current_date) - interval '1' day
)
,base2 as
(select 
    p.period_grp
    ,p.period
    ,p.days
    -- ,exchange_rate
    ,coalesce(driver_tier,'VN') as driver_tier
    ,sum(total_earning_before_tax/exchange_rate) / count(distinct (e.date_,partner_id))  as earning_per_driver
    ,count(distinct (e.date_,partner_id)) / p.days as avg_transacting_driver
from earning_tab e
inner join params p
    on e.date_ between p.start_date and p.end_date
-- where driver_tier != 'full time'
GROUP BY 1,2,3, grouping sets (driver_tier,())
)
select 
    *
from base2
