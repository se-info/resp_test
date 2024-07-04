WITH transaction_tab AS 
(SELECT 
         DATE(FROM_UNIXTIME(create_time - 3600)) AS created_date 
        ,user_id
        -- ,balance*1.00/100 as balance
        -- ,deposit*1.00/100 as deposit
        ,note
        ,extra_data
        ,txn_type
        ,SUM(balance*1.000/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) AS balance_accum
        ,SUM(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) AS deposit_accum
        ,(deposit*1.00/100) - (balance*1.00/100) AS deposit_on_date
        -- ,SUM(balance*1.00/100) AS balance
        -- ,SUM(deposit*1.00/100) AS deposit


FROM shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live

WHERE txn_type IN (1,103,151,509,510,511,801,802,803,804,702)	
AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2023-08-17' AND DATE'2023-09-17'
)
SELECT
        raw.*,
        case 
        when is_qualifed_working_type = 1 and SUM(tt.deposit_on_date) >= 1000000 then 1 
        else 0 end as is_qualified_bonus,
        MAP_AGG(tt.created_date,tt.deposit_on_date) AS date_x_deposit,
        SUM(tt.deposit_on_date) AS total_deposit 
FROM
(SELECT 
        pf.uid,
        case 
        when pf.city_id = 217 then 'HCM' 
        when pf.city_id = 218 then 'HN' else 'OTH'
        end as city_group,
        pf.shopee_uid,
        pf.full_name,
        pf.working_status,
        DATE(FROM_UNIXTIME(create_time - 3600)) AS onboard_date,
        ARRAY_AGG(DISTINCT sm.shipper_type) AS type_check,
        case 
        when CARDINALITY(FILTER(ARRAY_AGG(DISTINCT sm.shipper_type),x-> x = 'hub')) > 0 THEN 0 ELSE 1 END AS is_qualifed_working_type

FROM shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live pf

LEFT JOIN 
(SELECT 
        shipper_id,
        case 
        when shipper_type_id = 12 then 'hub' else 'non hub' end as shipper_type,
        try_cast(sm.grass_date AS date) AS report_date
FROM shopeefood.foody_mart__profile_shipper_master sm 
WHERE try_cast(sm.grass_date AS date) BETWEEN DATE'2023-08-01' AND DATE'2023-09-17'
) sm 
    on sm.shipper_id = pf.uid

WHERE DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2023-08-01' AND DATE'2023-09-17'
AND regexp_like(lower(full_name),'test|stress') = false
AND city_id IN (217,218)
-- AND pf.uid = 41591196
GROUP BY 1,2,3,4,5,6
) raw 

LEFT JOIN transaction_tab tt 
    on tt.user_id = raw.uid

WHERE raw.working_status = 1
GROUP BY 1,2,3,4,5,6,7,8,is_qualifed_working_type
