WITH transaction_tab AS 
(SELECT 
         DATE(FROM_UNIXTIME(create_time - 3600)) AS created_date 
        ,user_id
        ,note
        ,extra_data
        ,txn_type
        ,(deposit*1.00/100) - (balance*1.00/100) AS deposit_on_date
        ,case 
         when (deposit*1.00/100) - (balance*1.00/100) > 0 then row_number()over(partition by user_id order by create_time asc) 
         else 0 end as rank_txn
        ,row_number()over(partition by user_id order by DATE(FROM_UNIXTIME(create_time - 3600)) desc) as rank_date 
        ,SUM(balance*1.000/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) AS balance_accum
        ,SUM(((deposit*1.00/100) - (balance*1.00/100))) over (partition by user_id order by id asc rows between unbounded preceding and current row) AS deposit_accum
FROM shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live
WHERE txn_type IN (1,103,151,509,510,511,801,802,803,804,702)	
)
,agg_driver AS 
(SELECT
        shipper_id,
        ARRAY_AGG( DISTINCT
        CASE
        WHEN total_order > 0 THEN report_date ELSE NULL END            
        ) AS agg_delivered_date
FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab

WHERE total_order > 0
GROUP BY 1 
)
,metrics AS 
(SELECT 
        dp.report_date,
        dp.shipper_id,
        dp.city_name,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x = dp.report_date)),0) AS agg_a1,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x between dp.report_date - interval '6' day and dp.report_date)),0) AS agg_a7,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x between dp.report_date - interval '29' day and dp.report_date)),0) AS agg_a30,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x between dp.report_date - interval '59' day and dp.report_date)),0) AS agg_a60,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x between dp.report_date - interval '89' day and dp.report_date)),0) AS agg_a90,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x between dp.report_date - interval '119' day and dp.report_date)),0) AS agg_a120,
        dp.online_hour,
        dp.work_hour,
        dp.online_hour - dp.work_hour AS down_hour,
        dp.total_order,
        CASE 
        WHEN city_name IN ('HCM City', 'Ha Noi City', 'Da Nang City') THEN city_name
        WHEN city_name IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') THEN 'T2'
        ELSE 'T3' END AS city_group


FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab dp 

LEFT JOIN agg_driver agg 
    on agg.shipper_id = dp.shipper_id

WHERE 1 = 1 )
,f as 
(select 
        m.report_date,
        m.shipper_id,
        m.city_group,
        m.agg_a1,
        m.agg_a7,
        m.agg_a30,
        m.agg_a90,
        st.is_available_spxi,
        coalesce(max_by(tt.deposit_accum,tt.created_date),0) as deposit
        -- case 
        -- when tt.deposit_accum 


from metrics m 


left join dev_vnfdbi_opsndrivers.driver_ops_driver_services_tab st 
        on st.uid = m.shipper_id
        and st.report_date = m.report_date

left join transaction_tab tt 
        on tt.user_id = m.shipper_id
        and tt.created_date <= m.report_date

group by 1,2,3,4,5,6,7,8
order by 1 desc )
select 
        f.report_date,
        city_group,
        is_available_spxi,
        case 
        when deposit = 0 then '1. No deposit'
        when deposit < 300000 then '2. 0 - 300k'
        when deposit < 500000 then '3. 300 - 500k'
        when deposit < 1000000 then '4. 500 - 1000k'
        when deposit < 2000000 then '5. 1000 - 2000k'
        when deposit < 5000000 then '6. 2000 - 5000k'
        when deposit >= 5000000 then '7. ++5000k' end as deposit_range,
        count(distinct case when agg_a1 > 0 then shipper_id else null end) as a1,
        count(distinct case when agg_a7 > 0 then shipper_id else null end) as a7,
        count(distinct case when agg_a30 > 0 then shipper_id else null end) as a30

from f 
where is_available_spxi is not null 

group by 1,2,3,4


