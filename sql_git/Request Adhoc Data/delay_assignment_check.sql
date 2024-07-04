WITH raw AS 
(SELECT  
        DATE(CAST(timedt AS timestamp)) AS date_dt,
        dt,
        order_id,
        store_id,
        buyer_id,
        city_id,
        CASE
        WHEN order_flow = 1 THEN 'dff-mode'
        WHEN order_flow = 0 THEN 'mff-mode' 
        END AS order_mode,
        CASE 
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-04' then 'treatment'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-05' then 'treatment'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-06' then 'treatment'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-07' then 'treatment'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-08' then 'treatment'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-09' then 'treatment'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-10' then 'treatment'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-11' then 'treatment'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-12' then 'control'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-13' then 'control'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-14' then 'control'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-15' then 'control'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-16' then 'control'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-17' then 'control'
        when DATE(CAST(timedt AS timestamp)) = date'2023-10-18' then 'control' end as dt_type,
        -- original_predict_value,
        -- predict_value AS final_value,
        -- 0.0007 AS c,
        CASE 
        WHEN predict_value >= 0.0007 THEN 'dff-mode'
        WHEN predict_value < 0.0007 THEN 'mff-mode'
        END AS order_mode_expectation,
        is_cmd_order,
        ata/CAST(60 AS DOUBLE) AS ata,
        delay_assign_enable,
        case when delay_assign_enable = 1 AND delay_assign_time > 0 then 1 else 0 end as is_da,
        COALESCE((delay_assign_time*1.000/1000)/60,0) AS delay_assign_time

FROM shopeefood_assignment.algo_delay_assign_order_data_vn 

WHERE DATE(CAST(timedt AS timestamp)) BETWEEN DATE'2023-10-04' AND DATE'2023-10-20' 
AND ab_version in ('v0020','v0007')
)
,metrics as 
(SELECT 
        raw.*,
        od.created_timestamp,
        od.picked_timestamp,
        od.order_status,
        case 
        when od.order_status = 'Delivered' then 1 else 0 end as is_del,
        case 
        when od.order_status = 'Cancelled' then 1 else 0 end as is_cancel,
        CASE 
        WHEN od.group_id > 0 AND od.order_assign_type = 'Group' THEN 'group'
        WHEN od.group_id > 0 AND od.order_assign_type != 'Group' THEN 'stack'
        ELSE 'single' END AS assign_type,
        od.is_asap,
        date_diff('second',od.created_timestamp,od.delivered_timestamp) as ata_cal, 
        date_diff('second',od.created_timestamp,od.eta_drop_time) as eta,
        date_diff('second',dot.waybill_created,od.last_incharge_timestamp) as waybill_created_accept,
        date_diff('second',od.last_incharge_timestamp,od.max_arrived_at_merchant_timestamp) as accept_arrive_mex,
        date_diff('second',od.max_arrived_at_merchant_timestamp,od.picked_timestamp) as arrive_to_pick,
        date_diff('second',od.picked_timestamp,od.delivered_timestamp) as pick_to_completed,
        CASE 
        WHEN od.distance <= 1 THEN 30
        WHEN od.distance > 1 THEN LEAST(60,30 + 5*(CEILING(od.distance) -1))
        ELSE NULL END AS lt_sla,
        CASE
        WHEN od.delivered_timestamp > od.eta_drop_time THEN 1 ELSE 0 END AS is_late_eta,
        DATE_DIFF('second',od.max_arrived_at_merchant_timestamp,od.picked_timestamp)*1.00/60 AS driver_waiting,
        DATE_DIFF('second',od.first_auto_assign_timestamp,od.max_arrived_at_merchant_timestamp)*1.00/60 AS merchant_waiting,
        CASE
        WHEN (DATE_DIFF('second',od.created_timestamp,od.delivered_timestamp)*1.00/60) > 
             (CASE 
        WHEN od.distance <= 1 THEN 30
        WHEN od.distance > 1 THEN LEAST(60,30 + 5*(CEILING(od.distance) -1))
        ELSE NULL END) THEN 1 ELSE 0 END AS is_late_sla,
        prep.prepare_time_actual


FROM raw 

LEFT JOIN (SELECT id,prepare_time_actual from shopeefood.foody_order_db__order_completed_merchant_search_tab__reg_daily_s0_live) prep 
    on prep.id = raw.order_id

LEFT JOIN dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab od 
    on od.id = raw.order_id
    and od.order_type = 0
LEFT JOIN (select id,from_unixtime(create_time - 3600) as waybill_created,ref_order_category from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
    on dot.id = od.delivery_id
    and dot.ref_order_category = od.order_type
WHERE raw.city_id != 238
)
select
        date_dt,
        dt_type,
        count(distinct order_id)/cast(count(distinct date_dt) as double) as gross_order,
        count(distinct case when is_del = 1 then order_id else null end)/cast(count(distinct date_dt) as double) as net_order,
        count(distinct case when is_del = 1 then order_id else null end)/cast(count(distinct order_id) as double) as g2n,
        count(distinct case when is_cmd_order = 1 then order_id else null end)/cast(count(distinct order_id) as double) as cmd_rate,
        count(distinct case when is_del = 1 and assign_type in ('stack','group') then order_id else null end)/cast(count(distinct case when is_del = 1 then order_id else null end) as double) as stacked,
        count(distinct case when order_mode = 'mff-mode' then order_id else null end)/cast(count(distinct order_id) as double) as mff_rate,
        count(distinct case when is_da = 1 then order_id else null end)/cast(count(distinct order_id) as double) as da_rate,
        '' as compensated_rate,
        sum(case when delay_assign_time > 0 and dt_type = 'treatment' then delay_assign_time else null end)/cast(count(distinct case when delay_assign_time > 0 and dt_type = 'treatment' then order_id else null end) as double) as avg_delay_time,
        sum(case when is_asap = 1 and is_del = 1 then ata_cal else null end)/cast(count(distinct case when is_asap = 1 and is_del = 1 then order_id else null end) as double) as avg_ata,
        sum(case when is_asap = 1 and is_del = 1 then waybill_created_accept else null end)/cast(count(distinct case when is_asap = 1 and is_del = 1 then order_id else null end) as double) as waybill_created_accept,
        sum(case when is_asap = 1 and is_del = 1 then accept_arrive_mex else null end)/cast(count(distinct case when is_asap = 1 and is_del = 1 then order_id else null end) as double) as accept_arrive_mex,
        sum(case when is_asap = 1 and is_del = 1 then arrive_to_pick else null end)/cast(count(distinct case when is_asap = 1 and is_del = 1 then order_id else null end) as double) as arrive_to_pick,
        sum(case when is_asap = 1 and is_del = 1 then pick_to_completed else null end)/cast(count(distinct case when is_asap = 1 and is_del = 1 then order_id else null end) as double) as pick_to_completed,
        COUNT(DISTINCT CASE WHEN is_late_eta = 1 AND is_del = 1 THEN order_id ELSE NULL END)/CAST(COUNT(DISTINCT CASE WHEN is_del = 1 THEN order_id ELSE NULL END) AS DOUBLE) AS late_eta,
        COUNT(DISTINCT CASE WHEN is_late_sla = 1 AND is_del = 1 THEN order_id ELSE NULL END)/CAST(COUNT(DISTINCT CASE WHEN is_del = 1 THEN order_id ELSE NULL END) AS DOUBLE) AS late_sla,
        AVG(CASE WHEN prepare_time_actual > 0 THEN prepare_time_actual else null end) as prepare_time_actual



from metrics 
group by 1,2    

