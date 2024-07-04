with raw as 
(select 
        do.id,
        case 
        when doet.policy = 2 then 1 
        else 0 end as is_hub,
        shipper_id,
        sa.total_assign,
        date_trunc('month',date(delivered_timestamp)) as month_,
        do.distance,
        case 
        when doet.hub_id > 0 then 1 
        else 0 end as is_hub_qualified,
        do.is_asap,
        date_diff('second',created_timestamp,delivered_timestamp)*1.0000/60 as ata 


from driver_ops_raw_order_tab do 

left join (select 
                order_id,
                cast(json_extract(order_data,'$.shipper_policy.type') as bigint) as policy,  
                COALESCE(CAST(json_extract(order_data,'$.hub_id') as BIGINT),0) as hub_id
            from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da
            where date(dt) = current_date - interval '1' day
            ) doet 
    on doet.order_id = do.delivery_id

left join (select 
                   ref_order_id,
                   count(distinct (ref_order_id,create_time)) as total_assign 
            from driver_ops_order_assign_log_tab
            where order_category = 0
            group by 1  
            ) sa on sa.ref_order_id = do.id

where do.order_type = 0
and do.order_status = 'Delivered'
and do.created_date between date_trunc('month',current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
and do.distance <= 3.6
)
select 
        month_,
        count(distinct case when is_hub = 1 then id else null end)*1.0000/sum(case when is_hub = 1 then total_assign else null end) as compeltion_rate_hub,
        count(distinct case when is_hub = 0  then id else null end)*1.0000/sum(case when is_hub = 0 then total_assign else null end) as compeltion_rate_non_hub,
        sum(case when is_asap = 1 and is_hub = 1 then ata else null end)*1.0000/count(distinct case when is_asap = 1 and is_hub = 1 then id else null end) as hub_ata,
        sum(case when is_asap = 1 and is_hub = 0 then ata else null end)*1.0000/count(distinct case when is_asap = 1 and is_hub = 0 then id else null end) as non_hub_ata


from raw 
group by 1 