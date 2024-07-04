with hub_cost as
(select 
    -- from_unixtime(create_time-3600)
    reference_id as autopay_report_id
    ,user_id
    ,SUM(case when txn_type in (906,907) then balance*1.0000 + deposit*1.0000 else null end)/100 as hub_cost_auto
    ,SUM(case when txn_type in (906) then balance*1.0000 + deposit*1.0000 else null end)/100 as hub_cost_auto_shipping_fee
    ,SUM(case when txn_type in (907) then balance*1.0000 + deposit*1.0000 else null end)/100 as hub_cost_auto_daily_bonus
    -- ,
    -- ,*
from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
-- where user_id = 22283353
where 1=1
and txn_type in (906,907)
group by 1,2
)
,hub_order_tab as
(
    select 
        ho.uid as shipper_id
        ,ho.slot_id
        ,ho.autopay_report_id
        ,ho.ref_order_id
        ,ho.ref_order_category
        ,case when coalesce(oct.risk_bearer_id,0) != 2 then 1 else 0 end as is_hub_order
        ,date(from_unixtime(ho.autopay_date_ts-3600)) as autopay_date
        ,date(from_unixtime(ho.create_time-3600)) as created_date
    from shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_daily_s0_live ho
    left join (select id,cast(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') as int) as risk_bearer_id from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct) oct
        on ho.ref_order_id = oct.id and ho.ref_order_category = 0
)
,hub_order_agg as 
(
select
    shipper_id
    ,slot_id
    ,count(distinct (ref_order_id,ref_order_category)) as total_bill_hub
    -- risk_bearer_id --- 1: spf, 2- driver, spf thi van tinh bill hub bonus, 2 thi ko tinh bill hub bonus
    ,count(distinct case when is_hub_order = 1 then (ref_order_id,ref_order_category) else null end) as total_bill_hub_bonus
from hub_order_tab
group by 1,2
)

select 
    ho.shipper_id
    ,ho.slot_id
    ,ho.autopay_report_id
    ,ho.ref_order_id
    ,ho.ref_order_category
    ,ho.is_hub_order
    ,ho.autopay_date
    ,ho.created_date
    ,b.total_bill_hub
    ,b.total_bill_hub_bonus
    ,coalesce(hc.hub_cost_auto,0) as hub_cost_auto
    ,coalesce(hc.hub_cost_auto_shipping_fee,0) as hub_cost_auto_shipping_fee
    ,coalesce(hc.hub_cost_auto_daily_bonus,0) as hub_cost_auto_daily_bonus

    -- per order
    ,coalesce(hc.hub_cost_auto_shipping_fee,0)/total_bill_hub as hub_shipping_fee_per_order
    ,case when is_hub_order = 1 then coalesce(hc.hub_cost_auto_daily_bonus /total_bill_hub_bonus,0) else 0 end as hub_daily_bonus_per_order
    ,coalesce(hc.hub_cost_auto_shipping_fee,0)/total_bill_hub + case when is_hub_order = 1 then coalesce(hc.hub_cost_auto_daily_bonus /total_bill_hub_bonus,0) else 0 end as hub_cost_auto_per_order
    ,autopay_date as grass_date


from hub_order_tab ho
left join hub_order_agg b
    on ho.shipper_id = b.shipper_id and ho.slot_id = b.slot_id
left join hub_cost hc
    on ho.autopay_report_id = hc.autopay_report_id
where ho.autopay_date = date'2024-01-01'
and ho.shipper_id = 40903659 
