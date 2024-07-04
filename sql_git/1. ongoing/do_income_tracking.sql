with raw as 
(select 

    reference_id,
    user_id as partner_id,
    date(from_unixtime(create_time - 3600)) AS created_date,  
    date(from_unixtime(create_time - 3600)) - interval '1' day AS hub_date,  
    sum(case when txn_type in (906) then (balance + deposit)*1.00/100 else 0 end) as shipping_share_hub,
    sum(case when txn_type in (907) then (balance + deposit)*1.00/100 else 0 end) as daily_bonus_hub,
    sum(case when txn_type in (104,201,301,2101,2001,3000,1000,401) then (balance + deposit)*1.00/100 else 0 end) as shipping_share_non_hub, -- 104: delivery, 201: NS_user, 301: NS_merchant, 2101: Multidrop, 2001: Sameday, 3000: SPX
  --  sum(case when txn_type in (105,304) then (balance + deposit)*1.00/100 else 0 end) as additional_bonus, -- 105: delivery, 304: NS_merchant
    sum(case when txn_type in (114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137) then (balance + deposit)*1.00/100 else 0 end) as other_bonus, -- bwf for quit order
    sum(case when txn_type in (106,203,2003,303,1002,1005,1007,3002,3005,3007) then (balance + deposit)*1.00/100 else 0 end) as other_payable,
    sum(case when txn_type in (202,302,2002,2102,1001,402,3001) then (balance + deposit)*1.00/100 else 0 end) as return_fee_share, -- 202: NS_user, 302: NS_merchant
    sum(case when txn_type in (101,200,2000,2100,300,1006,3006,400,105) then (balance + deposit)*1.00/100 else 0 end) as bonus, -- order complete bonus
    sum(case when txn_type in (204,2004,304,2106,1003,3003,404) then (balance + deposit)*1.00/100 else 0 end) as bonus_shipper -- additional_bonus
    ,sum(case when txn_type in (512,560,900,901) then (balance + deposit)*1.00/100 else 0 end) as daily_bonus_non_hub
    ,sum(case when txn_type in (134,135,154,108,110,111) then (balance + deposit)*1.00/100 else 0 end) as tip
    ,count(case when txn_type in (134,135,108,110) and (balance + deposit) > 0 then reference_id else null end) as tip_txn

from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live 
where 1=1
and txn_type in 
(
104,201,301,2101,2001,3000,1000,401,906, -- shipping_share   
114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137, -- other bonus 
106,203,2003,303,1002,1005,1007,3002,3005,3007, -- other_payable
202,302,2002,2102,1001,402,3001, -- return_share 
101,200,2000,2100,300,1006,3006,400, -- order completed bonus 
204,2004,304,2106,1003,3003,404, -- additional bonus 
134,135,108,110, --- tipped 
512,560,900,901,907 --bonus             
) 
GROUP BY 1,2,3 
)
,hub_order_tab as
(select 
        ho.uid as shipper_id
        ,ho.slot_id
        ,ho.autopay_report_id
        ,ho.ref_order_id
        ,ho.ref_order_category
        ,case when coalesce(oct.risk_bearer_id,0) != 2 then 1 else 0 end as is_hub_order
        ,date(from_unixtime(ho.autopay_date_ts-3600)) as autopay_date
        ,date(from_unixtime(ho.create_time-3600)) as created_date
from shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_daily_s0_live ho

left join 
(select id,cast(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') as int) as risk_bearer_id 
from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
) oct on ho.ref_order_id = oct.id and ho.ref_order_category = 0
)
,f as 
(select
        date(raw.delivered_timestamp) as report_date,
        raw.order_code,
        raw.shipper_id,
        r.reference_id,
        r.partner_id,
        if(ho.ref_order_id is not null,13500,coalesce(r.shipping_share_non_hub,0)) as ship_shared,
        coalesce(r.other_bonus,0) as other_bonus,
        coalesce(r.other_payable,0) as other_payable,
        raw.order_type,
        if(ho.ref_order_id is not null,1,0) as is_hub_delivered,
        raw.group_id,
        raw.city_name

from driver_ops_raw_order_tab raw 

left join raw r on raw.id = r.reference_id and raw.shipper_id = r.partner_id

left join hub_order_tab ho on ho.ref_order_id = raw.id and ho.shipper_id = raw.shipper_id
where raw.order_status in ('Delivered','Returned','Quit')
)
,metrics as 
(select
        f.report_date,
        f.shipper_id,
        sm.city_name as shipper_city,
        if(sm.shipper_type_id = 12,'Hub','Non Hub') as driver_type,
        coalesce(driver_bonus.daily_bonus_hub + driver_bonus.daily_bonus_non_hub,0) AS driver_daily_bonus,
        -- map_agg(f.city_name,cast(count(f.city_name) as varchar)) as working_city,
        array_agg(distinct f.city_name) as working_city_list,
        count(distinct f.order_code) as total_bill,
        count(distinct case when f.is_hub_delivered > 0 then f.order_code else null end) as total_bill_hub,
        count(distinct case when order_type = 0 then f.order_code else null end) as total_bill_delivery,
        count(distinct case when order_type != 0 then f.order_code else null end) as total_bill_spxi,
        coalesce(sum(f.ship_shared),0) as total_ship_shared,
        coalesce(sum(case when f.order_type = 0 then f.ship_shared else null end),0) as ship_shared_delivery,
        coalesce(sum(case when f.order_type != 0 then f.ship_shared else null end),0) as ship_shared_spxi,

        coalesce(sum(f.other_bonus+f.other_payable),0) as total_other_income,
        coalesce(sum(case when f.order_type = 0 then f.other_bonus+f.other_payable else null end),0) as other_income_delivery,
        coalesce(sum(case when f.order_type != 0 then f.other_bonus+f.other_payable else null end),0) as other_income_spxi


from f 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = f.shipper_id and try_cast(sm.grass_date as date) = f.report_date

left join 
(select  
        hub_date,
        partner_id,
        sum(daily_bonus_hub) as daily_bonus_hub,
        sum(daily_bonus_non_hub) as daily_bonus_non_hub

from raw
group by 1,2
) driver_bonus
    on driver_bonus.partner_id = f.shipper_id 
    and driver_bonus.hub_date = f.report_date


where (
report_date between date'2023-04-30' - interval '7' day and date'2023-04-30'
or 
report_date = date'2023-05-01'
)
group by 1,2,3,4,5 
)
,k as 
(select 
         m.*,
        (m.driver_daily_bonus*1.00/m.total_bill)*total_bill_delivery as daily_bonus_delivery,
        (m.driver_daily_bonus*1.00/m.total_bill)*total_bill_spxi as daily_bonus_spxi,
        case when driver_type = 'Hub' and total_bill_hub = 0 then 'Non Hub' else driver_type end as driver_type_v2

from metrics m 
where shipper_city in ('HCM City','Ha Noi City','Da Nang City','Binh Duong','Nghe An')
)
select 
        report_date,
        shipper_city,
        -- driver_type_v2,
        count(distinct (shipper_id,report_date)) as a1,
        sum(total_bill) as total_ado,
        sum(total_bill_delivery) as delivery_ado,
        sum(total_bill_spxi) as spxi_ado,
        sum(total_bill_delivery)*1.0000/count(distinct (shipper_id,report_date)) as driver_ado_delivery,
        sum(total_ship_shared)+sum(total_other_income)+sum(driver_daily_bonus) as total_earning,
        sum(ship_shared_delivery)+sum(other_income_delivery)+sum(daily_bonus_delivery) as delivery_earning,
        sum(ship_shared_spxi)+sum(other_income_spxi)+sum(daily_bonus_spxi) as spxi_earning


from k 
group by 1,2

;
-- ver 2
select 
        report_date,
        shipper_city,
        -- driver_type_v2,
        count(distinct (shipper_id,report_date)) as a1,
        count(distinct case when total_bill_delivery > 0 then (shipper_id,report_date) else null end) as a1_delivery,
        count(distinct case when total_bill_spxi > 0 then (shipper_id,report_date) else null end) as a1_spxi,
        sum(total_bill) as total_ado,
        sum(total_bill_delivery) as delivery_ado,
        sum(total_bill_spxi) as spxi_ado,
        sum(total_bill_delivery)*1.0000/count(distinct (shipper_id,report_date)) as driver_ado_delivery,
        sum(total_ship_shared)+sum(total_other_income)+sum(driver_daily_bonus) as total_earning,
        sum(ship_shared_delivery)+sum(other_income_delivery)+sum(daily_bonus_delivery) as delivery_earning,
        sum(ship_shared_spxi)+sum(other_income_spxi)+sum(daily_bonus_spxi) as spxi_earning


from dev_vnfdbi_opsndrivers.driver_ops_driver_income_tracking_tab
where (
report_date between date'2023-04-30' - interval '7' day and date'2023-04-30'
or 
report_date = date'2023-05-01'
)
group by 1,2


