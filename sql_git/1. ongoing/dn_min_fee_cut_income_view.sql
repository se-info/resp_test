with raw as 
(select 
        date_trunc('month',ic.date_) as month_,
        ic.date_,
        case 
        when ic.city_name_full in ('HCM City','Ha Noi City','Da Nang City') then 'T1'
        when ic.city_name_full in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Binh Duong','Vung Tau') then 'T2'
        else 'OTH' end as cities,
        ic.current_driver_tier,
        ic.total_earning_before_tax,
        greatest(dp.online_hour,dp.work_hour) as online_hour,
        ic.partner_id,
        ic.city_name_full,
        total_bill,
        (total_bill_food + total_bill_market) as total_delivery_order,
        (total_bill_now_ship+total_bill_now_ship_shopee+total_bill_now_ship_instant+total_bill_now_ship_food_merchant+total_bill_now_ship_sameday) as total_spxi_order
from vnfdbi_opsndrivers.snp_foody_shipper_income_tab ic 

left join dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab dp
    on dp.shipper_id = ic.partner_id
    and dp.report_date = ic.date_
where 1 = 1 
and date_ >= date'2023-05-01'
and date_ <= date'2024-04-30'
and regexp_like(city_name_full,'Dien Bien|Stress|Test|test|null') = false 
order by 2 desc 
)
-- select * from raw 
select 
        month_,
        cities,
        city_name_full,
        current_driver_tier,
        count(partner_id)*1.00/count(distinct date_) as avg_a1,
        sum(total_earning_before_tax)*1.00/count(distinct (partner_id,date_)) as avg_earning_daily,
        sum(total_earning_before_tax)*1.00/count(distinct (partner_id)) as avg_earning_monthly,
        sum(online_hour)*1.00/count(distinct (partner_id,date_)) as avg_online_hour,
        sum(total_bill)*1.00/count(distinct (partner_id,date_)) as ado_all,
        sum(total_delivery_order)*1.00/count(distinct (partner_id,date_)) as ado_delivery,
        sum(total_spxi_order)*1.00/count(distinct (partner_id,date_)) as ado_spxi
from raw 

where current_driver_tier is not null 
group by 1,2,3,4


