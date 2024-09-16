with raw as
(
select 
    d.shipper_id
    ,d.shopee_uid
    ,d.city_name
    ,d.shipper_tier
    ,bf.grass_date
    ,sm.shipper_name
    ,coalesce(t2.completed_rate/100.00,0.00) sla
    ,count(distinct order_id) as total_orders
    ,count(distinct case when bf.source = 'Now Ship' then order_id else null end) as ship_orders 
    ,count(distinct case when bf.source = 'Now Ship Shopee' then order_id else null end) as shopee_orders
    ,count(distinct case when bf.source = 'Portal' then order_id else null end) as shopee_portal_orders
    ,count(distinct case when bf.source not in ('Portal','Now Ship Shopee','Now Ship') then order_id else null end) as delivery_orders


from dev_vnfdbi_opsndrivers.driver_ops_yagi_driver_list d
left join 
(select 
        bf.grass_date,
        bf.order_id,
        case 
        when sp.booking_type = 5 then 'Portal'
        else bf.source end as source,
        bf.partner_id,
        bf.city_name_full
        

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 
left join shopeefood.foody_express_db__shopee_booking_tab__reg_continuous_s0_live sp on sp.id = bf.order_id and bf.ref_order_category = 6
) bf on bf.partner_id = cast(d.shipper_id as bigint)


left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live t2 
    on bf.partner_id = t2.uid and bf.grass_date = date(from_unixtime(t2.report_date-3600))
left join shopeefood.foody_mart__profile_shipper_master sm
    on bf.grass_Date = TRY_CAST(sm.grass_date as date) and bf.partner_id = sm.shipper_id

where bf.grass_date between date '2024-09-10' and date '2024-09-15'
and city_name_full in ('Ha Noi City','Thai Nguyen','Hai Phong City', 'Quang Ninh', 'Bac Ninh','Hai Duong')
group by 1,2,3,4,5,6,7
)
,base_ as
(select 
    shipper_id
    ,shipper_name
    -- ,shopee_uid
    ,city_name
    ,grass_date
    ,total_orders
    ,ship_orders
    ,shopee_orders
    ,shopee_portal_orders
    ,delivery_orders
    ,sla
    ,case 
    when city_name = 'Hai Duong' then 1 
    when sla >= 90 then 1 else 0 
    end as is_eligible_sla
    ,count(grass_date) over(partition by shipper_id) as total_working_days

from raw
)
select * from
(select 
    b.*
    ,case 
        when total_orders < 13 then 0
        when total_orders < 16 then 40000
        when total_orders < 20 then 60000
        when total_orders >= 20 then 100000
        end as bonus_
    ,'spf_do_0012|Gia tang thu nhap cho bac tai bi anh huong bao_'||date_format(grass_date,'%Y-%m-%d') as txn_note

from base_ b
where total_working_days >= 3
and is_eligible_sla = 1
)
where bonus_ > 0 
