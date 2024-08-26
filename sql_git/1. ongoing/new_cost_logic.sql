with sunday_date_param as
(
    select distinct
        report_date
    from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim
    where 1=1
    and report_date between current_date - interval '90' day and current_date - interval '1' day
    and day_of_week_name = 'Sun'
    
)

,sunday_bonus as
        (select 
            case when trim(note) in ('HUB_MODEL_SHIP_30/04') then date('2021-04-30')
                            when trim(note) in ('HUB_MODEL_SHIP_05/05') then date('2021-05-05')
                            when trim(note) LIKE '%HUB_MODEL_DAILYBONUS%' then date(date_parse(substr(trim(note),22,10),'%d/%m/%Y'))
                            when trim(note) LIKE '%HUB_MODEL_EXTRASHIP%' then date(date_parse(substr(trim(note),21,10),'%d/%m/%Y'))
                            when trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_HUB%' then date(date_parse(substr(trim(note),-10,10),'%d/%m/%Y')) --- for Holiday Tet 2022
                            when trim(note) LIKE '%Thuong mung tai xe moi%' then date(date_parse(substr(trim(note),22,10),'%d/%m/%Y')) -- for new onboarding hub scheme
                            when trim(note) LIKE '%HUB_MODEL_BONUS_ADJ%' then date(date_parse(substr(trim(note),-10,10),'%d/%m/%Y')) -- for reactivate scheme 6.6
                            when replace(trim(note),' (điều chỉnh do trùng)') LIKE '%HUB_MODEL_Thuong tai xe guong mau chu nhat%' and regexp_like(replace(trim(note),' (điều chỉnh do trùng)'),'Truy thu gian lan|Truy thu do loi he thong') = false then 
                                    -- when regexp_like(REGEXP_REPLACE(trim(note),' (điều chỉnh do trùng)| (Truy thu gian lan)| (Truy thu do loi he thong)','') ,'HUB_MODEL_Thuong tai xe guong mau chu nhat') then
                                    case 
                                        when month(date(from_unixtime(create_time - 60*60))) = 1 and substr(replace(trim(replace(trim(note),' (điều chỉnh do trùng)')),' '),length(replace(trim(replace(trim(note),' (điều chỉnh do trùng)')),' '))-1,2) = '12'
                                        then date_parse(concat(substr(replace(trim(replace(trim(note),' (điều chỉnh do trùng)')),' '),length(replace(trim(replace(trim(note),' (điều chỉnh do trùng)')),' '))-4,5),'/',cast(year(date(from_unixtime(create_time - 60*60))) -1 as varchar)),'%d/%c/%Y')
                                        else date_parse(concat(substr(replace(trim(replace(trim(note),' (điều chỉnh do trùng)')),' '),length(replace(trim(replace(trim(note),' (điều chỉnh do trùng)')),' '))-4,5),'/',cast(year(date(from_unixtime(create_time - 60*60)))as varchar)),'%d/%c/%Y')
                                        end
                            when replace(trim(note),' (Truy thu gian lan)') LIKE '%HUB_MODEL_Thuong tai xe guong mau chu nhat%' and regexp_like(replace(trim(note),' (Truy thu gian lan)'),'Truy thu do loi he thong') = false  then 
                                    case 
                                        when month(date(from_unixtime(create_time - 60*60))) = 1 and substr(replace(trim(replace(trim(note),' (Truy thu gian lan)')),' '),length(replace(trim(replace(trim(note),' (Truy thu gian lan)')),' '))-1,2) = '12'
                                        then date_parse(concat(substr(replace(trim(replace(trim(note),' (Truy thu gian lan)')),' '),length(replace(trim(replace(trim(note),' (Truy thu gian lan)')),' '))-4,5),'/',cast(year(date(from_unixtime(create_time - 60*60))) -1 as varchar)),'%d/%c/%Y')
                                        else date_parse(concat(substr(replace(trim(replace(trim(note),' (Truy thu gian lan)')),' '),length(replace(trim(replace(trim(note),' (Truy thu gian lan)')),' '))-4,5),'/',cast(year(date(from_unixtime(create_time - 60*60)))as varchar)),'%d/%c/%Y')
                                        end 
                                     when replace(trim(note),' (Truy thu do loi he thong)') LIKE '%HUB_MODEL_Thuong tai xe guong mau chu nhat%' then 
                                    case 
                                        when month(date(from_unixtime(create_time - 60*60))) = 1 and substr(replace(trim(replace(trim(note),' (Truy thu do loi he thong)')),' '),length(replace(trim(replace(trim(note),' (Truy thu do loi he thong)')),' '))-1,2) = '12'
                                        then date_parse(concat(substr(replace(trim(replace(trim(note),' (Truy thu do loi he thong)')),' '),length(replace(trim(replace(trim(note),' (Truy thu do loi he thong)')),' '))-4,5),'/',cast(year(date(from_unixtime(create_time - 60*60))) -1 as varchar)),'%d/%c/%Y')
                                        else date_parse(concat(substr(replace(trim(replace(trim(note),' (Truy thu do loi he thong)')),' '),length(replace(trim(replace(trim(note),' (Truy thu do loi he thong)')),' '))-4,5),'/',cast(year(date(from_unixtime(create_time - 60*60)))as varchar)),'%d/%c/%Y')
                                        end                                     
                      else null end as date_ 

            ,*
        from (select * from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live trx
                      where(trx.note not LIKE '%HUB_MODEL_EXTRASHIP_Bù thu nhập do lỗi hệ thống%' and trx.note not LIKE '%HUB_MODEL_EXTRASHIP_Chưa nhận auto pay do sup hub điều chỉnh ca trong shift%'
                      and trx.note not LIKE '%Lỗi sai thu nhập do Work Schedules%' and trx.note not LIKE '%HUB_MODEL_EXTRASHIP_Điều chỉnh thu nhập do miss config%')
                      and cast(trx.id as bigint) not in (390114868,390114871,390114871,390114869,390114870,390114867,399878797,
                                         399878783,399878786,399878789,399878805,399878777,399878768,399878769,399878747,399878814,399878791,399878821,399878782,
                                         399878818,399878801,399878785,399878796,399878767,399878802,399878817,399878798,399878813,399878772,399878795,399878820,399878819,399878784,399878770)
                     ) trx
        where 1=1 
        and trx.txn_type in (501,518,512,519,520,560,567)
        and regexp_like(trim(note),'HUB_MODEL_Thuong tai xe guong mau chu nhat')
        -- and year(from_unixtime(create_time-3600)) =2023
        )
        ,sunday_bonus_amount_tab_p as
        (select 
            p.report_date
            ,sum(s.balance/100) as total_amount
        from sunday_date_param p
        left join sunday_bonus s
            on p.report_date = s.date_
         where s.balance/100 is null
        group by 1
        )
,ado_summary as
(select 
    date_
    ,exchange_rate
    ,sum(total_bill_food) total_bill
    ,sum(total_bill_hub_food) total_bill_hub
    ,sum(total_bill_food - total_bill_hub_food) as total_bill_non_hub
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_drivers_cpo_daily_tab cpo
where grass_date between current_date - interval '120' day and current_date - interval '1' day
group by 1,2
)
,sunday_adhoc as
(select 
        delivered_date
        -- ,exchange_rate
        ,avg(total_bill_hub) total_bill_hub
        ,sum(diff) as total_bonus
        ,sum(diff) / avg(total_bill_hub) as cpo_sunday_bonus_per_order
from (select * from dev_vnfdbi_opsndrivers.shopeefood_vn_tet_holiday_min_fee_tab_adhoc where min_fee_type = 'sunday' and grass_date >= current_date - interval '120' day ) s
inner join sunday_bonus_amount_tab_p p
    on s.delivered_date = p.report_date
left join ado_summary ado
    on s.delivered_date = ado.date_
-- where city_group_mapping = 'HP'
where 1=1
-- and delivered_date between current_date - interval '7' day and current_date - interval '1' day
and is_hub_order = 1
and autopay_date = delivered_date
and is_need_adjust_shipping_fee = 1
and source in ('Food')
group by 1
)


,driver_cost_base as 
(select 
    bf.*
    ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
    ,(case 
        when is_nan(bonus) = true then 0.00 

        when delivered_by = 'hub' then coalesce(sa.cpo_sunday_bonus_per_order,0) + (case when bf.grass_date <= date '2024-07-31' then bonus_hub else bonus_hub_v2 end) + case when bf.grass_date = date '2024-08-08' then coalesce(m.diff,0) else 0 end
        when delivered_by != 'hub' then (case when bf.grass_date <= date '2024-07-31' then bonus_non_hub else bonus_non_hub_v2 end)
        else null end)  /exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee_usd

    -- ,from_unixtime(oct.final_delivered_time-3600) as delivered_timestamp
    -- ,hour(from_unixtime(oct.final_delivered_time-3600)) as delivered_hour
    ,case when m.order_id is not null then 1 else 0 end as is_hub_surge_campaign
    ,coalesce(m.diff,0) /exchange_rate as surge_fee_hub_cp
    ,dotet.total_shipping_fee
    ,dotet.unit_fee
    ,dotet.min_fee
    ,dotet.surge_rate
    ,case 
        when bf.city_name in ('HCM', 'HN') then 13500 
        when bf.city_name in ('HP') then 12000
        else  dotet.min_fee end as min_fee_normal

    ,case when bf.grass_date <= date '2024-07-31' then bf.is_stack_group_order else god.is_actual_stack_group_order end as is_stack_group_order_new
from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join dev_vnfdbi_opsndrivers.shopeefood_bi_group_order_detail_tab god
    on bf.group_id = god.group_id and bf.order_id = god.ref_order_id
left join (select order_id, sub_source, ref_order_category from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level 
           where grass_date between date_trunc('month',current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
          ) pbf
    on bf.order_id = pbf.order_id and bf.sub_source = pbf.sub_source
left join (select * from dev_vnfdbi_opsndrivers.shopeefood_vn_tet_holiday_min_fee_tab_adhoc where min_fee_type = 'spike' and grass_date >= current_date - interval '120' day)m
    on bf.order_id = m.order_id and m.source in ('Food')
left JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = bf.order_id 
    and dot.ref_order_category = pbf.ref_order_category
    --  and dot.submitted_time > 1609439493
left join (SELECT order_id
                ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
            ,cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ) as hub_id
            
            from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet
            
            )dotet on dot.id = dotet.order_id
left join sunday_adhoc sa
    on bf.grass_date = sa.delivered_date and bf.source in ('Food')
where bf.grass_date between date_trunc('month',current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
and bf.sub_source in ('Food','NS Instant','SPX Portal','Now Ship Shopee')
)


,raw as
(select 
    d.*
    ,(case 
        when delivered_by = 'hub' then 0 
        when unit_fee = 0 then 0
        when delivered_by != 'hub' and is_stack_group_order_new != 0 then 0
        else (greatest(min_fee,unit_fee * distance * surge_rate) - greatest(min_fee_normal,unit_fee * distance * surge_rate))  end) / exchange_rate as surge_fee_non_hub_cp

    ,case when (dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee_usd) > 0 then 1 else 0 end as is_surge_pass_through
from driver_cost_base d
)
,order_stacking_and_hub_raw as
(select 
    date_trunc('month',grass_date) as report_month
    ,grass_date
    ,case 
        when sub_source in ('Food','Market') then 'Food'
        when sub_source in ('NS Instant','SPX Portal') then 'C2C'
        when sub_source in ('Now Ship Shopee') then 'Ecom' end as source

    ,delivered_by
    ,case when is_stack_group_order_new in (1,2) then 'stack' else 'non-stack' end as order_type
    ,cast(count(distinct order_id) as double) as total_orders

    ,cast(sum(dr_cost_base_usd + dr_cost_surge_usd + dr_cost_bonus_usd - coalesce(surge_fee_hub_cp,0) - coalesce(surge_fee_non_hub_cp,0)) as double)*(-1) as total_cost
    ,cast(sum(surge_fee_hub_cp + surge_fee_non_hub_cp +dr_cost_bw_fee_usd  + dr_cost_holiday_fee_usd) as double)*(-1) as total_surge  --- min_fee + bad weather + holiday
    ,cast(sum(coalesce(surge_fee_hub_cp,0) + coalesce(surge_fee_non_hub_cp,0)) as double)*(-1) as total_surge_min_fee
    ,cast(sum(dr_cost_bw_fee_usd  + dr_cost_holiday_fee_usd + dr_cost_late_night_usd) as double)*(-1) as total_surge_bwf_hldf
    

    ,cast(sum(dr_cost_base_usd + dr_cost_surge_usd - coalesce(surge_fee_hub_cp,0) - coalesce(surge_fee_non_hub_cp,0)) as double)*(-1) as total_cost_excl_surge
    ,cast(sum(dr_cost_bonus_usd) as double) * (-1) as total_driver_cost_bonus
    ,cast(sum(coalesce(surge_fee_hub_cp,0) + coalesce(surge_fee_non_hub_cp,0) + COALESCE(dr_cost_late_night_usd,0) + coalesce(dr_cost_bw_fee_usd,0) + coalesce(dr_cost_holiday_fee_usd,0)) as double)*(-1) as total_cost_surge

    
    ,count(distinct grass_date) as num_days

from raw
group by 1,2,3,4,5
)


-- where order_id = 50682104
-- -- where surge_fee_non_hub_cp > 0 
-- where 1=1
-- and source not in ('Food','Market')

-- select 
--     grass_date
--     ,case 
--         when sub_source in ('Food','Market') then 'Food'
--         when sub_source in ('NS Instant','SPX Portal') then 'C2C'
--         when sub_source in ('Now Ship Shopee') then 'Ecom' end as service
--     ,DATE_TRUNC('month',grass_date) as grass_month
--     ,sum(case when is_surge_pass_through = 1 then (dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee) else 0 end) as surge_buyer
--     ,sum(case when surge_fee_non_hub_cp > 0 or surge_fee_hub_cp >0 or (dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee) > 0 then coalesce(surge_fee_non_hub_cp,0)/exchange_rate  + coalesce(surge_fee_hub_cp,0)/exchange_rate +(dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee) else 0 end) as surge_driver
--     ,sum(case when surge_fee_non_hub_cp > 0 or surge_fee_hub_cp >0  then coalesce(surge_fee_non_hub_cp,0)/exchange_rate  + coalesce(surge_fee_hub_cp,0)/exchange_rate else 0 end) as surge_cp_min_fee
--     ,sum(case when (dr_cost_bw_fee_usd + dr_cost_holiday_fee) > 0  then (dr_cost_bw_fee_usd + dr_cost_holiday_fee)  else 0 end) as surge_bw_hld_fee
--     -- ,count(distinct case when is_surge_pass_through = 1 then order_id else null end) as order_surge_buyer
--     -- ,count(distinct case when surge_fee_non_hub_cp > 0 or surge_fee_hub_cp >0 or (dr_cost_bw_fee_usd + dr_cost_late_night_usd + dr_cost_holiday_fee) > 0 then order_id  else null end) as order_surge_driver
--     ,count(distinct order_id) as total_orders
-- from raw
-- -- where status = 7 --> net order
-- group by 1,2,3
-- where grass_date = date '2023-09-09'
-- and surge_fee_campaign_non_hub > 0



,order_stacking_and_hub_raw_flattern as
(select 
    report_month

    -- CPO by service
    ,sum(case when source = 'Food' then total_cost_excl_surge else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_cpo_excl_surge
    ,sum(case when source = 'Food' then total_driver_cost_bonus else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_cpo_bonus
    ,sum(case when source = 'Food' then total_cost_surge else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_cpo_surge

    ,sum(case when source = 'Ecom' then total_cost_excl_surge else 0 end) / sum(case when source = 'Ecom' then total_orders else 0 end) as ecom_cpo_excl_surge
    ,sum(case when source = 'Ecom' then total_driver_cost_bonus else 0 end) / sum(case when source = 'Ecom' then total_orders else 0 end) as ecom_cpo_bonus
    ,sum(case when source = 'Ecom' then total_cost_surge else 0 end) / sum(case when source = 'Ecom' then total_orders else 0 end) as ecom_cpo_surge

    ,sum(case when source = 'C2C' then total_cost_excl_surge else 0 end) / sum(case when source = 'C2C' then total_orders else 0 end) as c2c_cpo_excl_surge
    ,sum(case when source = 'C2C' then total_driver_cost_bonus else 0 end) / sum(case when source = 'C2C' then total_orders else 0 end) as c2c_cpo_bonus
    ,sum(case when source = 'C2C' then total_cost_surge else 0 end) / sum(case when source = 'C2C' then total_orders else 0 end) as c2c_cpo_surge

    -- surge due to bwf + hldf
    
    ,sum(case when source = 'Food' then total_surge_bwf_hldf else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_cpo_surge_bwf_hldf
    ,sum(case when source = 'Ecom' then total_surge_bwf_hldf else 0 end) / sum(case when source = 'Ecom' then total_orders else 0 end) as ecom_cpo_bwf_hldf
    ,sum(case when source = 'C2C' then total_surge_bwf_hldf else 0 end) / sum(case when source = 'C2C' then total_orders else 0 end) as c2c_cpo_bwf_hldf

    -- Food
    -- % ADO Food
    ,sum(case when source = 'Food' and delivered_by = 'hub' then total_orders else 0 end)/count(distinct grass_date) as food_ado_hub
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type = 'stack' then total_orders else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_ado_hub_stack
    ,sum(case when source = 'Food' and delivered_by = 'hub' and order_type != 'stack' then total_orders else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_ado_hub_single
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_ado_non_hub_stack
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) / sum(case when source = 'Food' then total_orders else 0 end) as food_ado_non_hub_single
    
    -- CPO Food

    ,sum(case when source = 'Food' and delivered_by = 'hub' then total_cost_excl_surge +  total_driver_cost_bonus else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' then total_orders else 0 end) as food_cpo_hub_stack
    ,sum(case when source = 'Food' and delivered_by = 'hub'  then total_cost_excl_surge +  total_driver_cost_bonus  else 0 end) / sum(case when source = 'Food' and delivered_by = 'hub' then total_orders else 0 end) as food_cpo_hub_single
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_cost_excl_surge +  total_driver_cost_bonus  else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as food_cpo_non_hub_stack
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_cost_excl_surge +  total_driver_cost_bonus  else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as food_cpo_non_hub_single


        -- CPO Food - surge
    ,sum(case when source = 'Food' and delivered_by = 'hub' then total_cost_surge else 0 end) / sum(case when source = 'Food'  and delivered_by = 'hub' then total_orders else 0 end) as food_cpo_surge_hub_stack
    ,sum(case when source = 'Food' and delivered_by = 'hub'then total_cost_surge else 0 end) / sum(case when source = 'Food' and delivered_by = 'hub' then total_orders else 0 end) as food_cpo_surge_hub_single
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_cost_surge else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as food_cpo_surge_non_hub_stack
    ,sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_cost_surge else 0 end) / sum(case when source = 'Food' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as food_cpo_surge_non_hub_single

    -- Ecom
    -- % ADO Ecom
    ,sum(case when source = 'Ecom' and delivered_by = 'hub' then total_orders else 0 end) / sum(case when source = 'Ecom' then total_orders else 0 end) as ecom_ado_hub_stack
    ,sum(case when source = 'Ecom' and delivered_by = 'hub' then total_orders else 0 end) / sum(case when source = 'Ecom' then total_orders else 0 end) as ecom_ado_hub_single
    ,sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) / sum(case when source = 'Ecom' then total_orders else 0 end) as ecom_ado_non_hub_stack
    ,sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) / sum(case when source = 'Ecom' then total_orders else 0 end) as ecom_ado_non_hub_single
    
    -- CPO Ecom

    ,sum(case when source = 'Ecom' and delivered_by = 'hub' then total_cost_excl_surge +  total_driver_cost_bonus else 0 end) / sum(case when source = 'Ecom'  and delivered_by = 'hub' then total_orders else 0 end) as ecom_cpo_hub_stack
    ,sum(case when source = 'Ecom' and delivered_by = 'hub' then total_cost_excl_surge +  total_driver_cost_bonus  else 0 end) / sum(case when source = 'Ecom' and delivered_by = 'hub' then total_orders else 0 end) as ecom_cpo_hub_single
    ,sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type = 'stack' then total_cost_excl_surge +  total_driver_cost_bonus  else 0 end) / sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as ecom_cpo_non_hub_stack
    ,sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type != 'stack' then total_cost_excl_surge +  total_driver_cost_bonus  else 0 end) / sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as ecom_cpo_non_hub_single


        -- CPO Ecom - surge
    ,sum(case when source = 'Ecom' and delivered_by = 'hub' then total_cost_surge else 0 end) / sum(case when source = 'Ecom'  and delivered_by = 'hub' then total_orders else 0 end) as ecom_cpo_hub_stack
    ,sum(case when source = 'Ecom' and delivered_by = 'hub' then total_cost_surge else 0 end) / sum(case when source = 'Ecom' and delivered_by = 'hub' then total_orders else 0 end) as ecom_cpo_surge_hub_single
    ,sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type = 'stack' then total_cost_surge else 0 end) / sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type = 'stack' then total_orders else 0 end) as ecom_cpo_surge_non_hub_stack
    ,sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type != 'stack' then total_cost_surge else 0 end) / sum(case when source = 'Ecom' and delivered_by != 'hub' and order_type != 'stack' then total_orders else 0 end) as ecom_cpo_surge_non_hub_single
    

from order_stacking_and_hub_raw
group by 1
)
select 
    *
from order_stacking_and_hub_raw_flattern
;