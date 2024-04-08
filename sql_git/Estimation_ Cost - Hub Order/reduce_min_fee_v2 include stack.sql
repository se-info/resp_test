drop table if exists dev_vnfdbi_opsndrivers.stack_order_recalculation;
create table if not exists dev_vnfdbi_opsndrivers.stack_order_recalculation as
with base as
(select 
    food.*
    ,date(from_unixtime(dot.submitted_time-3600)) as created_date
    ,dot.delivery_cost/100 as delivery_cost
    ,district.name_en as district_name
    --,if(distance <= 3 , '1. 0 - 3km', '2. 3km+') distance_range 
    ,case when distance between 0 and 2.3 then 1 
          when distance between 2.3 and 3 then 2 
          else 3 end as distance_range
    ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as dotet_total_shipping_fee
    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate
    ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
    -- ,dotet.order_data
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level food
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    on food.order_id = dot.ref_order_id  and dot.ref_order_category = 0
left join shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
    on dot.id = dotet.order_id
Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = food.pick_district_id
where food.grass_date between date '2022-12-01' and date '2022-12-31' and grass_date != date '2022-12-12'
-- and city_name_full in ('Nghe An', 'Lam Dong')
and source in ('Food')
and delivered_by != 'hub'
)

,shipping_fee_base as
(select 
    base.*
    ,GREATEST(13500,unit_fee*distance*surge_rate) as cal_system_param
    ,GREATEST(13500,
        (case 
            when city_name in ('HCM','HN') and unit_fee >= 3950 then 3950 
            when city_name in ('DN','OTH') and unit_fee >= 3900 then 3900
            else unit_fee end)
        *distance*surge_rate) as cal_current
    
    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3950 then 3950 
            when city_name in ('DN','OTH') and unit_fee >= 3900 then 3900
            else unit_fee end unit_fee_current
    ,13500 as min_fee_current

    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3950 then GREATEST(13500,3950*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3950 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3850 then GREATEST(13500,3850*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3850 then GREATEST(13500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt1

    ,case 
        when city_name in ('HCM','HN') and unit_fee >= 3950 then 3950
        when city_name in ('HCM','HN') and unit_fee < 3950 then unit_fee
        when city_name in ('DN','OTH') and unit_fee >= 3850 then 3850
        when city_name in ('HCM','HN') and unit_fee < 3850 then unit_fee

            else unit_fee end as unit_fee_opt1

    ,13500 as min_fee_opt1
    
    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3900 then GREATEST(13500,3900*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3900 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3800 then GREATEST(13500,3800*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3800 then GREATEST(13500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt2

    ,case 
        when city_name in ('HCM','HN') and unit_fee >= 3900 then 3900
        when city_name in ('HCM','HN') and unit_fee < 3900 then unit_fee
        when city_name in ('DN','OTH') and unit_fee >= 3800 then 3800
        when city_name in ('HCM','HN') and unit_fee < 3800 then unit_fee

            else unit_fee end as unit_fee_opt2
            
    ,13500 as min_fee_opt2

    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3850 then GREATEST(13500,3850*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3850 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3750 then GREATEST(13500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(13500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt3
    
    ,case 
        when city_name in ('HCM','HN') and unit_fee >= 3850 then 3850
        when city_name in ('HCM','HN') and unit_fee < 3850 then unit_fee
        when city_name in ('DN','OTH') and unit_fee >= 3750 then 3750
        when city_name in ('HCM','HN') and unit_fee < 3750 then unit_fee

            else unit_fee end as unit_fee_opt3
            
    ,13500 as min_fee_opt3
    
    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3800 then GREATEST(13500,3800*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3800 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3750 then GREATEST(12500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(12500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt4

    ,case 
        when city_name in ('HCM','HN') and unit_fee >= 3800 then 3800
        when city_name in ('HCM','HN') and unit_fee < 3800 then unit_fee
        when city_name in ('DN','OTH') and unit_fee >= 3750 then 3750
        when city_name in ('HCM','HN') and unit_fee < 3750 then unit_fee
        else unit_fee end as unit_fee_opt4
            
    ,case 
        when city_name in ('HCM','HN') then 13500
        when city_name in ('HCM','HN') then 12500
        else unit_fee end as min_fee_opt4
    
    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3750 then GREATEST(13500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3750 then GREATEST(12500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(12500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt5
    
    ,case 
        when city_name in ('HCM','HN') and unit_fee >= 3750 then 3750
        when city_name in ('HCM','HN') and unit_fee < 3750 then unit_fee
        when city_name in ('DN','OTH') and unit_fee >= 3750 then 3750
        when city_name in ('HCM','HN') and unit_fee < 3750 then unit_fee
        else unit_fee end as unit_fee_opt5
            
    ,case 
        when city_name in ('HCM','HN') then 13500
        when city_name in ('HCM','HN') then 12500
        else unit_fee end as min_fee_opt5
    
    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3750 then GREATEST(12500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(12500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3750 then GREATEST(12500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(12500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt6
        
    ,case 
        when city_name in ('HCM','HN') and unit_fee >= 3750 then 3750
        when city_name in ('HCM','HN') and unit_fee < 3750 then unit_fee
        when city_name in ('DN','OTH') and unit_fee >= 3750 then 3750
        when city_name in ('HCM','HN') and unit_fee < 3750 then unit_fee
        else unit_fee end as unit_fee_opt6
            
    ,case 
        when city_name in ('HCM','HN') then 12500
        when city_name in ('HCM','HN') then 12500
        else unit_fee end as min_fee_opt6

    ,row_number() over (partition by group_id order by order_id desc) as row_num 
    
from base

)
,stack_order_recalculation as
(select 
    raw.group_id
    
    ,raw.order_id as order_id_first
    ,raw.cal_current as first_cal_current
    ,raw.cal_opt1 as first_cal_opt1
    ,raw.cal_opt2 as first_cal_opt2
    ,raw.cal_opt3 as first_cal_opt3
    ,raw.cal_opt4 as first_cal_opt4
    ,raw.cal_opt5 as first_cal_opt5
    ,raw.cal_opt6 as first_cal_opt6

    ,raw.unit_fee_current
    ,raw.unit_fee_opt1
    ,raw.unit_fee_opt2
    ,raw.unit_fee_opt3
    ,raw.unit_fee_opt4
    ,raw.unit_fee_opt5
    ,raw.unit_fee_opt6

    ,raw.min_fee_current
    ,raw.min_fee_opt1
    ,raw.min_fee_opt2
    ,raw.min_fee_opt3
    ,raw.min_fee_opt4
    ,raw.min_fee_opt5
    ,raw.min_fee_opt6

    ,raw.surge_rate

    ,raw2.order_id as order_id_second
    ,raw2.cal_current as second_cal_current
    ,raw2.cal_opt1 as second_cal_opt1
    ,raw2.cal_opt2 as second_cal_opt2
    ,raw2.cal_opt3 as second_cal_opt3
    ,raw2.cal_opt4 as second_cal_opt4
    ,raw.cal_opt5 as second_cal_opt5
    ,raw2.cal_opt6 as second_cal_opt6

    -- A = Fee_1 + (Fee_2 / RE) * rate_a
    -- B = (Fee_1 / RE) * rate_b + Fee_2"
    -- Min.group shipping fee = MAX (A , B)

    ,raw.distance as first_distance
    ,raw2.distance as second_distance
    ,raw.distance_grp
    ,raw.distance_all
    ,raw.distance_all/raw.distance_grp as re_stack_cal

    ,cast(json_extract(re.extra_data,'$.re') as double) re_stack_system 
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double) as surge_rate_group
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as double) as unit_fee_group

    ,cast(json_extract(re.extra_data,'$.ship_fee_info.min_fee') as double) as c_min_group_shipping_fee

    ,raw.cal_current + raw2.cal_current/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_a_current
    ,raw2.cal_current + raw.cal_current/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_b_current
    ,greatest(raw.cal_current + raw2.cal_current/cast(json_extract(re.extra_data,'$.re') as double)*0.55 , raw2.cal_current + raw.cal_current/cast(json_extract(re.extra_data,'$.re') as double)*0.55) as min_group_shipping_fee_current
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)*raw.distance_grp*raw.unit_fee_current as a_spf_by_stacked_distance_current


    ,raw.cal_opt1 + raw2.cal_opt1/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_a_opt1
    ,raw2.cal_opt1 + raw.cal_opt1/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_b_opt1
    ,greatest(raw.cal_opt1 + raw2.cal_opt1/cast(json_extract(re.extra_data,'$.re') as double)*0.55 , raw2.cal_opt1 + raw.cal_opt1/cast(json_extract(re.extra_data,'$.re') as double)*0.55) as min_group_shipping_fee_opt1
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)*raw.distance_grp*raw.unit_fee_opt1 as a_spf_by_stacked_distance_opt1

    ,raw.cal_opt2 + raw2.cal_opt2/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_a_opt2
    ,raw2.cal_opt2 + raw.cal_opt2/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_b_opt2
    ,greatest(raw.cal_opt2 + raw2.cal_opt2/cast(json_extract(re.extra_data,'$.re') as double)*0.55 , raw2.cal_opt2 + raw.cal_opt2/cast(json_extract(re.extra_data,'$.re') as double)*0.55) as min_group_shipping_fee_opt2
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)*raw.distance_grp*raw.unit_fee_opt2 as a_spf_by_stacked_distance_opt2

    ,raw.cal_opt3 + raw2.cal_opt3/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_a_opt3
    ,raw2.cal_opt3 + raw.cal_opt3/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_b_opt3
    ,greatest(raw.cal_opt3 + raw2.cal_opt3/cast(json_extract(re.extra_data,'$.re') as double)*0.55 , raw2.cal_opt3 + raw.cal_opt3/cast(json_extract(re.extra_data,'$.re') as double)*0.55) as min_group_shipping_fee_opt3
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)*raw.distance_grp*raw.unit_fee_opt3 as a_spf_by_stacked_distance_opt3

    ,raw.cal_opt4 + raw2.cal_opt4/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_a_opt4
    ,raw2.cal_opt4 + raw.cal_opt4/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_b_opt4
    ,greatest(raw.cal_opt4 + raw2.cal_opt4/cast(json_extract(re.extra_data,'$.re') as double)*0.55 , raw2.cal_opt4 + raw.cal_opt4/cast(json_extract(re.extra_data,'$.re') as double)*0.55) as min_group_shipping_fee_opt4
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)*raw.distance_grp*raw.unit_fee_opt4 as a_spf_by_stacked_distance_opt4

    ,raw.cal_opt5 + raw2.cal_opt5/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_a_opt5
    ,raw2.cal_opt5 + raw.cal_opt5/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_b_opt5
    ,greatest(raw.cal_opt5 + raw2.cal_opt5/cast(json_extract(re.extra_data,'$.re') as double)*0.55 , raw2.cal_opt5 + raw.cal_opt5/cast(json_extract(re.extra_data,'$.re') as double)*0.55) as min_group_shipping_fee_opt5
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)*raw.distance_grp*raw.unit_fee_opt5 as a_spf_by_stacked_distance_opt5

    ,raw.cal_opt6 + raw2.cal_opt6/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_a_opt6
    ,raw2.cal_opt6 + raw.cal_opt6/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_b_opt6
    ,greatest(raw.cal_opt6 + raw2.cal_opt6/cast(json_extract(re.extra_data,'$.re') as double)*0.55 , raw2.cal_opt6 + raw.cal_opt6/cast(json_extract(re.extra_data,'$.re') as double)*0.55) as min_group_shipping_fee_opt6
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)*raw.distance_grp*raw.unit_fee_opt6 as a_spf_by_stacked_distance_opt6

from shipping_fee_base raw
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) re
    on re.id = raw.group_id
left join (select * from shipping_fee_base raw where raw.is_stack_group_order = 2 and raw.order_in_groups = 2 and row_num !=1) raw2
    on raw.group_id = raw2.group_id
where raw.is_stack_group_order = 2 and raw.order_in_groups = 2
and raw.row_num = 1
)
select
    *
from stack_order_recalculation
;

with stack_order as
(select 
    st.*
    ,greatest(least(a_spf_by_stacked_distance_current,(first_cal_current + second_cal_current)),c_min_group_shipping_fee) + 2000 as stack_fee_current
    ,greatest(least(a_spf_by_stacked_distance_opt1,(first_cal_opt1 + second_cal_opt1)),min_group_shipping_fee_opt1) + 2000 as stack_fee_opt1
    ,greatest(least(a_spf_by_stacked_distance_opt2,(first_cal_opt2 + second_cal_opt2)),min_group_shipping_fee_opt2) + 2000 as stack_fee_opt2
    ,greatest(least(a_spf_by_stacked_distance_opt3,(first_cal_opt3 + second_cal_opt3)),min_group_shipping_fee_opt3) + 2000 as stack_fee_opt3
    ,greatest(least(a_spf_by_stacked_distance_opt4,(first_cal_opt4 + second_cal_opt4)),min_group_shipping_fee_opt4) + 2000 as stack_fee_opt4
    ,greatest(least(a_spf_by_stacked_distance_opt5,(first_cal_opt5 + second_cal_opt5)),min_group_shipping_fee_opt5) + 2000 as stack_fee_opt5
    ,greatest(least(a_spf_by_stacked_distance_opt6,(first_cal_opt6 + second_cal_opt6)),min_group_shipping_fee_opt6) + 2000 as stack_fee_opt6

    -- frist order stack fee:
    ,(greatest(least(a_spf_by_stacked_distance_current,(first_cal_current + second_cal_current)),c_min_group_shipping_fee) + 2000)*first_distance/(first_distance+second_distance) as first_stack_fee_current
    ,(greatest(least(a_spf_by_stacked_distance_opt1,(first_cal_opt1 + second_cal_opt1)),min_group_shipping_fee_opt1) + 2000)*first_distance/(first_distance+second_distance) as first_stack_fee_opt1
    ,(greatest(least(a_spf_by_stacked_distance_opt2,(first_cal_opt2 + second_cal_opt2)),min_group_shipping_fee_opt2) + 2000)*first_distance/(first_distance+second_distance) as first_stack_fee_opt2
    ,(greatest(least(a_spf_by_stacked_distance_opt3,(first_cal_opt3 + second_cal_opt3)),min_group_shipping_fee_opt3) + 2000)*first_distance/(first_distance+second_distance) as first_stack_fee_opt3
    ,(greatest(least(a_spf_by_stacked_distance_opt4,(first_cal_opt4 + second_cal_opt4)),min_group_shipping_fee_opt4) + 2000)*first_distance/(first_distance+second_distance) as first_stack_fee_opt4
    ,(greatest(least(a_spf_by_stacked_distance_opt5,(first_cal_opt5 + second_cal_opt5)),min_group_shipping_fee_opt5) + 2000)*first_distance/(first_distance+second_distance) as first_stack_fee_opt5
    ,(greatest(least(a_spf_by_stacked_distance_opt6,(first_cal_opt6 + second_cal_opt6)),min_group_shipping_fee_opt6) + 2000)*first_distance/(first_distance+second_distance) as first_stack_fee_opt6

    -- second order stack fee:
    ,(greatest(least(a_spf_by_stacked_distance_current,(first_cal_current + second_cal_current)),c_min_group_shipping_fee) + 2000)*second_distance/(first_distance+second_distance) as second_stack_fee_current
    ,(greatest(least(a_spf_by_stacked_distance_opt1,(first_cal_opt1 + second_cal_opt1)),min_group_shipping_fee_opt1) + 2000)*second_distance/(first_distance+second_distance) as second_stack_fee_opt1
    ,(greatest(least(a_spf_by_stacked_distance_opt2,(first_cal_opt2 + second_cal_opt2)),min_group_shipping_fee_opt2) + 2000)*second_distance/(first_distance+second_distance) as second_stack_fee_opt2
    ,(greatest(least(a_spf_by_stacked_distance_opt3,(first_cal_opt3 + second_cal_opt3)),min_group_shipping_fee_opt3) + 2000)*second_distance/(first_distance+second_distance) as second_stack_fee_opt3
    ,(greatest(least(a_spf_by_stacked_distance_opt4,(first_cal_opt4 + second_cal_opt4)),min_group_shipping_fee_opt4) + 2000)*second_distance/(first_distance+second_distance) as second_stack_fee_opt4
    ,(greatest(least(a_spf_by_stacked_distance_opt5,(first_cal_opt5 + second_cal_opt5)),min_group_shipping_fee_opt5) + 2000)*second_distance/(first_distance+second_distance) as second_stack_fee_opt5
    ,(greatest(least(a_spf_by_stacked_distance_opt6,(first_cal_opt6 + second_cal_opt6)),min_group_shipping_fee_opt6) + 2000)*second_distance/(first_distance+second_distance) as second_stack_fee_opt6

from dev_vnfdbi_opsndrivers.stack_order_recalculation st
)
,base as
(select 
    food.*
    ,date(from_unixtime(dot.submitted_time-3600)) as created_date
    ,dot.delivery_cost/100 as delivery_cost
    ,district.name_en as district_name
    --,if(distance <= 3 , '1. 0 - 3km', '2. 3km+') distance_range 
    ,case when distance between 0 and 2.3 then 1 
          when distance between 2.3 and 3 then 2 
          else 3 end as distance_range
    ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as dotet_total_shipping_fee
    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate
    ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
    -- ,dotet.order_data
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level food
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    on food.order_id = dot.ref_order_id  and dot.ref_order_category = 0
left join shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
    on dot.id = dotet.order_id
Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = food.pick_district_id
where food.grass_date between date '2022-12-01' and date '2022-12-31' and grass_date != date '2022-12-12'
-- and city_name_full in ('Nghe An', 'Lam Dong')
and source in ('Food')
and delivered_by != 'hub'
)

,shipping_fee_base as
(select 
    base.*
    ,GREATEST(13500,unit_fee*distance*surge_rate) as cal_system_param
    ,GREATEST(13500,
        (case 
            when city_name in ('HCM','HN') and unit_fee >= 3950 then 3950 
            when city_name in ('DN','OTH') and unit_fee >= 3900 then 3900
            else unit_fee end)
        *distance*surge_rate) as cal_current
    

    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3950 then GREATEST(13500,3950*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3950 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3850 then GREATEST(13500,3850*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3850 then GREATEST(13500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt1

    
    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3900 then GREATEST(13500,3900*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3900 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3800 then GREATEST(13500,3800*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3800 then GREATEST(13500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt2



    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3850 then GREATEST(13500,3850*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3850 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3750 then GREATEST(13500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(13500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt3
    
    
    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3800 then GREATEST(13500,3800*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3800 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3750 then GREATEST(12500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(12500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt4

    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3750 then GREATEST(13500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(13500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3750 then GREATEST(12500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(12500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt5

    
    ,case 
            when city_name in ('HCM','HN') and unit_fee >= 3750 then GREATEST(12500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(12500,unit_fee*distance*surge_rate)
            when city_name in ('DN','OTH') and unit_fee >= 3750 then GREATEST(12500,3750*distance*surge_rate)
            when city_name in ('HCM','HN') and unit_fee < 3750 then GREATEST(12500,unit_fee*distance*surge_rate)

            else unit_fee end as cal_opt6
    from base

)
,final as
(select 
    b.order_id
    ,b.city_name

    ,coalesce(s1.first_stack_fee_current,s2.second_stack_fee_current,cal_current) as cal_current
    ,coalesce(s1.first_stack_fee_opt1,s2.second_stack_fee_opt1,cal_opt1) as cal_opt1
    ,coalesce(s1.first_stack_fee_opt2,s2.second_stack_fee_opt2,cal_opt2) as cal_opt2
    ,coalesce(s1.first_stack_fee_opt3,s2.second_stack_fee_opt3,cal_opt3) as cal_opt3
    ,coalesce(s1.first_stack_fee_opt4,s2.second_stack_fee_opt4,cal_opt4) as cal_opt4
    ,coalesce(s1.first_stack_fee_opt5,s2.second_stack_fee_opt5,cal_opt5) as cal_opt5
    ,coalesce(s1.first_stack_fee_opt6,s2.second_stack_fee_opt6,cal_opt6) as cal_opt6

from shipping_fee_base b
left join stack_order s1
    on b.order_id = s1.order_id_first
left join stack_order s2
    on b.order_id = s2.order_id_second
)
select  
    city_name
    ,count(distinct order_id) as total_orders
    ,sum(cal_current) cal_current
    ,sum(cal_opt1) cal_opt1
    ,sum(cal_opt2) as cal_opt2
    ,sum(cal_opt3) cal_opt3
    ,sum(cal_opt4) cal_opt4
    ,sum(cal_opt5) cal_opt5
    ,sum(cal_opt6) cal_opt6
from final
group by 1

