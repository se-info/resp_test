

/*
stacked fee = MAX( MIN( Total Distance * Unit Fee* surge Rate , SUM(Fee_A + Fee_B) ), Min Group Shipping Fee )

Min Group Shipping Fee= MAX (A , B)

A = Fee_A + (Fee_B / RE) * rate_a
B = (Fee_A / RE) * rate_b + Fee_B

Min Group Shipping Fee= SUM(Fee_A + Fee_B) * rate_mode6

rate_a & b = 0.4 all city 
rate_a & b = 0.5 spxi all city
*/

drop table if exists dev_vnfdbi_opsndrivers.shopeefood_bi_adhoc_raw_orders_kien;
create table if not exists dev_vnfdbi_opsndrivers.shopeefood_bi_adhoc_raw_orders_kien as

with config_tab as
(select 
    city_name
    ,cast(current_min_fee as double) as current_min_fee
    ,cast(adjust_min_fee_opt1 as double) as adjust_min_fee_opt1
    ,cast(adjust_min_fee_opt2 as double) as adjust_min_fee_opt2
    ,cast(adjust_min_fee_opt3 as double) as adjust_min_fee_opt3
    ,cast(adjust_min_fee_opt4 as double) as adjust_min_fee_opt4 -- revise add more scheme 
    ,cast(adjust_min_fee_opt5 as double) as adjust_min_fee_opt5 -- revise add more scheme 
from dev_vnfdbi_opsndrivers.driver_ops_min_fee_config_adhoc
)

, group_base as
(
    select 
        group_id
        ,count(distinct order_id) as total_orders
        ,avg(distance_all) / avg(distance_grp) as actual_re
        ,avg(distance_grp) as distance_grp
    from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level
    where group_id != 0
    and grass_date between date '2024-06-01' - interval '1' day and date '2024-06-30' + interval '1' day
    group by 1
    having count(distinct order_id) > 1
)



,ranking_order_tab as
(select 
    bf.group_id
    ,order_id
    -- ,bf.ref_order_category
    ,row_number () over(partition by bf.group_id order by order_id asc) as rnk
    
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
inner join group_base g
    on bf.group_id = g.group_id
where grass_date between date '2024-06-01' - interval '1' day and date '2024-06-30' + interval '1' day
)


,order_base as
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
    ,case 
        when COALESCE(food.city_name_full,'na') in ('Hai Phong City','Dong Nai','Da Nang City','Binh Duong','Hue City','Quang Nam','Can Tho City','Vung Tau','Khanh Hoa','Nghe An','Thai Nguyen','Bac Ninh','Lam Dong','Quang Ninh') then 'G1'
        when COALESCE(food.city_name_full,'na') in ('An Giang','Nam Dinh City','Kien Giang','Binh Dinh','Binh Thuan','Long An','Dak Lak','Hai Duong','Phu Yen','Thanh Hoa','Tien Giang','Dong Thap') then 'G2'
        else 'G3' end as city_group
    ,case 
        when rot.rnk is null then 'single' 
        when rot.rnk is not null and is_stack_group_order = 1 then 'group'
        when  rot.rnk is not null and is_stack_group_order = 2 then 'stack'
        end actual_order_type
    ,coalesce(rot.rnk , 1) as order_rank
    ,cf.current_min_fee
    ,cf.adjust_min_fee_opt1
    ,cf.adjust_min_fee_opt2
    ,cf.adjust_min_fee_opt3
    ,cf.adjust_min_fee_opt4
    ,cf.adjust_min_fee_opt5

    -- ,dotet.order_data
from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level food
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    on food.order_id = dot.ref_order_id  and dot.ref_order_category = 0
left join shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
    on dot.id = dotet.order_id
Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = food.pick_district_id
left join ranking_order_tab rot
    on food.order_id = rot.order_id and dot.ref_order_category = 0
left join config_tab cf
    on coalesce(food.city_name_full, 'na') = cf.city_name

where food.grass_date between date '2024-06-01' and date '2024-06-30' -- and grass_date != date '2022-12-12'
-- and COALESCE(food.city_name_full,'na') in ('Hai Phong City','Dong Nai','Da Nang City','Binh Duong','Hue City','Quang Nam','Can Tho City','Vung Tau','Khanh Hoa','Nghe An','Thai Nguyen','Bac Ninh','Lam Dong','Quang Ninh')
and source in ('Food')
and delivered_by != 'hub'
)



,raw_orders as
(select 
    *
    -- ,case 
    --     when city_group = 'G1' then GREATEST(min_fee,unit_fee * distance * surge_rate)
    --     when city_group = 'G2' then GREATEST(min_fee,unit_fee * distance * surge_rate)
    --     else GREATEST(min_fee,unit_fee * distance * surge_rate) end as spf_current

    ,GREATEST(current_min_fee,unit_fee * distance * surge_rate) as spf_current
    ,GREATEST(adjust_min_fee_opt1,unit_fee * distance * surge_rate) as spf_opt1
    ,GREATEST(adjust_min_fee_opt2,unit_fee * distance * surge_rate) as spf_opt2
    ,GREATEST(adjust_min_fee_opt3,unit_fee * distance * surge_rate) as spf_opt3
    ,GREATEST(adjust_min_fee_opt4,unit_fee * distance * surge_rate) as spf_opt4
    ,GREATEST(adjust_min_fee_opt5,unit_fee * distance * surge_rate) as spf_opt5

    -- ,case 
    --     when city_group = 'G1' then GREATEST(case when min_fee =12000 then 10000 else min_fee end,unit_fee * distance * surge_rate)
    --     when city_group = 'G2' then GREATEST(min_fee,unit_fee * distance * surge_rate)
    --     else GREATEST(min_fee,unit_fee * distance * surge_rate) end as spf_opt10k
    
    -- ,case 
    --     when city_group = 'G1' then GREATEST(case when min_fee =12000 then 9000 else min_fee end,unit_fee * distance * surge_rate)
    --     when city_group = 'G2' then GREATEST(min_fee,unit_fee * distance * surge_rate)
    --     else GREATEST(min_fee,unit_fee * distance * surge_rate) end as spf_opt9k

from order_base
)
select 
    *
from raw_orders
;


drop table if exists dev_vnfdbi_opsndrivers.shopeefood_bi_adhoc_stack_order_recalculation;
create table if not exists dev_vnfdbi_opsndrivers.shopeefood_bi_adhoc_stack_order_recalculation as


with group_base as
(
    select 
        group_id
        ,count(distinct order_id) as total_orders
        ,avg(distance_all) / avg(case when distance_grp = 0 then distance_all else distance_grp end ) as actual_re
        ,avg(distance_grp) as distance_grp
    from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
    -- left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) re
        -- on 
    where group_id != 0
    and grass_date between date '2024-06-01' - interval '1' day and date '2024-06-30' + interval '1' day
    group by 1
    having count(distinct order_id) > 1
)


,stack_orders_base as
(select 
    b.*
    ,gb.total_orders
    ,gb.actual_re
    -- ,gb.distance_grp
from dev_vnfdbi_opsndrivers.shopeefood_bi_adhoc_raw_orders_kien b
left join group_base gb
    on b.group_id = gb.group_id
where actual_order_type = 'stack'
)


,stack_order_recalculation as
(select 
    raw.grass_date
    ,raw.city_name_full
    ,raw.group_id
    ,raw.order_id as order_id_first
    ,raw.spf_current as first_cal_current
    ,raw2.spf_current as second_cal_current
    ,raw.spf_opt1 as first_spf_opt1
    ,raw.spf_opt2 as first_spf_opt2
    ,raw.spf_opt3 as first_spf_opt3
    ,raw.spf_opt4 as first_spf_opt4
    ,raw.spf_opt5 as first_spf_opt5

    ,raw2.spf_opt1 as second_spf_opt1
    ,raw2.spf_opt2 as second_spf_opt2
    ,raw2.spf_opt3 as second_spf_opt3
    ,raw2.spf_opt4 as second_spf_opt4
    ,raw2.spf_opt5 as second_spf_opt5
    ,raw.actual_re
    ,raw.distance_grp

    ,cast(json_extract(re.extra_data,'$.re') as double) re_stack_system 
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double) as surge_rate_group
    ,cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as double) as unit_fee_group

    ,cast(json_extract(re.extra_data,'$.ship_fee_info.min_fee') as double) as c_min_group_shipping_fee

    ,raw.spf_current + raw2.spf_current/raw.actual_re*0.4 as fee_a_current
    ,raw.spf_current/raw.actual_re*0.4 + raw2.spf_current as fee_b_current

    ,raw.spf_opt1 + raw2.spf_opt1/raw.actual_re*0.4 as fee_a_opt1
    ,raw.spf_opt1/raw.actual_re*0.4 + raw2.spf_opt1 as fee_b_opt1

    ,raw.spf_opt2 + raw2.spf_opt2/raw.actual_re*0.4 as fee_a_opt2
    ,raw.spf_opt2/raw.actual_re*0.4 + raw2.spf_opt2 as fee_b_opt2

    ,raw.spf_opt3 + raw2.spf_opt3/raw.actual_re*0.4 as fee_a_opt3
    ,raw.spf_opt3/raw.actual_re*0.4 + raw2.spf_opt3 as fee_b_opt3

    ,raw.spf_opt4 + raw2.spf_opt4/raw.actual_re*0.4 as fee_a_opt4
    ,raw.spf_opt4/raw.actual_re*0.4 + raw2.spf_opt4 as fee_b_opt4

    ,raw.spf_opt5 + raw2.spf_opt5/raw.actual_re*0.4 as fee_a_opt5
    ,raw.spf_opt5/raw.actual_re*0.4 + raw2.spf_opt5 as fee_b_opt5

    ,greatest(raw.spf_current + raw2.spf_current/raw.actual_re*0.4,raw.spf_current/raw.actual_re*0.4 + raw2.spf_current)  as min_group_shipping_fee_current
    
    ,greatest(raw.spf_opt1 + raw2.spf_opt1/raw.actual_re*0.4 , raw.spf_opt1/raw.actual_re*0.4 + raw2.spf_opt1)  as min_group_shipping_fee_opt1

    ,greatest(raw.spf_opt2 + raw2.spf_opt2/raw.actual_re*0.4 , raw.spf_opt2/raw.actual_re*0.4 + raw2.spf_opt2)  as min_group_shipping_fee_opt2

    ,greatest(raw.spf_opt3 + raw2.spf_opt3/raw.actual_re*0.4 , raw.spf_opt3/raw.actual_re*0.4 + raw2.spf_opt3)  as min_group_shipping_fee_opt3

    ,greatest(raw.spf_opt4 + raw2.spf_opt4/raw.actual_re*0.4 , raw.spf_opt4/raw.actual_re*0.4 + raw2.spf_opt4)  as min_group_shipping_fee_opt4

    ,greatest(raw.spf_opt5 + raw2.spf_opt5/raw.actual_re*0.4 , raw.spf_opt5/raw.actual_re*0.4 + raw2.spf_opt5)  as min_group_shipping_fee_opt5

    ,re.ship_fee/CAST(100 AS DOUBLE) AS system_stack_fee

    -- ,GREATEST(least(raw.distance_grp * cast(json_extract(re.extra_data,'$.ship_fee_info.per_km') as double) * cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double) , raw.spf_current + raw2.spf_current)
    -- ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)*raw.distance_grp*raw.unit_fee_current as a_spf_by_stacked_distance_current


    -- ,raw.cal_opt1 + raw2.cal_opt1/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_a_opt1
    -- ,raw2.cal_opt1 + raw.cal_opt1/cast(json_extract(re.extra_data,'$.re') as double)*0.55 as fee_b_opt1
    -- ,greatest(raw.cal_opt1 + raw2.cal_opt1/cast(json_extract(re.extra_data,'$.re') as double)*0.55 , raw2.cal_opt1 + raw.cal_opt1/cast(json_extract(re.extra_data,'$.re') as double)*0.55) as min_group_shipping_fee_opt1
    -- ,cast(json_extract(re.extra_data,'$.ship_fee_info.surge_rate') as double)*raw.distance_grp*raw.unit_fee_opt1 as a_spf_by_stacked_distance_opt1


from stack_orders_base raw
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) re
    on re.id = raw.group_id
left join (select * from stack_orders_base raw where actual_order_type = 'stack' and raw.order_rank = 2) raw2
    on raw.group_id = raw2.group_id
where raw.actual_order_type = 'stack'
and raw.order_rank = 1
)
select
    *
    -- ,stacked fee = MAX( MIN( Total Distance * Unit Fee* surge Rate , SUM(Fee_A + Fee_B) ), Min Group Shipping Fee )
    ,GREATEST(least(distance_grp * unit_fee_group * surge_rate_group , fee_a_current + fee_b_current) , min_group_shipping_fee_current) as stacked_fee_current
    ,GREATEST(least(distance_grp * unit_fee_group * surge_rate_group , fee_a_opt1 + fee_b_opt1) , min_group_shipping_fee_opt1) as stacked_fee_opt1
    ,GREATEST(least(distance_grp * unit_fee_group * surge_rate_group , fee_a_opt2 + fee_b_opt2) , min_group_shipping_fee_opt2) as stacked_fee_opt2
    ,GREATEST(least(distance_grp * unit_fee_group * surge_rate_group , fee_a_opt3 + fee_b_opt3) , min_group_shipping_fee_opt3) as stacked_fee_opt3
    ,GREATEST(least(distance_grp * unit_fee_group * surge_rate_group , fee_a_opt4 + fee_b_opt4) , min_group_shipping_fee_opt4) as stacked_fee_opt4
    ,GREATEST(least(distance_grp * unit_fee_group * surge_rate_group , fee_a_opt5 + fee_b_opt5) , min_group_shipping_fee_opt5) as stacked_fee_opt5
from stack_order_recalculation
-- where abs(min_group_shipping_fee_current - c_min_group_shipping_fee) > 500
;



select 
    *
from dev_vnfdbi_opsndrivers.shopeefood_bi_adhoc_stack_order_recalculation
limit 100
-------------------
;

select 
    grass_date
    ,city_name_full
    ,'single' as type_
    ,sum(spf_current) current_
    ,sum(spf_opt1) opt1
    ,sum(spf_opt2) opt2
    ,sum(spf_opt3) opt3
    ,sum(spf_opt4) opt4
    ,sum(spf_opt4) opt5
    ,count(distinct order_id) as total_orders
from dev_vnfdbi_opsndrivers.shopeefood_bi_adhoc_raw_orders_kien
where actual_order_type in ('single')
group by 1,2,3

union all

select 
    grass_date
    ,city_name_full
    ,'group' as type_
    ,sum(spf_current) current_
    ,sum(spf_opt1) opt1
    ,sum(spf_opt2) opt2
    ,sum(spf_opt3) opt3
    ,sum(spf_opt4) opt4
    ,sum(spf_opt5) opt5
    ,count(distinct order_id) as total_orders
from dev_vnfdbi_opsndrivers.shopeefood_bi_adhoc_raw_orders_kien
where actual_order_type in ('group')
group by 1,2,3

union all

select 
    grass_date
    ,city_name_full
    ,'stack' as type_
    ,sum(stacked_fee_current) current_
    ,sum(stacked_fee_opt1) as opt1
    ,sum(stacked_fee_opt2) as opt2
    ,sum(stacked_fee_opt3) as opt3
    ,sum(stacked_fee_opt4) as opt4
    ,sum(stacked_fee_opt5) as opt5
    ,count(distinct group_id)*2 as total_orders
from dev_vnfdbi_opsndrivers.shopeefood_bi_adhoc_stack_order_recalculation
group by 1,2,3 




