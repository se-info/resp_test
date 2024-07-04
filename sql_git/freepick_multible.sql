/*
drop table if exists free_pick_order_group_tab;
create table if not exists free_pick_order_group_tab as 
WITH free_pick as
(
select *
from
        (select
            order_id
            ,ref_order_category
            ,max_by(shipper_uid, create_timestamp) filter(where status in (3,4)) last_incharge_shipper_id 
            ,max_by(assign_type, create_timestamp) filter(where status in (3,4)) last_incharge_assign_type
            ,max(create_timestamp) create_timestamp
            -- *
        from dev_vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab
        where status in (3,4)
        and ref_order_category = 0 
        group by 1,2
        )
where last_incharge_assign_type = 5
)

, osl as 
(
select  order_id
      , max_by(shipper_uid, create_time) filter(where status = 11) last_incharge_shipper_id 
      , max(create_time) filter(where status = 11) last_incharge_unixtime 
from shopeefood.foody_order_db__order_status_log_tab_di
where 1=1 
and date(from_unixtime(create_time -3600)) between date'2023-05-01' and date'2023-08-06'
group by 1
)

, driver_order_raw as 

(
select  ref_order_id as order_id 
      , shipper_id as complete_shipper_id
      , is_asap 
      , city_group 
      , inflow_date
      , lt_completion_original*1.00000000/60 lt_completion
      , lt_incharge
      , lt_incharge_to_arrive_at_merchant
      , lt_arrive_at_merchant_to_pick
      , lt_pick_to_arrive_at_buyer
      , lt_arrive_at_buyer_to_del  
      , is_late_sla
      , is_late_delivered_time_eta_max

      , is_valid_submit_to_del
      , is_valid_lt_incharge_arrive_at_merchant
      , is_valid_lt_arrive_at_merchant_to_pick
      , is_valid_lt_arrive_at_buyer as is_valid_lt_pick_to_arrive_at_buyer
      , is_valid_lt_arrive_at_buyer_to_del
 
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev

where 1=1 
and ref_order_category = 0 and is_stack_order = 0 and is_group_order = 0
and is_del = 1
and city_id not in (0,238,467,468,469,470,471,472)
and inflow_date between date'2023-06-01' and date'2023-07-31'
)


, agg_raw as 
(
select a.*
     , coalesce(b.final_delivered_time,0) complete_unixtime
     , if(coalesce(c.order_id,-1) > 0, 1, 0) is_free_pick
     , coalesce(c.last_incharge_shipper_id,-1) last_incharge_shipper_id
     , coalesce(d.last_incharge_unixtime, -1) last_incharge_unixtime
from driver_order_raw a 
left join (select id, final_delivered_time from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live where date(from_unixtime(submit_time - 3600)) between date'2023-05-01' and date'2023-08-06') b
                on a.order_id = b.id
left join free_pick c on a.order_id = c.order_id and a.complete_shipper_id = c.last_incharge_shipper_id 
left join osl d on a.order_id = d.order_id and a.complete_shipper_id = d.last_incharge_shipper_id 
) 

, free_pick_agg_raw as 
(
select order_id     
     , inflow_date
     , complete_shipper_id 
     , complete_unixtime
     , last_incharge_unixtime

from agg_raw

where is_free_pick = 1 
and last_incharge_unixtime > 0 and complete_unixtime > 0 
)

--select * from free_pick_agg_raw where (last_incharge_unixtime <= 0 or complete_unixtime <= 0 )

select *
from 
(
select a.* 
     , from_unixtime(a.last_incharge_unixtime - 3600) last_incharge_ts_a
     , from_unixtime(a.complete_unixtime - 3600) complete_ts_a
     , b.order_id as order_id_b 
     , b.last_incharge_unixtime as last_incharge_unixtime_b 
     , b.complete_unixtime as complete_unixtime_b 

     , from_unixtime(b.last_incharge_unixtime - 3600) last_incharge_ts_b
     , from_unixtime(b.complete_unixtime - 3600) complete_ts_b

    --   ,array_agg(b.order_id) list_fp_stacked_order
    --   ,map_agg(b.order_id, from_unixtime(b.last_incharge_unixtime - 3600)) list_fp_stack_incharge_time 
    --   ,map_agg(b.order_id, from_unixtime(b.complete_unixtime - 3600)) list_fp_stack_complete_time 
from free_pick_agg_raw a 
join free_pick_agg_raw b on (a.inflow_date between b.inflow_date - interval '2' day and b.inflow_date + interval '2' day) and (a.order_id != b.order_id and a.complete_shipper_id = b.complete_shipper_id) and 
                            ((b.last_incharge_unixtime between a.last_incharge_unixtime and a.complete_unixtime) 
                            or (a.last_incharge_unixtime between b.last_incharge_unixtime and b.complete_unixtime))

where 1=1 --a.order_id = 550398486

--group by 1,2,3,4,5,6,7
)

*/


/*

    ,sum(case when is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered
    ,sum(case when is_cancel = 1 then total_order else 0 end) total_order_cancelled
    ,sum(case when is_late_delivered_time = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_late
    ,sum(case when is_late_delivered_time_eta_max = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_late_eta_max
    ,sum(case when is_late_arrive_buyer = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_late_arrive_buyer
    ,sum(case when is_late_sla = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_late_sla

    -- ,sum(case when is_valid_lt_arrive_at_merchant = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_valid_assign_to_arrive_at_merchant
    -- ,sum(case when is_valid_lt_incharge_arrive_at_merchant  = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_valid_incharge_to_arrive_at_merchant
    -- ,sum(case when is_valid_lt_arrive_at_buyer = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_valid_pick_to_arrive_at_buyer
    -- ,sum(case when is_valid_lt_arrive_at_buyer_to_del = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_valid_arrive_at_buyer_to_del
    -- ,sum(case when is_valid_lt_arrive_at_merchant_to_pick = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_valid_arrive_at_merchant_to_pick


    ,sum(case when is_valid_submit_to_del = 1 then lt_completion_original else 0 end)*1.0000/60 total_lt_completion_original
    ,sum(case when is_valid_submit_to_del = 1 then lt_completion_adjusted else 0 end)*1.0000/60 total_lt_completion_adjusted


    ,sum(case when is_valid_lt_arrive_at_merchant = 1 and is_valid_submit_to_del = 1 then lt_assign_to_arrive_at_merchant else 0 end) total_lt_assign_to_arrive_at_merchant
    ,sum(case when is_valid_lt_incharge_arrive_at_merchant = 1 and is_valid_submit_to_del = 1 then lt_incharge_to_arrive_at_merchant else 0 end) total_lt_incharge_to_arrive_at_merchant
    ,sum(case when is_valid_lt_arrive_at_buyer = 1 and is_valid_submit_to_del = 1 then lt_pick_to_arrive_at_buyer else 0 end) total_lt_pick_to_arrive_at_buyer
    ,sum(case when is_valid_lt_arrive_at_buyer_to_del =1 and is_valid_submit_to_del = 1 then lt_arrive_at_buyer_to_del else 0 end) total_lt_arrive_at_buyer_to_del
    ,sum(case when is_valid_lt_arrive_at_merchant_to_pick = 1 and is_valid_submit_to_del = 1 then lt_arrive_at_merchant_to_pick else 0 end) total_lt_arrive_at_merchant_to_pick

    ,sum(case when is_valid_submit_to_del = 1 then lt_incharge else 0 end) total_lt_incharge
*/


WITH free_pick as
(
select *
from
        (select
            order_id
            ,ref_order_category
            ,max_by(shipper_uid, create_timestamp) filter(where status in (3,4)) last_incharge_shipper_id 
            ,max_by(assign_type, create_timestamp) filter(where status in (3,4)) last_incharge_assign_type
            ,max(create_timestamp) create_timestamp
            -- *
        from dev_vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab
        where status in (3,4)
        and ref_order_category = 0 
        group by 1,2
        )
where last_incharge_assign_type = 5
)

, osl as 
(
select  order_id
      , max_by(shipper_uid, create_time) filter(where status = 11) last_incharge_shipper_id 
      , max(create_time) filter(where status = 11) last_incharge_unixtime 
from shopeefood.foody_order_db__order_status_log_tab_di
where 1=1 
and date(from_unixtime(create_time -3600)) between date'2023-05-01' and date'2023-08-06'
group by 1
)

, driver_order_raw as 

(
select  ref_order_id as order_id 
      , shipper_id as complete_shipper_id
      , is_asap 
      , city_group 
      , inflow_date
      , lt_completion_original*1.00000000/60 lt_completion
      , lt_incharge
      , lt_incharge_to_arrive_at_merchant
      , lt_arrive_at_merchant_to_pick
      , lt_pick_to_arrive_at_buyer
      , lt_arrive_at_buyer_to_del  
      , is_late_sla
      , is_late_delivered_time_eta_max

      , is_valid_submit_to_del
      , is_valid_lt_incharge_arrive_at_merchant
      , is_valid_lt_arrive_at_merchant_to_pick
      , is_valid_lt_arrive_at_buyer as is_valid_lt_pick_to_arrive_at_buyer
      , is_valid_lt_arrive_at_buyer_to_del
      , if(is_stack_order = 1 or is_group_order = 1, 1, 0) is_stack_group
      , total_order_in_group
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev

where 1=1 
and ref_order_category = 0
and is_del = 1
and city_id not in (0,238,467,468,469,470,471,472)
and inflow_date between date'2023-06-01' and date'2023-07-31'
)


, agg_raw as 
(
select a.*
     , coalesce(b.final_delivered_time,0) complete_unixtime
     , if(coalesce(c.order_id,-1) > 0, 1, 0) is_free_pick
     , coalesce(c.last_incharge_shipper_id,-1) last_incharge_shipper_id
     , coalesce(d.last_incharge_unixtime, -1) last_incharge_unixtime
from driver_order_raw a 
left join (select id, final_delivered_time from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live where date(from_unixtime(submit_time - 3600)) between date'2023-05-01' and date'2023-08-06') b
                on a.order_id = b.id
left join free_pick c on a.order_id = c.order_id and a.complete_shipper_id = c.last_incharge_shipper_id 
left join osl d on a.order_id = d.order_id and a.complete_shipper_id = d.last_incharge_shipper_id 
) 

,freepick_stack as 
(
select order_id     
     , inflow_date
     , complete_shipper_id 
     , complete_unixtime
     , last_incharge_unixtime
     , last_incharge_ts_a
     , complete_ts_a      

     , array_agg(order_id_b) list_fp_stacked_order
     , cardinality(array_agg(order_id_b)) + 1 as total_order_in_fp_group
     , map_agg(order_id_b, last_incharge_ts_b) list_fp_stack_incharge_time 
     , map_agg(order_id_b, complete_ts_b) list_fp_stack_complete_time      

from free_pick_order_group_tab

where 1=1 --order_id = 550398486

group by 1,2,3,4,5,6,7
)

,final_raw as 
(
select a.*
      ,if(coalesce(b.order_id,0)> 0 , 1, 0) as is_free_pick_group
      ,coalesce(b.total_order_in_fp_group,0) total_order_in_freepick_group

from agg_raw a 
left join freepick_stack b on a.order_id = b.order_id and a.complete_shipper_id = b.complete_shipper_id

where 1=1 --and a.order_id = 550414582 --550398486
)

,summary as 
(
select inflow_date 
    , city_group
    , order_type
    , total_order_in_group
    , sum(case when is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered
    , sum(case when is_valid_submit_to_del = 1 and is_asap = 1 then total_order else 0 end) total_order_delivered_asap
    , sum(case when is_late_delivered_time_eta_max = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_late_eta_max
    , sum(case when is_late_sla = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_late_sla

    , sum(case when is_valid_lt_incharge_arrive_at_merchant  = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_valid_incharge_to_arrive_at_merchant
    , sum(case when is_valid_lt_pick_to_arrive_at_buyer = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_valid_pick_to_arrive_at_buyer
    , sum(case when is_valid_lt_arrive_at_buyer_to_del = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_valid_arrive_at_buyer_to_del
    , sum(case when is_valid_lt_arrive_at_merchant_to_pick = 1 and is_valid_submit_to_del = 1 then total_order else 0 end) total_order_delivered_valid_arrive_at_merchant_to_pick


    , sum(case when is_valid_submit_to_del = 1 and is_asap = 1 then lt_completion else 0 end) total_lt_completion
    , sum(case when is_valid_lt_incharge_arrive_at_merchant = 1 and is_valid_submit_to_del = 1 then lt_incharge_to_arrive_at_merchant else 0 end) total_lt_incharge_to_arrive_at_merchant
    , sum(case when is_valid_lt_pick_to_arrive_at_buyer = 1 and is_valid_submit_to_del = 1 then lt_pick_to_arrive_at_buyer else 0 end) total_lt_pick_to_arrive_at_buyer
    , sum(case when is_valid_lt_arrive_at_buyer_to_del =1 and is_valid_submit_to_del = 1 then lt_arrive_at_buyer_to_del else 0 end) total_lt_arrive_at_buyer_to_del
    , sum(case when is_valid_lt_arrive_at_merchant_to_pick = 1 and is_valid_submit_to_del = 1 then lt_arrive_at_merchant_to_pick else 0 end) total_lt_arrive_at_merchant_to_pick
    , sum(case when is_valid_submit_to_del = 1 then lt_incharge else 0 end) total_lt_incharge

from 
(
select 
        is_asap 
      , city_group 
      , inflow_date
      , is_late_sla
      , is_late_delivered_time_eta_max
      , is_valid_submit_to_del
      , is_valid_lt_incharge_arrive_at_merchant
      , is_valid_lt_arrive_at_merchant_to_pick
      , is_valid_lt_pick_to_arrive_at_buyer
      , is_valid_lt_arrive_at_buyer_to_del 
      , case when is_free_pick_group = 1 then '1. Freepick - multiple orders'
             when is_free_pick = 1 then '2. Freepick - single orders'
             when is_stack_group = 1 then '3. Stack/Group'
             else 'Single Order' end as order_type 
      , case when is_free_pick_group = 1 then total_order_in_freepick_group
             when is_free_pick = 1 then 1
             when is_stack_group = 1 then total_order_in_group
             else 1 end as total_order_in_group
      , count(distinct order_id) total_order
      , sum(lt_completion) lt_completion
      , sum(lt_incharge) lt_incharge
      , sum(lt_incharge_to_arrive_at_merchant) lt_incharge_to_arrive_at_merchant
      , sum(lt_arrive_at_merchant_to_pick) lt_arrive_at_merchant_to_pick
      , sum(lt_pick_to_arrive_at_buyer) lt_pick_to_arrive_at_buyer
      , sum(lt_arrive_at_buyer_to_del) lt_arrive_at_buyer_to_del

from final_raw 

group by 1,2,3,4,5,6,7,8,9,10,11,12
)

group by 1,2,3,4
)

select a.* 
from free_pick_order_group_tab a 
join freepick_stack b on a.order_id = b.order_id and b.total_order_in_fp_group >= 5