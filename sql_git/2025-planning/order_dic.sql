with base as (
SELECT
        -- oct.shipper_uid as shipper_id
        distinct oct.id as order_id
        ,oct.order_code as order_code
        ,oct.status
        ,oct.created_date
        ,oct.created_hour
        ,oct.is_asap
        ,oct.distance
        ,oct.sub_total
        ,osl.cancel_time - oct.create_time as lt_submit_to_cancel
        ,oct.cancel_actor
        ,case 
            when oct.status = 8 then 
            case when oct.cancel_reason = 'I made duplicate orders' then 'Made duplicate orders'
                 when oct.cancel_reason = 'Make another order' then 'Buyer want to cancel order'
                 when oct.cancel_reason = 'I am busy and cannot receive order' then 'Busy and cannot receive order'
                 when oct.cancel_reason in ('Confirmed the order too late','Affected by quarantine area','Order limit due to Covid','Preorder','Missing reason','Other reason - Buyer','Other reason - Merchant','Other reason - Driver') then 'Others'
                 when oct.cancel_reason is null then 'Others' else coalesce(oct.cancel_reason,'Not Cancel') end
            when oct.status = 9 then 'quit_by_user'
            else 'Not Cancel' end as cancel_reason
        ,case when oct.cancel_actor = 'Fraud' then 'System' 
            when oct.is_co_oos_all_item = 1 then 'Driver'
        else cancel_actor end as cancel_main_actor
        ,case when status = 8 then 
            case when oct.cancel_reason is null then null
                 when oct.cancel_reason in ('No driver') and cancel_actor = 'Buyer' and osl.cancel_time - oct.create_time < 300 then 'Buyer-induced Cancellation' --- reg y/cau 5 phut
                 when oct.cancel_reason in ('No driver','Other reason - Driver') then 'Driver-induced Cancellation'
                 when oct.cancel_reason in ('Out of stock', 'Shop closed','Shop busy','Shop did not confirm','Shop did not confirm order','Wrong price','Other reason - Merchant') then 'Store-induced Cancellation'
                 when oct.cancel_reason in ('Pending status from bank') then 'Buyer-induced Cancellation'
                 when oct.cancel_reason in ('Payment failed') then 'Buyer-induced Cancellation'
                 when oct.cancel_reason in ('Affected by quarantine area','Order limit due to Covid') then 'Buyer-induced Cancellation'
                 else 'Buyer-induced Cancellation' end 
            when status = 9 then 'Buyer-induced Cancellation' 
            else 'Not Cancel' end as cancel_type
        ,case 
            when oct.payment_method = 'Cash' then 'After payment'
            when oct.payment_method !='Cash' and osl.received_time is not null then 'After payment'
            else 'Pending payment' 
            end as before_after_payment            
        ,deny.last_deny_time - osl.first_assign_time as lt_assign_to_last_deny
        ,coalesce(deny.count_deny_time,0) count_deny_time
        ,osl.count_incharge_time
        ,r.bad_weather_fee,
        r.late_night_service_fee,
        r.holiday_service_fee,
        oct.city_name

    FROM vnfdbi_opsndrivers.shopeefood_vn_bnp_ops_order_detail_tab__vn_daily_s0_live oct
    left join driver_ops_raw_order_tab r on r.id = oct.id
    left join 
        (
        select order_id 
            , max(case when status = 8 then create_time  else null end) as cancel_time
            , max(create_time) filter(where status in (2)) as received_time
            , min(create_time) filter(where status in (14,21)) as first_assign_time
            , count(order_id) filter(where status in (11)) as count_incharge_time

        from shopeefood.foody_order_db__order_status_log_tab_di
        group by 1
        ) osl on osl.order_id = oct.id 

    left join (
        SELECT 
            dot.ref_order_id
            , min(dod.create_time) as first_deny_time
            , max(dod.create_time) as last_deny_time
            , count(distinct dod.id) as count_deny_time
                                
        FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod 
        JOIN shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on dod.order_id = dot.id
        WHERE 1=1 
            and dot.ref_order_category = 0
        GROUP BY 1
    ) deny on oct.id = deny.ref_order_id

    WHERE 1=1
        and oct.city_id not in (0,238,468,469,470,471,472)
        and oct.foody_service_id = 1 
        -- tu thang 5/2024, chi report Food only
)
-- select 
--         date_trunc('month',created_date) as month_,
--         sum(dic)*1.000/sum(gross) as dic,
--         sum(case when bw_threshold > 0.2 then dic else null end)*1.0000/sum(case when bw_threshold > 0 then gross else null end) as dic_bw,
--         sum(case when holiday_order > 0 then dic else null end)*1.0000/sum(case when holiday_order > 0 then gross else null end) as dic_holiday,
--         sum(case when is_peak > 0 then dic else null end)*1.0000/sum(case when is_peak > 0 then gross else null end) as dic_cp ,
--         sum(case when is_peak = 0 and bw_threshold <= 0.2 and holiday_order = 0 then dic else null end)*1.0000/sum(case when is_peak = 0 and bw_threshold <= 0.2 and holiday_order = 0 then gross else null end) as dic_normal,
--         sum(bw_order_net)*1.0000/sum(net) as ado_bw,
--         sum(bad_weather_fee)*1.0000/sum(bw_order_net) as avg_bw_value        
select *
from 
(select 
        created_date,
        if(day(created_date)=month(created_date),1,0) as is_peak,
        city_name,
        count(distinct order_code) as gross,
        coalesce(try(count(distinct case when bad_weather_fee > 0 and status = 7 then order_code else null end)*1.00/count(distinct case when status = 7 then order_code else null end)),0) as bw_threshold, 
        count(distinct case when holiday_service_fee > 0 then order_code else null end) as holiday_order,
        count(distinct case when base.status != 7 and base.cancel_type = 'Driver-induced Cancellation' then order_code else null end) as dic, 
        sum(case when status = 7 then bad_weather_fee else null end) as bad_weather_fee,
        count(distinct case when bad_weather_fee > 0 and status = 7 then order_code else null end) as bw_order_net,
        count(distinct case when status = 7 then order_code else null end) as net

from base
where created_date >= date'2024-01-01'
-- and created_Date <= date'2024-04-30'
group by 1,2,3 
)
-- group by 1 
