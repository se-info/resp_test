WITH bill_fee AS 
(SELECT order_id
    ,partner_id
    ,date_
    ,year_week
    ,city_name
    ,city_name_full
    ,shipper_type_id
    ,is_new_policy
    ,shipper_type
    ,source
    ,sub_source
    ,distance
    ---- add distance range
    ,CASE when distance <= 3 then '0-3 km'
          when distance > 3 then '3++ km'
          end as distance_range
    ,status
    ,total_shipping_fee
    ,current_driver_tier
    ,new_driver_tier
    ,is_qualified_hub
    ,delivered_by
    ,coalesce(total_bill,0) as total_bill
    ,coalesce(total_bill_hub,0) as total_bill_hub
    ,(case 
            when is_nan(bonus) = true then 0.00 
            when delivered_by = 'hub' then bonus_hub
            when delivered_by != 'hub' then bonus_non_hub
            else null end)*1.000000  /exchange_rate as bonus_usd_all
    , (driver_cost_base + return_fee_share_basic)*1.000000  /exchange_rate as total_driver_cost_base_all
    , (driver_cost_surge + return_fee_share_surge)*1.000000  /exchange_rate as total_driver_cost_surge_all    
    
    from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level
    
)

select date_    
      ,sub_source 
      ,is_qualified_hub 
      ,delivered_by
      ,distance_range
      ,sum(total_bill) total_bill
      ,sum(total_bill_hub) total_bill_hub
      ,sum(total_bill - total_bill_hub) total_bill_non_hub 
      ,sum(total_driver_cost_base_all + total_driver_cost_surge_all) driver_cost_basic 
      ,sum(bonus_usd_all) driver_cost_bonus

from bill_fee 

where 1=1 
and date_ between date'2022-06-01' and date'2022-07-31' 
and sub_source in ('Food')

group by 1,2,3,4,5