with assign_raw as
(select 
        date_  
    --    ,hour_
       ,case when hour_ <=  2 then '1. 0 -  2'
            when hour_ <=  4 then '2. 2 -  4'
            when hour_ <=  6 then '3. 4 -  6'
            when hour_ <=  8 then '4. 6 -  8'
            when hour_ <=  10 then '5. 8 -  10'
            when hour_ <=  12 then '6. 10 -  12'
            when hour_ <=  14 then '7. 12 -  14'
            when hour_ <=  16 then '8. 14 -  16'
            when hour_ <=  18 then '9. 16 -  18'
            when hour_ <=  20 then '10. 18 -  20'
            when hour_ <=  22 then '11. 20 -  22'
            when hour_ > 22 then '12. > 22'
             end as hour_range   
       ,city_name 
       ,shipper_id
       ,shipper_type
       ,coalesce(sum(total_assign_excl_fp),0) total_assign_excl_fp
       ,coalesce(sum(total_deny),0) total_deny        


from dev_vnfdbi_opsndrivers.phong_assignment_tracker

group by 1,2,3,4,5
)

select 
         date_ 
        ,hour_range
        ,city_name
        ,shipper_type
        ,case   when pct_assign <= 0.1 then'1. 0 - 10%'
                when pct_assign <= 0.2 then'2. 10 - 20%'
                when pct_assign <= 0.3 then'3. 20 - 30%'
                when pct_assign <= 0.4 then'4. 30 - 40%'
                when pct_assign <= 0.5 then'5. 40 - 50%'
                when pct_assign <= 0.6 then'6. 50 - 60%'
                when pct_assign <= 0.7 then'7. 60 - 70%'
                when pct_assign <= 0.8 then'8. 70 - 80%'
                when pct_assign <= 0.9 then'9. 80 - 90%'
                when pct_assign <= 1 then'10. 90 - 100%'
                end as assign_success_rate
        ,count(distinct shipper_id) as total_driver        
        ,1 - (coalesce((sum(total_deny)/count(distinct shipper_id)),0)/cast(coalesce((sum(total_assign_excl_fp)/count(distinct shipper_id)),0) as double)) as avg_assign_rate
        -- ,approx_percentile(pct_assign,0.1) as pct10_assign_rate
        -- ,approx_percentile(pct_assign,0.2) as pct20_assign_rate
        -- ,approx_percentile(pct_assign,0.3) as pct30_assign_rate
        -- ,approx_percentile(pct_assign,0.4) as pct40_assign_rate
        -- ,approx_percentile(pct_assign,0.5) as pct50_assign_rate
        -- ,approx_percentile(pct_assign,0.6) as pct60_assign_rate
        -- ,approx_percentile(pct_assign,0.7) as pct70_assign_rate
        -- ,approx_percentile(pct_assign,0.8) as pct80_assign_rate
        -- ,approx_percentile(pct_assign,0.9) as pct90_assign_rate


from
(select a.* ,case when total_assign_excl_fp > 0 then 1 - (coalesce(sum(total_deny),0)/cast(coalesce(sum(total_assign_excl_fp),0) as double))
             when total_assign_excl_fp = 0 and total_deny > 0 then 0
             when total_assign_excl_fp = 0 and total_deny = 0 then 1
             end as pct_assign    


from assign_raw a 

-- where city_name = 'HCM City'
group by 1,2,3,4,5,6,7
)

group by 1,2,3,4,5
