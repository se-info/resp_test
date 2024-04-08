select * 


from  dev_vnfdbi_opsndrivers.merchant_level_data_v2 a 

where (a.inflow_date = date'2022-11-20'
       or 
       a.inflow_date = date'2022-11-11'
       or 
       a.inflow_date = date'2022-11-06')
;
select * 

from dev_vnfdbi_opsndrivers.phong_raw_group_assignment_v1

where period_group = '1. Daily'

and (
    (case when period_group = '1. Daily' then cast(period as date) else null end) = date'2022-11-20'
     or 
    (case when period_group = '1. Daily' then cast(period as date) else null end) = date'2022-11-11' 
     or 
    (case when period_group = '1. Daily' then cast(period as date) else null end) = date'2022-11-06'    
    )
