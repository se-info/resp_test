with raw as 
(select * 

from dev_vnfdbi_opsndrivers.snp_foody_sf_chat_new 

where issue_reason = 'Driver report long prepare time - support'

-- order by start_at asc 
)
,metrics as 
(select
         oct.id AS order_id
        ,oct.restaurant_id
        ,mm.merchant_name
        ,mm.city_name
        ,mm.district_name 
        ,date(from_unixtime(oct.submit_time - 3600)) as created_order_date
        ,case when oct.status = 7 then 'Delivered'
              when oct.status = 8 then 'Cancelled'
              when oct.status = 9 then 'Quit' end as order_status
        ,map_agg(raw.request_at,now_uid) as mapping_request_time_caseid
        ,cardinality(array_agg(distinct chat_id)) as total_request                  

from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct

LEFT JOIN raw on raw.ref_id = oct.order_code

LEFT JOIN shopeefood.foody_mart__profile_merchant_master mm on mm.merchant_id = oct.restaurant_id and try_cast(mm.grass_date as date) =  date(from_unixtime(oct.submit_time - 3600))

where date(from_unixtime(oct.submit_time - 3600)) between current_date - interval '90' day and current_date - interval '1' day


group by 1,2,3,4,5,6,7
)

select * from metrics where mapping_request_time_caseid is not null and total_request >= 2 
