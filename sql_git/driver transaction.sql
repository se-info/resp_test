select 

                                
    user_id,
    date(from_unixtime(create_time - 3600)) AS created_date,  
    date(from_unixtime(create_time - 3600)) - interval '1' day AS hub_date,  
    sum(case when txn_type in (104,201,301,2101,2001,3000,1000,401,906) then (balance + deposit)*1.00/100 else 0 end) as shipping_share,                                                                                            
                                                                                                                                               
    sum(case when txn_type in (114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137) then (balance + deposit)*1.00/100 else 0 end) as other_bonus,                      
    sum(case when txn_type in (106,203,2003,303,1002,1005,1007,3002,3005,3007) then (balance + deposit)*1.00/100 else 0 end) as other_payable,
    sum(case when txn_type in (202,302,2002,2102,1001,402,3001) then (balance + deposit)*1.00/100 else 0 end) as return_fee_share,                                  
    sum(case when txn_type in (101,200,2000,2100,300,1006,3006,400,105) then (balance + deposit)*1.00/100 else 0 end) as bonus,                        
    sum(case when txn_type in (204,2004,304,2106,1003,3003,404) then (balance + deposit)*1.00/100 else 0 end) as bonus_shipper                    
    ,sum(case when txn_type in (512,560,900,901,907) then (balance + deposit)*1.00/100 else 0 end) as daily_bonus
    ,sum(case when txn_type in (134,135,154,108,110,111) then (balance + deposit)*1.00/100 else 0 end) as tip
    ,count(case when txn_type in (134,135,108,110) and (balance + deposit) > 0 then reference_id else null end) as tip_txn
    ,array_agg(case when txn_type in (134,135,108,110) and (balance + deposit) > 0 then reference_id else null end) as tip_ext
from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live 
where 1=1
and txn_type in 
(
104,201,301,2101,2001,3000,1000,401,906, -- shipping_share   
114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137, -- other bonus 
106,203,2003,303,1002,1005,1007,3002,3005,3007, -- other_payable
202,302,2002,2102,1001,402,3001, -- return_share 
101,200,2000,2100,300,1006,3006,400, -- order completed bonus 
204,2004,304,2106,1003,3003,404, -- additional bonus 
134,135,108,110, --- tipped 
512,560,900,901,907 --bonus             
) 
group by 1,2

