SELECT 
         a.uid AS shipper_id 
        ,a.shopee_uid
        ,a.full_name AS shipper_name 
        ,b.name_en AS city_name
        ,DATE(FROM_UNIXTIME(a.create_time - 3600)) AS onboard_date
        ,CASE WHEN a.take_order_status = 1 THEN 'normal' WHEN a.take_order_status = 2 THEN 'stop' ELSE 'pending' END AS order_status 
        ,CASE WHEN a.working_status = 1 THEN 'normal' WHEN a.working_status = 2 THEN 'off' ELSE 'pending' END AS working_status
        ,c.last_order_timestamp
        ,DATE(last_order_timestamp) AS last_order_date
        ,inapp.inapp_completed_contract
        ,inapp.contract_agg
        ,inapp.annex_agg
        ,ect.econtract_completed
        ,ect.econtract_agg

FROM shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live a 

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live b 
    on b.id = a.location_id
    and b.country_id = 86

LEFT JOIN (select uid,count(id) as inapp_completed_contract,array_agg(contract_number) AS contract_agg, array_agg(contract_annex_number) AS annex_agg from shopeefood.foody_internal_db__shipper_inapp_contract_tab__reg_daily_s0_live 
           where contract_status = 30
           group by 1) inapp on inapp.uid = a.uid

LEFT JOIN (select shipper_uid as uid,count(id) as econtract_completed, array_agg(file_path) as econtract_agg from shopeefood.foody_internal_db__shipper_contract_tab__reg_daily_s0_live 
           where status = 2
           group by 1 ) ect on ect.uid = a.uid

LEFT JOIN 
        (SELECT 
                uid,max(from_unixtime(real_drop_time - 3600)) AS last_order_timestamp

        FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
        WHERE date(dt) = current_date - interval '1' day
        GROUP BY 1
        ) c on c.uid = a.uid
where a.working_status != 2 
and regexp_like(b.name_en,'test|Test|TEST|Dien Bien') = false
and regexp_like(a.full_name,'test|Test|TEST|Dien Bien') = false
and inapp.inapp_completed_contract is null
and ect.econtract_completed is null
;
-- spp_account
WITH raw AS
(SELECT
        a.uid AS shipper_id 
       ,a.shopee_uid
       ,a.full_name AS shipper_name 
       ,b.name_en AS city_name
       ,date(from_unixtime(a.create_time - 3600)) as onboard_date
       ,case when a.take_order_status = 1 then 'normal' when a.take_order_status = 2 then 'stop' else 'pending' end as order_status 
       ,case when a.working_status = 1 then 'normal' when a.working_status = 2 then 'off' else 'pending' end as working_status
       ,a.bank_id
       ,a.bank_name
       ,spp.username as shopeepay_username
       ,c.last_order_timestamp
    --    ,a.shopee_toc_account_userid
    --    ,a.shopee_toc_account_username
    --    ,a.shopeepay_enabled
       ,DATE(c.last_order_timestamp) AS last_order_date




FROM shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live a 

LEFT JOIN shopeefood.foody_accountant_db__partner_airpay_withdrawal_account_tab__reg_daily_s0_live spp 
    on spp.uid = a.uid
    and spp.is_active = 1

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live b 
    on b.id = a.location_id
    and b.country_id = 86

LEFT JOIN 
        (SELECT 
                uid,max(from_unixtime(real_drop_time - 3600)) AS last_order_timestamp

        FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
        WHERE date(dt) = current_date - interval '1' day
        GROUP BY 1
        ) c on c.uid = a.uid

where 1 = 1 
-- and bank_id is null and spp.username is null
and regexp_like(b.name_en,'test|Test|TEST|Dien Bien') = false
and regexp_like(a.full_name,'test|Test|TEST|Dien Bien') = false

)
select * from raw 