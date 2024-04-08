with driver_list as 
(
select 
        uid
       ,date(from_unixtime(create_time - 3600)) as onboard_date
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||substr(cast(birth_date as varchar),7,2)||'/'||substr(cast(birth_date as varchar),5,2)||'/'||substr(cast(birth_date as varchar),1,4) as lookup_value_1
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||national_id_number as lookup_value_2
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||personal_email as lookup_value_3
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||main_phone as lookup_value_4 

from shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live
)
,registration_tab as 
(select 
        raw.id as no_,
        city.name_en as city_name,
        date(from_unixtime(raw.create_time - 3600)) as created,
        date(from_unixtime(raw.update_time - 3600)) as updated,
        case 
        when raw.meetup_date > 0 then date(from_unixtime(raw.meetup_date - 3600)) 
        else null end as meetup_date,
        last_name||' '||first_name as full_name,
        identity_number,
        email,
        phone,
        note,
        case 
        when raw.status = 1 then 'SUBMITTED'
        when raw.status = 4 then 'TEST_PASSED'
        when raw.status = 5 then 'TEST_FAILED'
        when raw.status = 6 then 'REVIEW_PASSED'
        when raw.status = 7 then 'REVIEW_FAILED'
        when raw.status = 8 then 'OFFER_SENT'
        when raw.status = 11 then 'SUCCESS'
        when raw.status = 12 then 'CANCELLED'
        when raw.status = 13 then 'OFFER_SENT_EXPIRED'
        when raw.status = 14 then 'INFORMATION_SUPPLEMENTED'
        when raw.status = 15 then 'SEND_CONTRACT_GENERATING'
        when raw.status = 16 then 'SEND_CONTRACT'
        when raw.status = 17 then 'CONTRACT_READY_TO_SIGN_GENERATING'
        when raw.status = 18 then 'CONTRACT_READY_TO_SIGN'
        when raw.status = 19 then 'CONTRACT_SIGNED_GENERATING'
        when raw.status = 20 then 'CONTRACT_SIGNED'        
        end as registration_status,
        cast(is_now_shipper_before as boolean) as is_now_shipper_before,
        case
        when raw.referral_source = 1 then 'NOW_DRIVER'
        when raw.referral_source = 2 then 'DIRECTLY'
        when raw.referral_source = 3 then 'FRIEND_OR_FAMILLY'
        when raw.referral_source = 4 then 'NOW_STAFF'
        when raw.referral_source = 5 then 'ADS'
        when raw.referral_source = 6 then 'FORUM_OR_BLOG'
        when raw.referral_source = 7 then 'OTHERS' 
        end as referral_source,
        TRIM(regexp_replace(LOWER(last_name||' '||first_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||identity_number as lookup_value_2,
        TRIM(regexp_replace(LOWER(last_name||' '||first_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||email as lookup_value_3,
        TRIM(regexp_replace(LOWER(last_name||' '||first_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||phone as lookup_value_4 

from shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live raw 
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = raw.city_id and city.country_id = 86
)
select *
from
(select 
        a.* 
        ,case 
              when c.uid is not null then c.uid  
              when d.uid is not null then d.uid  
              when k.uid is not null then k.uid          
        else null end as shipper_id_previous
        ,case 
              when c.uid is not null then c.onboard_date
              when d.uid is not null then d.onboard_date  
              when k.uid is not null then k.onboard_date          
        else null end as onboard_date_previous


from registration_tab a 

left join driver_list c on a.lookup_value_2 = c.lookup_value_2 and a.updated > c.onboard_date

left join driver_list d on a.lookup_value_3 = d.lookup_value_3 and a.updated > d.onboard_date

left join driver_list k on a.lookup_value_4 = k.lookup_value_4 and a.updated > k.onboard_date
)
where shipper_id_previous is not null
order by 3 desc 

limit 100



