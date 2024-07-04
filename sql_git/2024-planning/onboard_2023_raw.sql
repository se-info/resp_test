select 
        a.uid,
        sm.shipper_name,
        sm.city_name,
        a.working_status,
        a.birth_date,
        a.onboard_date,
        a.worked_company,
        a.worked_company_text
from
(select 
        uid,
        date(from_unixtime(create_time - 3600)) as onboard_date,
        case 
        when working_status = 1 then 'normal'
        when working_status = 2 then 'off'
        when working_status = 3 then 'pending' end as working_status,
        SUBSTR(cast(birth_date as varchar),7,2)||'-'||SUBSTR(cast(birth_date as varchar),5,2)||'-'||SUBSTR(cast(birth_date as varchar),1,4) AS birth_date,
        CASE
        WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 6) = true then 'Student'
        WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 7) = true then 'Officer'
        WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 3) = true then 'Baemin'
        WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 1) = true then 'Grab'
        WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 2) = true then 'Gojek'
        WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 4) = true then 'BE'
        WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 5) = true then 'Ahamove'
        WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 8) = true then 'Other'
        WHEN json_extract(extra_data, '$.worked_companys.types') is null then sr.worked_company 
        ELSE 'Other'
        end as worked_company,
        json_extract(extra_data,'$.worked_companys.context') worked_company_text
-- select * from shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live where identity_number = '001094044693'
from shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live o 

LEFT JOIN
    (SELECT
        id
        , last_name || ' ' || first_name AS driver_name
        , json_extract(extra_data, '$.shipper_uid') AS shipper_uid
        , json_extract(extra_data, '$.worked_companys.types') AS work_company_total
        , json_array_length(json_extract(extra_data, '$.worked_companys.types')) AS length
        , CASE
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 6) = true then 'Student'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 7) = true then 'Officer'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 3) = true then 'Baemin'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 1) = true then 'Grab'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 2) = true then 'Gojek'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 4) = true then 'BE'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 5) = true then 'Ahamove'
             WHEN json_array_contains(json_extract(extra_data, '$.worked_companys.types'), 8) = true then 'Other'
        ELSE 'Other' END AS worked_company
        , status
    FROM shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live
    WHERE json_extract(extra_data, '$.shipper_uid') IS NOT NULL
    ) sr ON o.uid = CAST(sr.shipper_uid AS BIGINT)

) a 


left join shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = a.uid 
    and sm.grass_date = 'current'

where 1 = 1 
and onboard_date >= date'2023-01-01'
and regexp_like(lower(sm.shipper_name),'test|stress') = false
and regexp_like(lower(sm.city_name),'test|stress|dien bien') = false

