-- location
SELECT 
        raw.uid,
        city_name,
        i.id AS hub_id_add,
        raw.original_hub,
        CASE 
        WHEN CARDINALITY(FILTER(original_hub,x -> x = i.id)) > 0 THEN 'current_hub' ELSE '1' END AS action

FROM 
(SELECT 
      hub.uid,
      sm.city_id,
      sm.city_name,
      ARRAY_AGG(hub.hub_id) AS original_hub


FROM shopeefood.foody_internal_db__shipper_hub_mapping_tab__reg_daily_s0_live hub 

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = hub.uid 
    and sm.grass_date = 'current'

WHERE sm.shipper_status_code = 1 
GROUP BY 1,2,3    
) raw

LEFT JOIN 
(select id,city_id from shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
where city_id in (217,218,220)
) i on raw.city_id = i.city_id
WHERE city_name = 'Hai Phong City'
AND (CASE 
        WHEN CARDINALITY(FILTER(original_hub,x -> x = i.id)) > 0 THEN 'current_hub' ELSE '1' END) = '1'
;
--shift
with raw as 
(SELECT 
        sp.uid,
        sp.shift_categories,
        sm.shipper_type_id,
        sm.city_name,
        ARRAY_JOIN(SPLIT(sp.shift_categories,','),';') AS original_shift,
        CARDINALITY(SPLIT(sp.shift_categories,',')) AS num_of_original_shift,
        '3;5;8;10' AS import_shift

FROM shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live sp

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = sp.uid 
    and sm.grass_date = 'current'

WHERE 1 = 1 
AND sm.shipper_type_id = 12 
AND regexp_like(lower(sm.city_name),'dien bien|test') = false 
AND sm.shipper_status_code = 1 
)
select 
        uid,
        city_name,
        original_shift, 
        import_shift,
        replace(replace(replace(replace(original_shift, '1', '5'), '2', '8'), '3', '10'),'4','3') AS shift_revert

from raw where city_name = 'Ha Noi City'

        