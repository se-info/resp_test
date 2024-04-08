    with hub as 
    (select 
    id,hub_name
    ,cast(json_extract(extra_data,'$.geo_data.points') as array<json>) as geo_data



    from shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
)


    SELECT geo_data as lat_long
        ,hub_name
    FROM
    (
    (
    SELECT * from hub
    )
    CROSS JOIN unnest(geo_data) AS t(geo_data)
    )