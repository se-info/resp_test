add jar hdfs://R2/projects/shopeefood_assignment/hdfs/udf/unzip.jar;
drop function if exists gzip_unzip;
create function if not exists gzip_unzip as 'com.shopee.udf.GzipUDF' using jar "hdfs://R2/projects/shopeefood_assignment/hdfs/udf/unzip.jar";

with 
replay_tab as (
    select 
        trace_id
        ,city_id
        ,grass_date
        ,create_time
        ,hour(from_unixtime(create_time-3600)) as _hour
        ,string(get_json_object(_processing_info, '$.ds_request')) AS ds_request
        ,string(get_json_object(_processing_info, '$.ds_response')) AS ds_response
    from (
        select  
            trace_id
            ,city_id
            ,grass_date
            ,create_time
            ,gzip_unzip(processing_info) as _processing_info
        from    (
            select  
                trace_id
                ,area_id as city_id
                ,grass_date
                ,create_time
                ,processing_info
            from    shopeefood_assignment.shopee_foodalgo_assignment_algorithm_solve_req_dump_live_vn__reg_continuous_s0_live
            -- where   dt = '2024-08-16' and area_id = 222 and create_time > 1723798800
            -- limit 1
        )
    )
)
select * from replay_tab where ds_response like '%562515588%'
limit 10

-- run on sparksql
