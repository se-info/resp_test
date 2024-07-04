SELECT distinct 
                                                                                                                                                 
                mco.order_id
                ,cast(json_extract_scalar(mco.extra_data,'$.adjustment_amount') as bigint) as compensation_amount
                ,case when cast(json_extract_scalar(mco.extra_data,'$.adjustment_type') as int) = 1 then 'add'
                      when cast(json_extract_scalar(mco.extra_data,'$.adjustment_type') as int) = 2 then 'deduct'
                      else 'Others' end as adjustment_type
                ,case when cast(json_extract_scalar(mco.extra_data,'$.amount_type') as int) = 1 then 'full'
                      when cast(json_extract_scalar(mco.extra_data,'$.amount_type') as int) = 2 then 'partial'
                      else 'Others' end as refund_type
                ,case when cast(json_extract_scalar(mco.extra_data,'$.adjustment_channel') as int) = 1 then 'wallet'
                      when cast(json_extract_scalar(mco.extra_data,'$.adjustment_channel') as int) = 2 then 'other'
                      end as compensation_channel   
                ,date(from_unixtime(mco.create_time - 60*60)) as compensate_date    
                ,json_extract_scalar(mco.extra_data,'$.cs_note') as cs_note
                --,json_extract_scalar(mco.extra_data,'$.reference') as FIN_note
                ,split(json_extract_scalar(mco.extra_data,'$.reference'),'_')[4] as FIN_note
                ,mco.extra_data
                ,dot.uid AS shipper_id 

FROM shopeefood.foody_delivery_admin_db__admin_order_payment_adjustment_tab__vn_daily_s0_live mco

-- foody_accountant_db__accountant_partner_transaction_adjustment_tab__vn_daily_s0_live

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE date(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = mco.order_id
    and dot.ref_order_category = 0

WHERE 1 = 1
and cast(json_extract_scalar(mco.extra_data,'$.adjustment_type') as int) = 1
and mco.object_type = 1 --- driver adjustment ---
and date(from_unixtime(mco.create_time - 3600)) between date'2023-06-26' and date'2023-07-02'