SELECT  
         user_id
        ,txn_type
        ,CASE 
                WHEN txn_type = 1 THEN 'GOFAST_ADD_DEPOSIT '
                WHEN txn_type = 2 THEN 'GOFAST_ADD_REWARD '
                WHEN txn_type = 3 THEN 'GOFAST_ADD_ADVANCE '
                WHEN txn_type = 4 THEN 'GOFAST_ADD_PAYMENT '
                WHEN txn_type = 5 THEN 'GOFAST_ADD_STUFF_REFUND '
                WHEN txn_type = 50 THEN 'GOFAST_DEDUCT_DEPOSIT '
                WHEN txn_type = 51 THEN 'GOFAST_DEDUCT_PENALTY '
                WHEN txn_type = 52 THEN 'GOFAST_DEDUCT_ADVANCE '
                WHEN txn_type = 53 THEN 'GOFAST_DEDUCT_PAYMENT '
                WHEN txn_type = 54 THEN 'GOFAST_DEDUCT_STUFF '
                WHEN txn_type = 101 THEN 'DELIVERY_ADD_BONUS '
                WHEN txn_type = 102 THEN 'DELIVERY_ADD_MERCHANT_PAID_AMOUNT '
                WHEN txn_type = 103 THEN 'DELIVERY_ADD_DEPOSIT '
                WHEN txn_type = 104 THEN 'DELIVERY_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 105 THEN 'DELIVERY_ADD_BONUS_MANUAL '
                WHEN txn_type = 106 THEN 'DELIVERY_ADD_PARKING_FEE '
                WHEN txn_type = 107 THEN 'DELIVERY_ADD_MERCHANT_PARKING_FEE '
                WHEN txn_type = 108 THEN 'DELIVERY_ADD_TIP_FEE '
                WHEN txn_type = 109 THEN 'DELIVERY_ADD_CUSTOMER_PARKING_FEE '
                WHEN txn_type = 110 THEN 'DELIVERY_ADD_AFTER_DELIVERY_TIP_FEE '
                WHEN txn_type = 111 THEN 'DELIVERY_ON_HOLD_ADD_TIP_FEE '
                WHEN txn_type = 112 THEN 'DELIVERY_ADD_BAD_WEATHER_FEE '
                WHEN txn_type = 113 THEN 'DELIVERY_ADD_HAND_DELIVERY_FEE '
                WHEN txn_type = 114 THEN 'DELIVERY_ADD_WEATHER_SUPPORT_SHARED '
                WHEN txn_type = 115 THEN 'DELIVERY_ADD_WEATHER_SUPPORT_PASSTHROUGH '
                WHEN txn_type = 116 THEN 'DELIVERY_ADD_HOLIDAY_FEE_SHARED '
                WHEN txn_type = 117 THEN 'DELIVERY_ADD_HOLIDAY_FEE_PASSTHROUGH '
                WHEN txn_type = 118 THEN 'DELIVERY_ADD_LATE_NIGHT_FEE_SHARED '
                WHEN txn_type = 119 THEN 'DELIVERY_ADD_LATE_NIGHT_FEE_PASSTHROUGH '
                WHEN txn_type = 120 THEN 'DELIVERY_ADD_PLATFORM_SERVICE_FEE_SHARED '
                WHEN txn_type = 121 THEN 'DELIVERY_ADD_PLATFORM_SERVICE_FEE_PASSTHROUGH '
                WHEN txn_type = 122 THEN 'DELIVERY_ADD_SMALL_ORDER_FEE_SHARED '
                WHEN txn_type = 123 THEN 'DELIVERY_ADD_SMALL_ORDER_FEE_PASSTHROUGH '
                WHEN txn_type = 124 THEN 'DELIVERY_ADD_NON_PARTNER_MERCHANT_FEE_SHARED '
                WHEN txn_type = 125 THEN 'DELIVERY_ADD_NON_PARTNER_MERCHANT_FEE_PASSTHROUGH '
                WHEN txn_type = 126 THEN 'DELIVERY_ADD_MINIMUM_REQUIRE_MERCHANT_FEE_SHARED '
                WHEN txn_type = 127 THEN 'DELIVERY_ADD_MINIMUM_REQUIRE_MERCHANT_FEE_PASSTHROUGH '
                WHEN txn_type = 128 THEN 'DELIVERY_ADD_HAND_DELIVERY_FEE_SHARED '
                WHEN txn_type = 129 THEN 'DELIVERY_ADD_HAND_DELIVERY_FEE_PASSTHROUGH '
                WHEN txn_type = 130 THEN 'DELIVERY_ADD_PARKING_FEE_SHARED '
                WHEN txn_type = 131 THEN 'DELIVERY_ADD_PARKING_FEE_PASSTHROUGH '
                WHEN txn_type = 132 THEN 'DELIVERY_ADD_MERCHANT_PARKING_FEE_SHARED '
                WHEN txn_type = 133 THEN 'DELIVERY_ADD_MERCHANT_PARKING_FEE_PASSTHROUGH '
                WHEN txn_type = 134 THEN 'DELIVERY_ADD_TIP_FEE_SHARED '
                WHEN txn_type = 135 THEN 'DELIVERY_ADD_TIP_FEE_PASSTHROUGH '
                WHEN txn_type = 136 THEN 'DELIVERY_ADD_PACKAGING_FEE_SHARED '
                WHEN txn_type = 137 THEN 'DELIVERY_ADD_PACKAGING_FEE_PASSTHROUGH '
                WHEN txn_type = 151 THEN 'DELIVERY_DEDUCT_DEPOSIT '
                WHEN txn_type = 152 THEN 'DELIVERY_DEDUCT_COLLECTED_MONEY '
                WHEN txn_type = 153 THEN 'DELIVERY_DEDUCT_QUIT_ORDER_RISK '
                WHEN txn_type = 154 THEN 'DELIVERY_ON_HOLD_DEDUCT_TIP_FEE '
                WHEN txn_type = 200 THEN 'NOW_SHIP_USER_ADD_BONUS '
                WHEN txn_type = 201 THEN 'NOW_SHIP_USER_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 202 THEN 'NOW_SHIP_USER_ADD_RECEIVED_SHIPPING_FEE_RETURNED '
                WHEN txn_type = 203 THEN 'NOW_SHIP_USER_ADD_PARKING_FEE '
                WHEN txn_type = 204 THEN 'NOW_SHIP_USER_ADD_BONUS_MANUAL '
                WHEN txn_type = 205 THEN 'NOW_SHIP_USER_ADD_HAND_DELIVERY_FEE '
                WHEN txn_type = 206 THEN 'NOW_SHIP_USER_ADD_PICK_PARKING_FEE '
                WHEN txn_type = 207 THEN 'NOW_SHIP_USER_ADD_DROP_PARKING_FEE '
                WHEN txn_type = 208 THEN 'NOW_SHIP_USER_ADD_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 251 THEN 'NOW_SHIP_USER_DEDUCT_COLLECTED_MONEY '
                WHEN txn_type = 252 THEN 'NOW_SHIP_USER_DEDUCT_COLLECTED_SHIPPING_RETURNED '
                WHEN txn_type = 253 THEN 'NOW_SHIP_USER_DEDUCT_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 254 THEN 'NOW_SHIP_USER_DEDUCT_PICK_PARKING_FEE '
                WHEN txn_type = 255 THEN 'NOW_SHIP_USER_DEDUCT_DROP_PARKING_FEE '
                WHEN txn_type = 300 THEN 'NOW_SHIP_MERCHANT_ADD_BONUS '
                WHEN txn_type = 301 THEN 'NOW_SHIP_MERCHANT_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 302 THEN 'NOW_SHIP_MERCHANT_ADD_RECEIVED_SHIPPING_FEE_RETURNED '
                WHEN txn_type = 303 THEN 'NOW_SHIP_MERCHANT_ADD_PARKING_FEE '
                WHEN txn_type = 304 THEN 'NOW_SHIP_MERCHANT_ADD_BONUS_MANUAL '
                WHEN txn_type = 305 THEN 'NOW_SHIP_MERCHANT_ADD_HAND_DELIVERY_FEE '
                WHEN txn_type = 306 THEN 'NOW_SHIP_MERCHANT_ADD_PICK_PARKING_FEE '
                WHEN txn_type = 307 THEN 'NOW_SHIP_MERCHANT_ADD_DROP_PARKING_FEE '
                WHEN txn_type = 308 THEN 'NOW_SHIP_MERCHANT_ADD_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 351 THEN 'NOW_SHIP_MERCHANT_DEDUCT_COLLECTED_MONEY '
                WHEN txn_type = 352 THEN 'NOW_SHIP_MERCHANT_DEDUCT_COLLECTED_SHIPPING_RETURNED '
                WHEN txn_type = 353 THEN 'NOW_SHIP_MERCHANT_DEDUCT_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 354 THEN 'NOW_SHIP_MERCHANT_DEDUCT_PICK_PARKING_FEE '
                WHEN txn_type = 355 THEN 'NOW_SHIP_MERCHANT_DEDUCT_DROP_PARKING_FEE '
                WHEN txn_type = 400 THEN 'NOW_MOTO_ADD_BONUS '
                WHEN txn_type = 401 THEN 'NOW_MOTO_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 402 THEN 'NOW_MOTO_ADD_RECEIVED_SHIPPING_FEE_RETURNED '
                WHEN txn_type = 403 THEN 'NOW_MOTO_ADD_PARKING_FEE '
                WHEN txn_type = 404 THEN 'NOW_MOTO_ADD_BONUS_MANUAL '
                WHEN txn_type = 405 THEN 'NOW_MOTO_ADD_HAND_DELIVERY_FEE '
                WHEN txn_type = 406 THEN 'NOW_MOTO_ADD_PICK_PARKING_FEE '
                WHEN txn_type = 407 THEN 'NOW_MOTO_ADD_DROP_PARKING_FEE '
                WHEN txn_type = 451 THEN 'NOW_MOTO_DEDUCT_COLLECTED_MONEY '
                WHEN txn_type = 452 THEN 'NOW_MOTO_DEDUCT_COLLECTED_SHIPPING_RETURNED '
                WHEN txn_type = 453 THEN 'NOW_MOTO_DEDUCT_PICK_PARKING_FEE '
                WHEN txn_type = 454 THEN 'NOW_MOTO_DEDUCT_DROP_PARKING_FEE '
                WHEN txn_type = 501 THEN 'ACCOUNTANT_ADD_ADJUSTMENT '
                WHEN txn_type = 502 THEN 'ACCOUNTANT_ADD_OTHER '
                WHEN txn_type = 503 THEN 'ACCOUNTANT_ADD_PAYMENT '
                WHEN txn_type = 504 THEN 'ACCOUNTANT_ADD_INITIAL_BALANCE '
                WHEN txn_type = 505 THEN 'ACCOUNTANT_ADD_WEEKLY_BONUS '
                WHEN txn_type = 506 THEN 'ACCOUNTANT_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 507 THEN 'ACCOUNTANT_ADD_BONUS '
                WHEN txn_type = 508 THEN 'ACCOUNTANT_ADD_PAYMENT_BY_SUNMI '
                WHEN txn_type = 509 THEN 'ACCOUNTANT_ADD_DEPOSIT '
                WHEN txn_type = 510 THEN 'ACCOUNTANT_ADD_DEPOSIT_BONUS_TO_AVAILABLE_BALANCE '
                WHEN txn_type = 511 THEN 'ACCOUNTANT_ADD_DEPOSIT_BONUS_TO_DEPOSIT '
                WHEN txn_type = 512 THEN 'ACCOUNTANT_ADD_DAILY_BONUS '
                WHEN txn_type = 513 THEN 'ACCOUNTANT_ADD_BANK_TRANSFER '
                WHEN txn_type = 514 THEN 'ACCOUNTANT_ADD_ACA '
                WHEN txn_type = 515 THEN 'ACCOUNTANT_ADD_SHARED_REVENUE_PIT '
                WHEN txn_type = 516 THEN 'ACCOUNTANT_ADD_DAILY_BONUS_PIT '
                WHEN txn_type = 517 THEN 'ACCOUNTANT_ADD_COLLECT_TAX '
                WHEN txn_type = 518 THEN 'ACCOUNTANT_ADD_ADJUST_SHIPPING_FEE '
                WHEN txn_type = 519 THEN 'ACCOUNTANT_ADD_ADJUST_OTHERS '
                WHEN txn_type = 520 THEN 'ACCOUNTANT_ADD_ADJUST_ORDER_BONUS '
                WHEN txn_type = 521 THEN 'ACCOUNTANT_ADD_ADJUST_OTHER_INCOME '
                WHEN txn_type = 551 THEN 'ACCOUNTANT_DEDUCT_ADJUSTMENT '
                WHEN txn_type = 552 THEN 'ACCOUNTANT_DEDUCT_OTHER '
                WHEN txn_type = 553 THEN 'ACCOUNTANT_DEDUCT_PAYMENT '
                WHEN txn_type = 554 THEN 'ACCOUNTANT_DEDUCT_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 555 THEN 'ACCOUNTANT_DEDUCT_BONUS '
                WHEN txn_type = 556 THEN 'ACCOUNTANT_DEDUCT_BUYING_QUIT_ORDER '
                WHEN txn_type = 557 THEN 'ACCOUNTANT_DEDUCT_DEPOSIT '
                WHEN txn_type = 558 THEN 'ACCOUNTANT_DEDUCT_DEPOSIT_BONUS_TO_AVAILABLE_BALANCE '
                WHEN txn_type = 559 THEN 'ACCOUNTANT_DEDUCT_DEPOSIT_BONUS_TO_DEPOSIT '
                WHEN txn_type = 560 THEN 'ACCOUNTANT_DEDUCT_DAILY_BONUS '
                WHEN txn_type = 561 THEN 'ACCOUNTANT_DEDUCT_ACA '
                WHEN txn_type = 562 THEN 'ACCOUNTANT_DEDUCT_SHARED_REVENUE_PIT '
                WHEN txn_type = 563 THEN 'ACCOUNTANT_DEDUCT_DAILY_BONUS_PIT '
                WHEN txn_type = 564 THEN 'ACCOUNTANT_DEDUCT_COLLECT_TAX '
                WHEN txn_type = 565 THEN 'ACCOUNTANT_DEDUCT_ADJUST_SHIPPING_FEE '
                WHEN txn_type = 566 THEN 'ACCOUNTANT_DEDUCT_ADJUST_OTHERS '
                WHEN txn_type = 567 THEN 'ACCOUNTANT_DEDUCT_ADJUST_ORDER_BONUS '
                WHEN txn_type = 568 THEN 'ACCOUNTANT_DEDUCT_ADJUST_OTHER_INCOME '
                WHEN txn_type = 600 THEN 'PAYNOW_ADD_PAYMENT '
                WHEN txn_type = 601 THEN 'PAYNOW_ADD_PAYMENT_REFUND_VIA_SACOMBANK '
                WHEN txn_type = 602 THEN 'PAYNOW_ADD_PAYMENT_BY_WITHDRAWAL_REFUND_VIA_SACOMBANK '
                WHEN txn_type = 603 THEN 'PAYNOW_ADD_PAYMENT_BY_WITHDRAWAL_CANCEL_VIA_SACOMBANK '
                WHEN txn_type = 604 THEN 'PAYNOW_ADD_PAYMENT_BY_SACOMBANK '
                WHEN txn_type = 605 THEN 'PAYNOW_ADD_PAYMENT_BY_AIRPAY '
                WHEN txn_type = 606 THEN 'PAYNOW_ADD_ACA '
                WHEN txn_type = 650 THEN 'PAYNOW_DEDUCT_PAYMENT_VIA_SACOMBANK '
                WHEN txn_type = 651 THEN 'PAYNOW_DEDUCT_PAYMENT_REFUND_VIA_SACOMBANK '
                WHEN txn_type = 652 THEN 'PAYNOW_DEDUCT_PAYMENT_VIA_AIRPAY '
                WHEN txn_type = 653 THEN 'PAYNOW_DEDUCT_PAYMENT_REFUND_VIA_AIRPAY '
                WHEN txn_type = 654 THEN 'PAYNOW_DEDUCT_ACA '
                WHEN txn_type = 701 THEN 'SYSTEM_ADD_WEEKLY_BONUS '
                WHEN txn_type = 702 THEN 'SYSTEM_ADD_REFUND_DEPOSIT '
                WHEN txn_type = 750 THEN 'SYSTEM_DEDUCT_DEPOSIT '
                WHEN txn_type = 801 THEN 'PARTNER_ADD_DEPOSIT_BONUS_TO_AVAILABLE_BALANCE '
                WHEN txn_type = 802 THEN 'PARTNER_ADD_DEPOSIT_BONUS_TO_DEPOSIT '
                WHEN txn_type = 803 THEN 'PARTNER_ADD_DEPOSIT_VIA_SACOMBANK '
                WHEN txn_type = 804 THEN 'PARTNER_ADD_DEPOSIT_VIA_AIRPAY '
                WHEN txn_type = 900 THEN 'SYSTEM_ADD_DAILY_BONUS '
                WHEN txn_type = 901 THEN 'SYSTEM_DEDUCT_DAILY_BONUS '
                WHEN txn_type = 902 THEN 'SYSTEM_ADD_COLLECT_TAX '
                WHEN txn_type = 903 THEN 'SYSTEM_DEDUCT_COLLECT_TAX '
                WHEN txn_type = 904 THEN 'SYSTEM_DEDUCT_SHARED_REVENUE_PIT '
                WHEN txn_type = 905 THEN 'SYSTEM_DEDUCT_DAILY_BONUS_PIT '
                WHEN txn_type = 906 THEN 'DELI_HUB_MODEL_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 907 THEN 'HUB_MODEL_ADD_DAILY_BONUS '
                WHEN txn_type = 908 THEN 'SYSTEM_ADD_ADJUSTMENT '
                WHEN txn_type = 909 THEN 'SYSTEM_ADD_ADJUST_ORDER_BONUS '
                WHEN txn_type = 910 THEN 'SYSTEM_ADD_ADJUST_OTHER_INCOME '
                WHEN txn_type = 950 THEN 'HUB_MODEL_DEDUCT_DAILY_BONUS '
                WHEN txn_type = 951 THEN 'DELI_HUB_MODEL_DEDUCT_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 952 THEN 'SYSTEM_DEDUCT_ADJUSTMENT '
                WHEN txn_type = 953 THEN 'SYSTEM_DEDUCT_ADJUST_ORDER_BONUS '
                WHEN txn_type = 954 THEN 'SYSTEM_DEDUCT_ADJUST_OTHER_INCOME '
                WHEN txn_type = 1000 THEN 'NOW_SHIP_SHOPEE_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 1001 THEN 'NOW_SHIP_SHOPEE_ADD_RECEIVED_SHIPPING_FEE_RETURNED '
                WHEN txn_type = 1002 THEN 'NOW_SHIP_SHOPEE_ADD_PARKING_FEE '
                WHEN txn_type = 1003 THEN 'NOW_SHIP_SHOPEE_ADD_BONUS_MANUAL '
                WHEN txn_type = 1004 THEN 'NOW_SHIP_SHOPEE_ADD_HAND_DELIVERY_FEE '
                WHEN txn_type = 1005 THEN 'NOW_SHIP_SHOPEE_ADD_PICK_PARKING_FEE '
                WHEN txn_type = 1006 THEN 'NOW_SHIP_SHOPEE_ADD_BONUS '
                WHEN txn_type = 1007 THEN 'NOW_SHIP_SHOPEE_ADD_DROP_PARKING_FEE '
                WHEN txn_type = 1008 THEN 'NOW_SHIP_SHOPEE_ADD_AUTO_BONUS '
                WHEN txn_type = 1050 THEN 'NOW_SHIP_SHOPEE_DEDUCT_COLLECTED_MONEY '
                WHEN txn_type = 1051 THEN 'NOW_SHIP_SHOPEE_DEDUCT_COLLECTED_SHIPPING_RETURNED '
                WHEN txn_type = 1052 THEN 'NOW_SHIP_SHOPEE_DEDUCT_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 1053 THEN 'NOW_SHIP_SHOPEE_DEDUCT_PICK_PARKING_FEE '
                WHEN txn_type = 1054 THEN 'NOW_SHIP_SHOPEE_DEDUCT_DROP_PARKING_FEE '
                WHEN txn_type = 1055 THEN 'NOW_SHIP_SHOPEE_DEDUCT_AUTO_BONUS '
                WHEN txn_type = 2000 THEN 'NOW_SHIP_SAME_DAY_ADD_BONUS '
                WHEN txn_type = 2001 THEN 'NOW_SHIP_SAME_DAY_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 2002 THEN 'NOW_SHIP_SAME_DAY_ADD_RECEIVED_SHIPPING_FEE_RETURNED '
                WHEN txn_type = 2003 THEN 'NOW_SHIP_SAME_DAY_ADD_PARKING_FEE '
                WHEN txn_type = 2004 THEN 'NOW_SHIP_SAME_DAY_ADD_BONUS_MANUAL '
                WHEN txn_type = 2005 THEN 'NOW_SHIP_SAME_DAY_ADD_HAND_DELIVERY_FEE '
                WHEN txn_type = 2006 THEN 'NOW_SHIP_SAME_DAY_ADD_PICK_PARKING_FEE '
                WHEN txn_type = 2007 THEN 'NOW_SHIP_SAME_DAY_ADD_DROP_PARKING_FEE '
                WHEN txn_type = 2008 THEN 'NOW_SHIP_SAME_DAY_ADD_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 2051 THEN 'NOW_SHIP_SAME_DAY_DEDUCT_COLLECTED_MONEY '
                WHEN txn_type = 2052 THEN 'NOW_SHIP_SAME_DAY_DEDUCT_COLLECTED_SHIPPING_RETURNED '
                WHEN txn_type = 2053 THEN 'NOW_SHIP_SAME_DAY_DEDUCT_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 2054 THEN 'NOW_SHIP_SAME_DAY_DEDUCT_PICK_PARKING_FEE '
                WHEN txn_type = 2055 THEN 'NOW_SHIP_SAME_DAY_DEDUCT_DROP_PARKING_FEE '
                WHEN txn_type = 2100 THEN 'NOW_SHIP_MULTI_DROP_ADD_BONUS '
                WHEN txn_type = 2101 THEN 'NOW_SHIP_MULTI_DROP_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 2102 THEN 'NOW_SHIP_MULTI_DROP_ADD_RECEIVED_SHIPPING_FEE_RETURNED '
                WHEN txn_type = 2103 THEN 'NOW_SHIP_MULTI_DROP_ADD_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 2104 THEN 'NOW_SHIP_MULTI_DROP_ADD_PICK_PARKING_FEE '
                WHEN txn_type = 2105 THEN 'NOW_SHIP_MULTI_DROP_ADD_DROP_PARKING_FEE '
                WHEN txn_type = 2106 THEN 'NOW_SHIP_MULTI_DROP_ADD_BONUS_MANUAL '
                WHEN txn_type = 2150 THEN 'NOW_SHIP_MULTI_DROP_DEDUCT_BONUS '
                WHEN txn_type = 2151 THEN 'NOW_SHIP_MULTI_DROP_DEDUCT_COLLECTED_MONEY '
                WHEN txn_type = 2152 THEN 'NOW_SHIP_MULTI_DROP_DEDUCT_COLLECTED_SHIPPING_RETURNED '
                WHEN txn_type = 2153 THEN 'NOW_SHIP_MULTI_DROP_DEDUCT_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 2154 THEN 'NOW_SHIP_MULTI_DROP_DEDUCT_PICK_PARKING_FEE '
                WHEN txn_type = 2155 THEN 'NOW_SHIP_MULTI_DROP_DEDUCT_DROP_PARKING_FEE '
                WHEN txn_type = 3000 THEN 'NOW_SHIP_SPX_ADD_RECEIVED_SHIPPING_FEE '
                WHEN txn_type = 3001 THEN 'NOW_SHIP_SPX_ADD_RECEIVED_SHIPPING_FEE_RETURNED '
                WHEN txn_type = 3002 THEN 'NOW_SHIP_SPX_ADD_PARKING_FEE '
                WHEN txn_type = 3003 THEN 'NOW_SHIP_SPX_ADD_BONUS_MANUAL '
                WHEN txn_type = 3004 THEN 'NOW_SHIP_SPX_ADD_HAND_DELIVERY_FEE '
                WHEN txn_type = 3005 THEN 'NOW_SHIP_SPX_ADD_PICK_PARKING_FEE '
                WHEN txn_type = 3006 THEN 'NOW_SHIP_SPX_ADD_BONUS '
                WHEN txn_type = 3007 THEN 'NOW_SHIP_SPX_ADD_DROP_PARKING_FEE '
                WHEN txn_type = 3008 THEN 'NOW_SHIP_SPX_ADD_AUTO_BONUS '
                WHEN txn_type = 3050 THEN 'NOW_SHIP_SPX_DEDUCT_COLLECTED_MONEY '
                WHEN txn_type = 3051 THEN 'NOW_SHIP_SPX_DEDUCT_COLLECTED_SHIPPING_RETURNED '
                WHEN txn_type = 3052 THEN 'NOW_SHIP_SPX_DEDUCT_COLLECTED_ITEM_VALUE '
                WHEN txn_type = 3053 THEN 'NOW_SHIP_SPX_DEDUCT_PICK_PARKING_FEE '
                WHEN txn_type = 3054 THEN 'NOW_SHIP_SPX_DEDUCT_DROP_PARKING_FEE '
                WHEN txn_type = 3055 THEN 'NOW_SHIP_SPX_DEDUCT_AUTO_BONUS '
                END AS txn_type_name  
        ,(balance+deposit)/CAST(100 AS DOUBLE) AS balance
        ,FROM_UNIXTIME(create_time - 3600) AS created_timestamp 
        ,note
        ,DATE(FROM_UNIXTIME(create_time - 3600)) AS  created_date

FROM shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

WHERE 1 = 1 
-- AND txn_type in (600,601,602,603,604,605,606,650,651,652,653,654)
-- -- ,801,802,803,804)
-- AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2023-03-01' AND DATE'2023-04-03'
-- AND user_id IN 
-- (40769188
-- ,40719140
-- ,40652013
-- ,40623523
-- ,40354530
-- ,40266052
-- ,40157297
-- ,40038456
-- ,23122656
-- ,22181184)

ORDER BY 1,5 DESC