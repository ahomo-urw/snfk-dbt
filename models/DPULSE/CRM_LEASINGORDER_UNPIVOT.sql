create or replace view EXPOSE_DEV.DPULSE.CRM_LEASINGORDER_UNPIVOT(
	LEASING_ORDER_ID,
	INDICATOR_NAME,
	INDICATOR_AMOUNT,
	CURRENCY,
	RU_USAGE_CALC,
	PERCENTAGE,
	STARTDATE,
	ENDDATE,
) COMMENT='AGAUDET: Created for external vizualisation tool and Plan-It Interface => Only Target KPIs are used-AHOMO modif202505'
 as 
 
with lt_leasing_order_pivot  as ( 
                                 select leasing_order_id--, leasing_order_id_high_level
                                      , indicator_name, indicator_amount
                                      , case when lower(indicator_name) = 'available_indicator_lo' then '' else currency end as currency 
                                      ,'All' as RU_USAGE_CALC
                                      , null as percentage, null as startdate, null as enddate 
                                 from dpulse.crm_leasingorder_w_key
                                 unpivot ( indicator_amount for indicator_name in (
                                             TARGET_MGR,TARGET_PARKING_MGR,TARGET_SEPARATED_STORAGE_MGR/*,TARGET_RENT_FREE_PERIOD SGE_TO_DO*/
                                            , TARGET_FOC,TARGET_STEP_RENT
                                            , TARGET_RELETTING_WORKS
                                            , TARGET_KEY_MONEY--,TARGET_EVICTION_COST
                                            
                                            , available_indicator_lo    )               )    
                                 ),
      lt_leasing_order_pivot_dates  as ( 
                                 select leasing_order_id
                                      , indicator_name
                                      , 1   as indicator_amount
                                      , ''  as currency
                                      ,'None' as RU_USAGE_CALC
                                      , null as percentage
                                      , startdate
                                      , null as enddate 
                                 from dpulse.crm_leasingorder_w_key
                                 unpivot ( startdate for indicator_name in (
                                             CREATED_ON
                                            ,TARGET_SIGNATURE_DATE )               )    
                                 ),
        lt_final as ( select leasing_order_id , indicator_name, indicator_amount, currency, RU_USAGE_CALC, percentage, startdate, enddate from lt_leasing_order_pivot
            union all select leasing_order_id , indicator_name, indicator_amount, currency, RU_USAGE_CALC, percentage, startdate, enddate from lt_leasing_order_pivot_dates)                         
                               
    select leasing_order_id--, leasing_order_id_high_level
         , indicator_name, indicator_amount
          ,decode(indicator_name 
                              ,'',''
                              --,upper('TARGET_CONTRACT_TERM'),''
                              --,upper('TARGET_FIRM_PERIOD'), ''
                              --,upper('TOTAL_GLA'), ''
                              ,currency) as currency
         ,RU_USAGE_CALC, percentage, startdate, enddate 
    from lt_final
    where indicator_amount <> 0;