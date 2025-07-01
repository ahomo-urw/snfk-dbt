create or replace view EXPOSE_DEV.DPULSE.CRM_DEAL_UNPIVOT(
	LEASING_ORDER_ID,
	DEAL_ID,
	QUOTE_ID,
	INDICATOR_NAME,
	INDICATOR_AMOUNT,
	CURRENCY,
	RU_USAGE_CALC,
	PERCENTAGE,
	STARTDATE,
	ENDDATE,
    DEAL_DESC,       
    PROGRESSION_PERCENTAGE, 
    CREATED_ON,
    DATEFINANCIALPROPOSALAGREED,
    DATEVALIDATED,
    SIGNATUREDATE,
    SIGNATUREDATE_PROCESS
) COMMENT='AGAUDET: Created for external vizualisation tool and Plan-It Interface- modif AHOMO202505'
 as 
    with 
     lt_deal_referential     as (select * from dpulse.crm_deal_w_key ), --table temporaire crm_deal_w_key

     ---selection des champs de crm_deal_w_key+ mini cleaning
     lt_rent_incentives      as (
             select  
                --dl.key_leasing_order_deal
                dl.leasing_order_id
              , dl.deal_id
              , dl.quote_id
              , ri.deductionamountcalc     as indicator_amount
              , ri.percentagecalc / 100    as percentage
              , ri.startdate
              , ri.enddatecalc as enddate
              , ifnull(curr.isocurrencycode,'') as currency
              , decode( typecodename
                      , 'Step Rent','STEP_RENT'
                      , 'Rent Free Period','RENT_FREE_PERIOD'
                      , 'ERROR_RI' )            as indicator_name --cap_typecode --typecode
              , unitcodename                    as RU_USAGE_CALC --unitcode
--aho 20250520
              ,dl.DEAL_DESC       
                ,dl.PROGRESSION_PERCENTAGE 
                ,dl.CREATED_ON
                ,dl.DATEFINANCIALPROPOSALAGREED
                ,dl.DATEVALIDATED
                ,dl.SIGNATUREDATE
                ,dl.SIGNATUREDATE_PROCESS
            from            dpulse.crm_steprent             as ri
            inner join      lt_deal_referential             as dl   on dl.quote_id = ri.quote_id
            left outer join dpulse.crm_transactioncurrency  as curr on curr.transactioncurrency_id = ri.transactioncurrency_id
            where ifnull(ri.deductionamountcalc,0) <> 0
              and typecodename is not null
      ),
---transposition des indicateurs
lt_deal_pivot           as ( 
                         select --key_leasing_order_deal
                                leasing_order_id, deal_id, quote_id
                              , indicator_name
                              , indicator_amount
                              , case when lower(indicator_name) = 'available_indicator_deal' then '' else currency end as currency 
                              , estimated_handover_date as startdate
                              , ifnull(enddate,estimated_handover_date) as enddate
--aho 2025
                            ,DEAL_DESC       
                            ,PROGRESSION_PERCENTAGE 
                            ,CREATED_ON
                            ,DATEFINANCIALPROPOSALAGREED
                            ,DATEVALIDATED
                            ,SIGNATUREDATE
                            ,SIGNATUREDATE_PROCESS
                         from lt_deal_referential
                         unpivot ( indicator_amount for indicator_name 
                         --liste des indicateurs recupérés
                                 in (available_indicator_deal
                                    ,New_Retail_MGR
                                    ,mgr_performance
                                    ,economic_rent
                                    ,effective_rent
                                    ,parking_mgr
                                    , RESILIATIONCOST
                                    ,INDEMNITIES_PAID_BY_TENANT
                                    ,expectedsales
                                    ,ECORENTPERFPERC
                                    ,ECORENTPERFPERCLFL
                                    --,ECONOMICRENTM
                                    ,ECONOMICRENTPERFORMANCE
                                    ,EFFRENTPERC
                                    ,EFFRENTPERCLFL
                                    --,EFFECTIVERENTM
                                    ,EFFECTIVERENTPERFORMANCE
                                    ,EVICTIONCOSTTECH
                                    ,FOCTECH
                                    ,KEYMONEYTECH
                                    ,RELETTINGWORKSTECH
                                    ,MGRPERFPERC
                                    ,MGRPERFOPERCLFL
                                    ,MGRVSBPPERF
                                    ,MGRVSERVPERF
                                    ,PARKINGMGRM2CALC
                                    ,PARKINGMGRM2MCALC
                                    ,PARKINGMGRYTECH
                                    ,NEWRETAILMGRBYM2
                                    ,RETAILMGRYTECH
                                    ,SSTMGRM2CALC
                                    ,SSTMGRM2MCALC
                                    ,SSTMGRYTECH
                                    ,STATEREASONCODE
                                    ,TOTALLI
                                    ,TOTALLIMOOFMGR
                                    ,MGRLFL
                                    ,MGRPERFLFL
                                    ,EFFECTIVERENTLFL
                                    --,EFFECTIVERENTM2LFL
                                    ,EFFECTIVERENTPERFLFL
                                    ,ECONOMICRENTLFL
                                    --,ECONOMICRENTM2LFL
                                    ,ECONOMICRENTPERFLFL
                                    , YEARLY_SBR_RDL
                                    , BCOM_RDL

--------------------------------------------------ASECK NRI 04022025

                                    ,qu_nriimpactn
                                    ,qu_nriimpactnplus1
                                    ,qu_nrivslon
                                    ,qu_nrivslonplus1
                                    ,qu_nrivsoldn
                                    ,qu_nrivsoldnplus1
                                    --,qu_oldnrilfln
                                    --,qu_oldnrilflnplus1
                        ) )
),
--pivotage des dates, recuperation des dates comme indicateur
        lt_deal_pivot_Dates     as (

                                select --key_leasing_order_deal
                                        leasing_order_id, deal_id, quote_id
                                      , indicator_name
                                      , startdate
                                      , startdate as enddate
                                 from lt_deal_referential
                                 unpivot ( startdate for indicator_name 
                                                     in (    Signature_Date_RPT
                                                            ,Estimated_handover_date
                                                            ,SIGNATUREDATE
                                                            ,estimatedsignaturedate
                                                            ,enddate
                                                            ,datefinancialproposalagreed
                                                            ,datevalidated
                                                            ,datelegalagreement
                                                            ,signaturedate_process
                                                            ,Created_On
                                                            ,modified_on    ) )
                                )
----aggregation des tables temporaires

        select --key_leasing_order_deal, 
               leasing_order_id, deal_id, quote_id, indicator_name, indicator_amount
              ,decode(indicator_name 
                                  ,'',''
                                  ,currency) as currency
             
             , decode(indicator_name, 'RETAILMGRYTECH',         'Retail'
                                    , 'SSTMGRYTECH',            'Storage'
                                    , 'PARKINGMGRYTECH',        'Parking'
                                    , 'All' )                             as RU_USAGE_CALC  
             , null as percentage
             , startdate
             , enddate
--aho 2025
            ,DEAL_DESC       
            ,PROGRESSION_PERCENTAGE * 100 
            ,CREATED_ON
            ,DATEFINANCIALPROPOSALAGREED
            ,DATEVALIDATED
            ,SIGNATUREDATE
            ,SIGNATUREDATE_PROCESS
        from lt_deal_pivot
        where indicator_amount <> 0
    union all
        select --key_leasing_order_deal, 
               leasing_order_id, deal_id, quote_id, indicator_name, indicator_amount
             , currency, RU_USAGE_CALC
             , percentage
             , startdate, enddate
--aho 2025
            ,DEAL_DESC       
            ,PROGRESSION_PERCENTAGE *100
            ,CREATED_ON
            ,DATEFINANCIALPROPOSALAGREED
            ,DATEVALIDATED
            ,SIGNATUREDATE
            ,SIGNATUREDATE_PROCESS             
        from lt_rent_incentives
        where indicator_amount <> 0
    union all
        select --key_leasing_order_deal, 
               leasing_order_id, deal_id, quote_id, indicator_name
             , 1        as indicator_amount
             , ''       as currency
             , 'None'   as RU_USAGE_CALC
             , null     as percentage
             , startdate
             , null     as enddate
--aho 2025
            , null     as DEAL_DESC       
            , null     as PROGRESSION_PERCENTAGE 
            , null     as CREATED_ON
            , null     as DATEFINANCIALPROPOSALAGREED
            , null     as DATEVALIDATED
            , null     as SIGNATUREDATE
            , null     as SIGNATUREDATE_PROCESS             
        from lt_deal_pivot_Dates
        ;