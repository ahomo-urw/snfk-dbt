{{ config(materialized='view') }}

create or replace table EXPOSE_DEV.DPERFMGT.T_FACT_PERFORMANCE
--create or replace view  EXPOSE_DEV.DPULSE.V_FACT_PERFORMANCE_GLOBAL
COMMENT = 'AHOMO: Created for external vizualisation 2025' as --deal metrics

--create or replace table as select * from  FACT_LO_DEAL_GLOBAL
--create or replace table as select * from  EXPOSE_DEV.DPULSE.CRM_RENTALUNIT_W_KEY

WITH lt_deal_and_lo_pivot as (
    select
        *
    from
        DPERFMGT.T_FACT_LO_DEAL_PERFORMANCE --attention penser a rafraichir la table !!!!
),
--spot vacancy
LT_PLANIT_OPERATION_CUBE_RENT_ROLL_FOR_VACANCY_W_CURRENCY_AGG AS (
    SELECT
        key_r,
        currency,
        indicator_name,
        indicator_num
    FROM
        EXPOSE.DPERFMGT.PLANIT_OPERATION_CUBE_RENT_ROLL_FOR_VACANCY_W_CURRENCY_AGG
    UNION ALL
    SELECT
        key_r,
        null currency,
        indicator_name,
        indicator_num
    FROM
        EXPOSE.DPERFMGT.PLANIT_OPERATION_CUBE_RENT_ROLL_FOR_VACANCY_Wo_CURRENCY_AGG
),
---currency
lt_planit_transverse_table_fx_rate_pivot_currency as (
    select
        *
    from
        EXPOSE.DPERFMGT.planit_transverse_table_fx_rate_pivot_currency
),
lt_crm_rentalunit_w_key as (
    select
        *
    from
        DPULSE.T_CRM_RENTALUNIT_W_KEY --attention penser a rafraichir la table !!!!
),
--meilleure quote du deal selon l'avancement
lt_crm_deal_quote_relevant as (
    select
        *
    from
        DPULSE.CRM_deal_quote_relevant
),
LT_TERRITORY AS (
    SELECT
        DISTINCT LTT_PC.MDMID AS MDMID_PC,
        LTT_BU.MDMID AS MDMID_BU
    FROM
        dpulse.crm_territory LTT_BU
        INNER JOIN dpulse.crm_territory LTT_PC ON LTT_BU.PARENTTERRITORYID = LTT_PC.TERRITORY_ID 
        and ltt_bu.typecode='809020000' --bu
        and ltt_pc.typecode='809020002' --pc
),


--choix du lo pour le deal/lo sans seal
lt_crm_lo_dl_link as (
    select
        *
    from
        DPULSE.CRM_LO_DL_LINK
),
lt_crm_leasingorder as (
    select
        *
    from
        DPULSE.crm_leasingorder
),
---enrichissement des deals
LT_CRM_DEAL as (
    SELECT
        *
    FROM
        DPULSE.CRM_DEAL
)
---branch lessee
,LT_CRM_ACCOUNT as (
    SELECT
        *
    FROM
        FINOPS.DWH.CRM_ACCOUNT
)
,LT_DEAL_ACCOUNT AS (
    SELECT
        DEAL.DEAL_ID AS DEAL_GUID,
        DEAL.ID AS DEAL_ID,
        DEAL.ACCOUNT_ID AS BRAND_GUID,
        BRD.ACCOUNTNUMBER AS BRAND_ID,
        DEAL.TENANT_ID AS LESSEE_GUID,
        LES.DUNSNUMBER AS LESSEE_ID,
    FROM
        LT_CRM_DEAL DEAL
        LEFT JOIN LT_CRM_ACCOUNT LES ON LES.ACCOUNTID = DEAL.ACCOUNT_ID
        AND LES.CATEGORY = '809020014' -- Lessee
        LEFT JOIN LT_CRM_ACCOUNT BRD ON BRD.ACCOUNTID = DEAL.ACCOUNT_ID
        AND BRD.CATEGORY = '809020001' --Brand
),
lt_rental_unit as (
    select
        LTT.MDMID_PC AS PC_ID,
        ru.BUEXTREF,
        ru.leasing_order_id,
        lo.id AS LO_ID,
        ru.deal_id DEAL_GUID,
        D.id deal_id , 
        DL.BRAND_GUID,
        DL.BRAND_ID,
        DL.LESSEE_GUID,
        DL.LESSEE_ID,
        ru.quote_id,
        ru.rental_unit_id,
        ru.rental_unit_ref_id,
        rental_unit_common_id,
        case
            when ru.deal_id = '' then 'LO'
            else 'DL'
        end as Object_Type,
        ru.currency,
        ifnull(ru.erv, 0)::number(17, 4) as erv,
        ifnull(ru.rrv, 0)::number(17, 4) as rrv,
        ifnull(ru.trv, 0)::number(17, 4) as trv,
        ifnull(old_mgr, 0)::number(17, 4) as old_mgr,
        ifnull(old_sbr, 0)::number(17, 4) as old_sbr -----aseck 26/05/2025
,
        ifnull(old_contract_code, '') as old_contract_code,
        ru.unit,
        ifnull(ru.gla, 0)::number(17, 4) as gla,
        ifnull(ru.old_gla, 0)::number(17, 4) as old_gla,
        ifnull(ru.gla_resized, 0)::number(17, 4) as gla_resized,
        ifnull(ru.TARGETEVICTIONCOST, 0)::number(17, 4) as TARGETEVICTIONCOST,
        ifnull(ru.TARGETDURATIONSHORTTERM, 0)::number(17, 4) as TARGETDURATIONSHORTTERM,
        ifnull(ru.TARGETMGRSHORTTERM, 0)::number(17, 4) as TARGETMGRSHORTTERM,
        ru.TARGETTERMINATIONDATE,
        ru.usage_split,
        ru.tot_gla,
        ru.tot_gla_usage,
        ru.allocation_gla,
        ru.allocation_gla_usage,
        'GENERAL' as source,
        dqr.YM_START,
        dqr.YM_END,
        dqr.FACT_PROG as percentage
    from
        lt_crm_rentalunit_w_key as ru ---materialisation de la table temporaire pour les test
       inner join lt_crm_deal_quote_relevant dqr on dqr.QUOTEID = ru.quote_id --on elimine tout de suite les quote inutiles cree Produit cartesien
        LEFT JOIN LT_TERRITORY LTT ON LTT.MDMID_BU = REPLACE(ru.BUEXTREF, '-', '/')  
        LEFT JOIN LT_DEAL_ACCOUNT DL ON DL.DEAL_GUID = ru.deal_id  
        LEFT JOIN LT_CRM_DEAL D ON D.deal_id = ru.deal_id  
        LEFT JOIN lt_crm_leasingorder lo ON lo.leasing_order_id = ru.leasing_order_id 
        /*left outer join dpulse.DIM_RENTALUNIT_COMMON_REFERENTIAL      as ref
                                                on     ( (ru.rental_unit_id             =  ref.rental_unit_id and ref.new_rentalunit = ''Yes')
                                                    or   (ru.RENTAL_UNIT_REF_ID             =  ref.RENTAL_UNIT_REF_ID and ref.new_rentalunit = ''No'))*/
    --where   ru.quote_id= '726b512c-d8c5-ee11-9079-000d3ab00f0f'
)
--select * from lt_rental_unit;--190,8
----recuperation de la gla, sbr, mgr...
,
lt_rental_unit_pivot as (
    select
        PC_ID,
        BUEXTREF,
        leasing_order_id,
        LO_ID,
        DEAL_GUID,
        DEAL_ID,
        BRAND_GUID,
        BRAND_ID,
        LESSEE_GUID,
        LESSEE_ID,
        quote_id,
        rental_unit_id,
        rental_unit_ref_id,
        rental_unit_common_id,
        Object_Type,
        indicator_name,
        indicator_amount,
        old_contract_code,
        decode(
            indicator_name,
            upper('gla'),
            unit,
            upper('old_gla'),
            unit,
            upper('gla_resized'),
            unit,
            ''
        ) as unit,
        decode(
            indicator_name,
            upper('erv'),
            currency,
            upper('rrv'),
            currency,
            upper('trv'),
            currency,
            upper('old_mgr'),
            currency,
            upper('old_sbr'),
            currency -----aseck 26/05/2025
,
            upper('TARGETEVICTIONCOST'),
            currency,
            upper('TARGETMGRSHORTTERM'),
            currency,
            ''
        ) as currency,
        source        ,
        ru.YM_START,
         ru.YM_END,
         ru.percentage
    from
        lt_rental_unit ru unpivot (
            indicator_amount for indicator_name in (
                gla,
                old_gla,
                gla_resized,
                erv,
                rrv,
                trv,
                old_mgr,
                old_sbr,
                TARGETEVICTIONCOST,
                TARGETDURATIONSHORTTERM,
                TARGETMGRSHORTTERM
            )
        )
       -- join lt_crm_deal_quote_relevant dqr on dqr.QUOTEID = ru.quote_id
) 
--select * from lt_rental_unit_pivot; --1,3M
/*,
lt_rental_unit_pivot_dates as (
    select
        leasing_order_id,
        deal_id,
        quote_id,
        rental_unit_id,
        rental_unit_ref_id,
        rental_unit_common_id,
        Object_Type,
        'TARGETTERMINATIONDATE' as indicator_name,
        1 as indicator_amount,
        '' as currency,
        null as unit,
        TARGETTERMINATIONDATE as startdate
        --                      , null as enddate
,
        null as percentage,
        0 as indicator_min_amount,
        0 as indicator_numberofmonths,
        null as frequency,
        null as calc_parameter_1,
        null as calc_parameter_2,
        null as islumpsum,
        source,
        old_contract_code
    from
        lt_rental_unit
)*/

--select * from lt_deal_and_lo_pivot;--190,8
--on recupere les données niveau deal qu'on joint avec les données rental unit
--find all the deal KPI and assign a void rental unit if needed
,
lt_deal_lo_fact as (
    select
        ru.PC_ID,
        ru.buextref,
        dqr.YM_START || '-' || dqr.YM_END as YM_START_END,
        dqr.YM_START as YM_START,
        dqr.YM_END as YM_END,
        fact.DEAL_CREATION,
        fact.DEAL_FINANCIALPROPOSALAGREED,
        fact.DEAL_DATEVALIDATED,
        fact.DEAL_SIGNATUREDATE,
        fact.DEAL_LANDLORDSIGNATUREDATE,
        fact.leasing_order_id,
        lo.ID AS LO_ID,
        fact.DEAL_ID as DEAL_GUID,
        ru.deal_ID as DEAL_ID,
        ru.BRAND_GUID,
        ru.BRAND_ID,
        ru.LESSEE_GUID,
        ru.LESSEE_ID,
        fact.quote_id AS quote_id,
        fact.Object_Type,
        fact.indicator_name,
        fact.indicator_amount,
        fact.indicator_min_amount,
        fact.indicator_numberofmonths,
        fact.frequency,
        fact.calc_parameter_1,
        fact.calc_parameter_2,
        fact.islumpsum,
        fact.source,
        fact.currency,
        fact.RU_USAGE_CALC,
        dqr.fact_prog as percentage,
        --aho
        ru.rental_unit_id,
        ru.rental_unit_ref_id,
        ru.rental_unit_common_id,
        ru.usage_split,
        ru.gla,
        ru.allocation_gla,
        ru.tot_gla,
        ru.allocation_gla_usage,
        ru.tot_gla_usage,
        case
            when ru.rental_unit_id is null then true
            else false
        end as is_rental_space_void,
        ru.old_contract_code
    from
        lt_deal_and_lo_pivot as fact
        JOIN lt_crm_deal_quote_relevant dqr ON fact.quote_id = dqr.QUOTEID --on vire les not relevant par inner join
        LEFT JOIN lt_crm_leasingorder lo ON lo.leasing_order_id = fact.leasing_order_id
        left join lt_rental_unit as ru on ru.leasing_order_id = fact.leasing_order_id
        and ru.deal_guid = fact.deal_id
        and ru.quote_id = fact.quote_id
        and fact.RU_USAGE_CALC <> 'None'
)
--select * from  lt_deal_lo_fact;--1,4M
    ---on alloue les montants en  fonction de la gla
,lt_dl_lo_deag_comp as (
        select
            PC_ID,
            buextref,
            YM_START_END,
            YM_START,
            YM_END,
            DEAL_CREATION,
            DEAL_FINANCIALPROPOSALAGREED,
            DEAL_DATEVALIDATED,
            DEAL_SIGNATUREDATE,
            DEAL_LANDLORDSIGNATUREDATE,
            leasing_order_id,
            LO_ID,
            DEAL_GUID,
            DEAL_ID,
            BRAND_GUID,
            BRAND_ID,
            LESSEE_GUID,
            LESSEE_ID,
            quote_id,
            ifnull(rental_unit_id, '') as rental_unit_id,
            ifnull(rental_unit_ref_id, '') as rental_unit_ref_id,
            ifnull(rental_unit_common_id, '') as rental_unit_common_id,
            Object_Type,
            indicator_name,
            case
                when is_rental_space_void then indicator_amount
                when RU_USAGE_CALC = 'All' then indicator_amount * allocation_gla
                when RU_USAGE_CALC <> 'All'
                and RU_USAGE_CALC = usage_split then indicator_amount * allocation_gla_usage
                else 0
            end as indicator_amount,
            /*      case
                        when is_rental_space_void then indicator_min_amount
                        when RU_USAGE_CALC = 'All' then indicator_min_amount * allocation_gla
                        when RU_USAGE_CALC <> 'All'
                        and RU_USAGE_CALC = usage_split then indicator_min_amount * allocation_gla_usage
                        else 0
                    end as indicator_min_amount,
                    indicator_numberofmonths,        --frequency,        calc_parameter_1,        calc_parameter_2,        islumpsum,        source,
             */
            case
                when RU_USAGE_CALC = 'None' then ''
                else currency
            end as currency,
            '' as unit,
            indicator_amount as before_calc_indicator_amount,
            allocation_gla,
            allocation_gla_usage,
            ru_usage_calc --                      , startdate
            --                      , enddate
            ,percentage,
            old_contract_code
        from
            lt_deal_lo_fact
    )
--select * from lt_dl_lo_deag_comp; --2,7M
--premiere aggregation des faits deal avec rental unit
,lt_fact_consolidation_flat as (
        select
            PC_ID,
            buextref,
            YM_START || '-' || YM_END as YM_START_END,
            YM_START,
            YM_END,
            DEAL_CREATION,
            DEAL_FINANCIALPROPOSALAGREED,
            DEAL_DATEVALIDATED,
            DEAL_SIGNATUREDATE,
            DEAL_LANDLORDSIGNATUREDATE,
            leasing_order_id,
            LO_ID,
            DEAL_GUID,
            DEAL_ID,
            BRAND_GUID,
            BRAND_ID,
            LESSEE_GUID,
            LESSEE_ID,
            quote_id,
            rental_unit_id,
            rental_unit_ref_id,
            rental_unit_common_id,
            Object_Type,
            indicator_name,
            indicator_amount,
            currency,
            unit,
            -- startdate  ,      -- enddate,
            percentage,
            --indicator_min_amount,        indicator_numberofmonths, frequency,         calc_parameter_1,        calc_parameter_2,        islumpsum,         source,         old_contract_code
        from
            lt_dl_lo_deag_comp --where         abs(indicator_amount) + abs(indicator_min_amount) + abs(indicator_numberofmonths) <> 0
        union all
        select
            PC_ID,
            buextref,
            YM_START || '-' || YM_END as YM_START_END,            --on met a vide car on n'a pas les infos deal au niveau rental unit
            YM_START,
            YM_END,
            null as DEAL_CREATION,
            null as DEAL_FINANCIALPROPOSALAGREED,
            null as DEAL_DATEVALIDATED,
            null as DEAL_SIGNATUREDATE,
            null as DEAL_LANDLORDSIGNATUREDATE,
            leasing_order_id,
            LO_ID,
            DEAL_GUID,
            DEAL_ID,
            BRAND_GUID,
            BRAND_ID,
            LESSEE_GUID,
            LESSEE_ID,
            quote_id,
            rental_unit_id,
            rental_unit_ref_id,
            rental_unit_common_id,
            Object_Type,
            indicator_name,
            indicator_amount,
            currency,
            unit,
            --, null as startdate ,       -- null as enddate,
            percentage
            --        0 as indicator_min_amount,        0 as indicator_numberofmonths,        null as frequency,
            --        null as calc_parameter_1,        null as calc_parameter_2,        null as islumpsum,        source,        old_contract_code
        from
            lt_rental_unit_pivot
            /*                            union all select leasing_order_id, deal_id, quote_id, rental_unit_id
                                                                            , rental_unit_ref_id
                                                                            , rental_unit_common_id
                                                                            , Object_Type,indicator_name, indicator_amount, currency, unit
                                                                            --, startdate
                                                                            --, enddate
                                                                            , percentage,indicator_min_amount, indicator_numberofmonths
                                                                            , frequency, calc_parameter_1
                                                                            ,calc_parameter_2,islumpsum, source, old_contract_code
                                                     from lt_rental_unit_pivot_dates 
                                    */
            --where startdate is not null
    ) 
    
    --select * from lt_fact_consolidation_flat;--4M
    --ajout des slots de currency
,    lt_result_lc as (
        select
            PC_ID,
            buextref,
            YM_START_END,
            YM_START,
            YM_END,
            DEAL_CREATION,
            DEAL_FINANCIALPROPOSALAGREED,
            DEAL_DATEVALIDATED,
            DEAL_SIGNATUREDATE,
            DEAL_LANDLORDSIGNATUREDATE,
            mn.leasing_order_id,
            mn.LO_ID,
            mn.DEAL_GUID,
            mn.DEAL_ID,
            mn.BRAND_GUID,
            mn.BRAND_ID,
            mn.LESSEE_GUID,
            mn.LESSEE_ID,
            mn.quote_id,
            mn.rental_unit_id,
            mn.rental_unit_ref_id,
            mn.rental_unit_common_id,
            mn.Object_Type,
            mn.indicator_name,
            mn.indicator_amount,
            --mn.indicator_min_amount,        mn.indicator_numberofmonths,        mn.frequency,        mn.calc_parameter_1,        mn.calc_parameter_2,        mn.islumpsum,        mn.source,
            ifnull(mn.currency, '') as currency,
            ifnull(mn.unit, '') as unit,
            --                      , startdate        --                      , enddate
            mn.percentage,
            --old_contract_code
        from
            lt_fact_consolidation_flat as mn --where indicator_amount <> 0
    )
--    select * from lt_result_lc;--6,2M
    
    --recuperation des taux de change de planit
,
    lt_fx_rate as (
        select
            curr_from,
            curr_to,
            rate
        from
            lt_planit_transverse_table_fx_rate_pivot_currency
        where
            cscenario = '5YBP_2025_2029'
            and cversion = 'VF'
            and mode_conversion = 'AVG'
            and curr_to = 'EUR'
            and calmonth = '202501'
    )
    
--select * from lt_fx_rate;
    --application des taux de changes
,
    lt_final_currency as (
        select
            PC_ID,
            buextref,
            YM_START_END,
            YM_START,
            YM_END,
            DEAL_CREATION,
            DEAL_FINANCIALPROPOSALAGREED,
            DEAL_DATEVALIDATED,
            DEAL_SIGNATUREDATE,
            DEAL_LANDLORDSIGNATUREDATE,
            leasing_order_id,
            LO_ID,
            DEAL_GUID,
            DEAL_ID,
            BRAND_GUID,
            BRAND_ID,
            LESSEE_GUID,
            LESSEE_ID,
            quote_id,
            rental_unit_id,
            rental_unit_ref_id,
            rental_unit_common_id,
            Object_Type,
            indicator_name,
            indicator_amount,
            --indicator_min_amount,        indicator_numberofmonths,        frequency,        calc_parameter_1,        calc_parameter_2,        islumpsum,        source,
            currency,
            unit,
            --                      , startdate        --                      , enddate
            percentage --old_contract_code
        from
            lt_result_lc
        union all
        select
            PC_ID,
            buextref,
            YM_START_END,
            YM_START,
            YM_END,
            DEAL_CREATION,
            DEAL_FINANCIALPROPOSALAGREED,
            DEAL_DATEVALIDATED,
            DEAL_SIGNATUREDATE,
            DEAL_LANDLORDSIGNATUREDATE,
            leasing_order_id,
            LO_ID,
            DEAL_GUID,
            DEAL_ID,
            BRAND_GUID,
            BRAND_ID,
            LESSEE_GUID,
            LESSEE_ID,
            quote_id,
            rental_unit_id,
            rental_unit_ref_id,
            rental_unit_common_id,
            Object_Type,
            indicator_name,
            indicator_amount * fx.rate as indicator_amount,
            --mn.indicator_min_amount * fx.rate as indicator_min_amount,        mn.indicator_numberofmonths,        mn.frequency,        mn.calc_parameter_1,       mn.calc_parameter_2,        mn.islumpsum,        mn.source,
            'GC' as currency,
            unit,
            --                      , startdate        --                      , enddate
            percentage --old_contract_code
        from
            lt_result_lc as mn
            inner join lt_fx_rate as fx on fx.curr_from = mn.currency
    )
--select * from lt_final_currency;--4,8M
    --on recuperre tous les lo des deal
,
    lt_deal_lo_date_key as (
        select
            link.deal_id,
            lo.leasing_order_id,
            lo.createdon,
            min(lo.createdon) over (partition by link.deal_id) as min_lo_crea_date
        from
            lt_crm_lo_dl_link as link
            inner join lt_crm_leasingorder as lo on lo.leasing_order_id = link.leasing_order_id
    )

--select * from lt_final_currency;
    ---on cherche le premier lo des deal
,
    lt_deal_main_lo as (
        select
            deal_id,
            min(leasing_order_id) as min_leasing_order_id
        from
            lt_deal_lo_date_key
        where
            createdon = min_lo_crea_date
        group by
            deal_id
    )
    --on rattache le lo au deal
,
    lt_fact_w_main_lo as (
        select
            ifnull(
                main_lo.min_leasing_order_id,
                mn.leasing_order_id
            ) as main_leasing_order,
            mn.*
        from
            lt_final_currency as mn
            left outer join lt_deal_main_lo as main_lo on mn.DEAL_GUID = main_lo.deal_id
    )
--select * from    lt_fact_w_main_lo;
--'7,3M'
    -------------------------------------------------------------deal à zero
    --on cherche le planit op code des lo sans deal
,
    lt_lo_wo_deal as (
        select
            distinct lo.leasing_order_id LO_GUID,
            lo.id AS LO_ID,
            lo.planitoperationcode
        from
            lt_crm_leasingorder lo
            left join lt_crm_lo_dl_link lk on lo.leasing_order_id = lk.leasing_order_id
        where
            lk.leasing_order_id is null --doit etre unique
    )
    --on rattache le planit operation code au deal/on change l'object type
,
    lt_fact_deal as (
        select
            mn.MAIN_LEASING_ORDER,
            mn.PC_ID,
            mn.buextref,
            nvl (mn.RENTAL_UNIT_REF_ID,mn.RENTAL_UNIT_ID) AS PRODUCT_EXT_REF,
            mn.YM_START_END,
            mn.YM_START,
            mn.YM_END,
            -- mn.DEAL_CREATION,
            -- mn.DEAL_FINANCIALPROPOSALAGREED,
            -- mn.DEAL_DATEVALIDATED,
            mn.DEAL_SIGNATUREDATE,
            -- mn.DEAL_LANDLORDSIGNATUREDATE,
            mn.LEASING_ORDER_ID AS LO_GUID,
            ifnull(mn.LO_ID, lo.LO_ID) AS LO_ID,
            ifnull(mn.deal_id, lo.planitoperationcode) as DEAL_ID,
            mn.DEAL_GUID,
            mn.BRAND_GUID,
            mn.BRAND_ID,
            mn.LESSEE_GUID,
            mn.LESSEE_ID,
            mn.QUOTE_ID,
            -- mn.RENTAL_UNIT_ID,
            -- mn.RENTAL_UNIT_REF_ID,
            -- mn.RENTAL_UNIT_COMMON_ID,
            mn.INDICATOR_NAME,
            mn.INDICATOR_AMOUNT,
            --mn.INDICATOR_MIN_AMOUNT,        mn.INDICATOR_NUMBEROFMONTHS,        mn.FREQUENCY,        mn.CALC_PARAMETER_1,        mn.CALC_PARAMETER_2,        mn.ISLUMPSUM,        mn.SOURCE,
            mn.CURRENCY,
            mn.UNIT,
            nvl(mn.PERCENTAGE::number, 0) as percentage,
            --mn.OLD_CONTRACT_CODE,
            case
                when mn.DEAL_GUID IS NULL then 'LO_DP' --cas des deal a 0
                when mn.DEAL_GUID IS not null then mn.OBJECT_TYPE
                else null
            end as FACT_TYPE,
            NULL AS CONTRACT_ID
        from
            lt_fact_w_main_lo mn
            left join lt_lo_wo_deal lo on mn.leasing_order_id = lo.LO_GUID
        UNION ALL
            -- SGE202506: vacancy planit du rent roll pour reporting *PBI
        SELECT
            NULL AS MAIN_LEASING_ORDER,
            PC_ID,
            bu_extref,
            PRODUCT_EXT_REF,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN year(CURRENT_DATE) || '01'  || '-' || year(CURRENT_DATE) || '12'
                ELSE year(DATEADD(year, 1, CURRENT_DATE)) || '01' || '-'  || year(DATEADD(year, 1, CURRENT_DATE)) || '01'
            END AS YM_START_END,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN '01/01/' || year(CURRENT_DATE)
                ELSE '01/01/' || year(DATEADD(year, 1, CURRENT_DATE))
            END AS YM_START,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN '31/12/' || year(CURRENT_DATE)
                ELSE '31/12/' || year(DATEADD(year, 1, CURRENT_DATE))
            END AS YM_END,
            -- NULL AS DEAL_CREATION,
            -- NULL AS DEAL_FINANCIALPROPOSALAGREED,
            -- NULL AS DEAL_DATEVALIDATED,
            NULL AS DEAL_SIGNATUREDATE,
            -- NULL AS DEAL_LANDLORDSIGNATUREDATE,
            NULL AS LO_GUID,
            NULL AS LO_ID,
            NULL AS DEAL_ID,
            NULL AS DEAL_GUID,
            NULL AS BRAND_GUID,
            NULL AS BRAND_ID,
            NULL AS LESSEE_GUID,
            NULL AS LESSEE_ID,
            NULL AS QUOTE_GUID,
            -- PRODUCT_EXT_REF AS RENTAL_UNIT_GUID,
            -- PRODUCT_EXT_REF AS RENTAL_UNIT_REF_ID,
            -- PRODUCT_EXT_REF AS RENTAL_UNIT_COMMON_ID,
            --        NULL AS OBJECT_TYPE,
            indicator_name AS INDICATOR_NAME,
            indicator_num AS INDICATOR_AMOUNT,
            --NULL AS INDICATOR_MIN_AMOUNT,        NULL AS INDICATOR_NUMBEROFMONTHS,        NULL AS FREQUENCY,        NULL AS CALC_PARAMETER_1,        NULL AS CALC_PARAMETER_2,        NULL AS ISLUMPSUM,        NULL AS SOURCE,
            CURRENCY,
            NULL AS UNIT,
            0 as percentage,
            --NULL AS OLD_CONTRACT_CODE,
            'CONTRACT_PRODUCT' AS FACT_TYPE,
            -- PRODUCT_EXT_REF,
            CONTRACT_ID
        FROM
            EXPOSE_DEV.DPERFMGT.V_DIM_RENT_ROLL_FOR_VACANCY dim
            INNER JOIN LT_PLANIT_OPERATION_CUBE_RENT_ROLL_FOR_VACANCY_W_CURRENCY_AGG kpi ON kpi.key_r = dim.key_r
        WHERE
            dim.status = 'Occupied'
        UNION ALL
            --SGE202506: vacancy panit du rent roll pour reporting PBI :
        SELECT
            NULL AS MAIN_LEASING_ORDER,
            PC_ID,
            bu_extref,
            PRODUCT_EXT_REF,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN year(CURRENT_DATE) || '01'  || '-' || year(CURRENT_DATE) || '12'
                ELSE year(DATEADD(year, 1, CURRENT_DATE)) || '01' || '-'  || year(DATEADD(year, 1, CURRENT_DATE)) || '01'
            END AS YM_START_END,
            CASE
                WHEN SCENARIOS_RR like 'CLOSING4_%' THEN '01/01/' || year(CURRENT_DATE)
                ELSE '01/01/' || year(DATEADD(year, 1, CURRENT_DATE))
            END AS YM_START,
            CASE
                WHEN SCENARIOS_RR like 'CLOSING4_%' THEN '31/12/' || year(CURRENT_DATE)
                ELSE '31/12/' || year(DATEADD(year, 1, CURRENT_DATE))
            END AS YM_END,
            -- NULL AS DEAL_CREATION,
            -- NULL AS DEAL_FINANCIALPROPOSALAGREED,
            -- NULL AS DEAL_DATEVALIDATED,
            NULL AS DEAL_SIGNATUREDATE,
            -- NULL AS DEAL_LANDLORDSIGNATUREDATE,
            NULL AS LO_GUID,
            NULL AS LO_ID,
            NULL AS DEAL_ID,
            NULL AS DEAL_GUID,
            NULL AS BRAND_GUID,
            NULL AS BRAND_ID,
            NULL AS LESSEE_GUID,
            NULL AS LESSEE_ID,
            NULL AS QUOTE_GUID,
            -- PRODUCT_EXT_REF AS RENTAL_UNIT_GUID,
            -- PRODUCT_EXT_REF AS RENTAL_UNIT_REF_ID,
            -- PRODUCT_EXT_REF AS RENTAL_UNIT_COMMON_ID,
            --        NULL AS OBJECT_TYPE,
            indicator_name AS INDICATOR_NAME,
            indicator_num AS INDICATOR_AMOUNT,
            --NULL AS INDICATOR_MIN_AMOUNT,        NULL AS INDICATOR_NUMBEROFMONTHS,        NULL AS FREQUENCY,        NULL AS CALC_PARAMETER_1,        NULL AS CALC_PARAMETER_2,        NULL AS ISLUMPSUM,        NULL AS SOURCE,
            CURRENCY,
            NULL AS UNIT,
            0 as percentage,
            --NULL AS OLD_CONTRACT_CODE,
            'PRODUCT_PRODUCT' AS FACT_TYPE,
            CONTRACT_ID
        FROM
            EXPOSE_DEV.DPERFMGT.V_DIM_RENT_ROLL_FOR_VACANCY dim
            INNER JOIN LT_PLANIT_OPERATION_CUBE_RENT_ROLL_FOR_VACANCY_W_CURRENCY_AGG kpi ON kpi.key_r = dim.key_r
    )
    --select * from lt_fact_deal;--5,8M
    ----SGE 2025 on s'occupe des expiry vacancy
,
    lt_expvac as (
        select
            product_ext_ref,
            occupancystartdate,
            status,
            ym_start,
            ym_end --max(occupancystartdate)
        from
            EXPOSE_DEV.DPERFMGT.V_DIM_RENT_ROLL_FOR_VACANCY
    ),
    lt_expvac_max as(
        select
            product_ext_ref,
            max(status) status,
            ym_start,
            ym_end
        from
            lt_expvac
        group by
            product_ext_ref,
            ym_start,
            ym_end
    ),
    lt_full_w_expvac as (
        select
            tf.MAIN_LEASING_ORDER,
            tf.PC_ID,
            nvl(tf.product_ext_ref,s.product_ext_ref) as PRODUCT_EXT_REF,
            tf.buextref,
            tf.YM_START_END as YM_START_END,
            nvl(tf.ym_start,s.ym_start) as ym_start,
            nvl(tf.ym_end,s.ym_end) as ym_end,
            -- tf.DEAL_CREATION,
            -- tf.DEAL_FINANCIALPROPOSALAGREED,
            -- tf.DEAL_DATEVALIDATED,
            -- tf.DEAL_SIGNATUREDATE,
            -- tf.DEAL_LANDLORDSIGNATUREDATE,
            tf.LO_GUID,
            tf.LO_ID,
            tf.DEAL_ID,
            tf.DEAL_GUID,
            tf.BRAND_GUID,
            tf.BRAND_ID,
            tf.LESSEE_GUID,
            tf.LESSEE_ID,
            tf.QUOTE_ID,
            -- tf.RENTAL_UNIT_ID,
            -- tf.RENTAL_UNIT_REF_ID,
            -- tf.RENTAL_UNIT_COMMON_ID,
            --        tf.OBJECT_TYPE,
            tf.INDICATOR_NAME,
            tf.INDICATOR_AMOUNT,
            tf.CURRENCY,
            tf.UNIT,
            tf.PERCENTAGE,
            tf.FACT_TYPE,
            tf.CONTRACT_ID,
            case
                when s.status = 'OCCUPIED' then 'EXPIRY'
                else 'VACANT'
            end as EXPIRY_VACANCY_TYPE,
            left(s.occupancystartdate, 6) as YM_EXPIRY_VACANCY
        from
            lt_fact_deal tf
            left join lt_expvac s on s.product_ext_ref = tf.product_ext_ref
            and to_char(tf.DEAL_SIGNATUREDATE, 'YYYYMM') between s.YM_START
            and s.YM_END
            left join lt_expvac_max sm on sm.product_ext_ref = s.product_ext_ref
            and sm.status = s.status
            and sm.ym_start = s.ym_start
    )
select * from lt_full_w_expvac;
--8,3M