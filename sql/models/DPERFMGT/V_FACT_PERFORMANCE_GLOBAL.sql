--create or replace table DPERFMGT.T_FACT_PERFORMANCE_GLOBAL --2min40;
--create or replace table EXPOSE.DPERFMGT.T_FACT_LO_DEAL_PERFORMANCE as select * from  DPERFMGT.V_FACT_LO_DEAL_PERFORMANCE; --40secondes
--create or replace table EXPOSE.DPULSE.T_CRM_RENTALUNIT_W_KEY as select * from  DPULSE.CRM_RENTALUNIT_W_KEY; --1min20
;
SELECT * FROM DPERFMGT.T_FACT_PERFORMANCE_GLOBAL --WHERE  deal_guid='5934698f-ee82-4a83-8120-55c22b03a67c'
;
SELECT * FROM expose.dpulse.t_fact_lo_deal_rental_unit;
--SELECT * FROM DPERFMGT.T_FACT_LO_DEAL_PERFORMANCE WHERE  deal_id='5934698f-ee82-4a83-8120-55c22b03a67c'
--;
create or replace view DPERFMGT.V_FACT_PERFORMANCE_GLOBAL --2min40
COMMENT = 'GENONI-HOMO: Created for external vizualisation 2025' as --deal metrics

WITH lt_deal_and_lo_pivot as (
    select
        /*  d.created_on as DEAL_CREATION,
            d.datefinancialproposalagreed DEAL_FINANCIALPROPOSALAGREED,
            d.datevalidated DEAL_DATEVALIDATED,
            d.signaturedate DEAL_SIGNATUREDATE,
            d.datelegalagreement DEAL_LANDLORDSIGNATUREDATE,
            d.signature_date_rpt deal_SIGNATURE_DATE_RPT,*/
        f.*
    from
         -- DPERFMGT.T_FACT_LO_DEAL_PERFORMANCE f--attention penser a rafraichir la table !!!!
        DPERFMGT.V_FACT_LO_DEAL_PERFORMANCE f
),
lt_crm_rentalunit_w_key as (
    select
        *
    from
        DPULSE.CRM_RENTALUNIT_W_KEY 
        -- DPULSE.T_CRM_RENTALUNIT_W_KEY --attention penser a rafraichir la table !!!!
),
lt_dim_calendar as (
    select
        *
    from
        DPERFMGT.dim_calendar_global
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
        and ltt_bu.typecode = '809020000' --bu
        and ltt_pc.typecode = '809020002' --pc
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
---enrichissement desdeals
LT_CRM_DEAL as (
    SELECT
        *
    FROM
        DPULSE.CRM_DEAL
)
---branch lessee
,
LT_CRM_ACCOUNT as (
    SELECT
        *
    FROM
        FINOPS.DWH.CRM_ACCOUNT
),
LT_DEAL_ACCOUNT AS (
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
        D.id deal_id,
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
        dqr.FACT_PROG as percentage,
    from
        lt_crm_rentalunit_w_key as ru ---materialisation de la table temporaire pour les test
        JOIN lt_crm_deal_quote_relevant dqr on dqr.QUOTEID = ru.quote_id --on elimine tout de suite les quote inutiles cree Produit cartesien
        LEFT JOIN LT_TERRITORY LTT ON LTT.MDMID_BU = REPLACE(ru.BUEXTREF, '-', '/')
        LEFT JOIN LT_DEAL_ACCOUNT DL ON DL.DEAL_GUID = ru.deal_id
        LEFT JOIN LT_CRM_DEAL D ON D.deal_id = ru.deal_id
        LEFT JOIN lt_crm_leasingorder lo ON lo.leasing_order_id = ru.leasing_order_id -- )    select * from lt_rental_unit;
        /*left outer join dpulse.DIM_RENTALUNIT_COMMON_REFERENTIAL      as ref
                                                    on     ( (ru.rental_unit_id             =  ref.rental_unit_id and ref.new_rentalunit = ''Yes')
                                                        or   (ru.RENTAL_UNIT_REF_ID             =  ref.RENTAL_UNIT_REF_ID and ref.new_rentalunit = ''No'))*/
        --where   ru.quote_id= '726b512c-d8c5-ee11-9079-000d3ab00f0f'
        --where 1=0
    union all
    select
        LTT.MDMID_PC AS PC_ID,
        ru.BUEXTREF,
        ru.leasing_order_id,
        lo.id AS LO_ID,
        ru.deal_id DEAL_GUID,
        D.id deal_id,
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
        null YM_START,
        null YM_END,
        null percentage,
    from
        lt_crm_rentalunit_w_key as ru ---materialisation de la table temporaire pour les test
        --on elimine tout de suite les quote inutiles cree Produit cartesien
        LEFT JOIN LT_TERRITORY LTT ON LTT.MDMID_BU = REPLACE(ru.BUEXTREF, '-', '/')
        LEFT JOIN LT_DEAL_ACCOUNT DL ON DL.DEAL_GUID = ru.deal_id
        LEFT JOIN LT_CRM_DEAL D ON D.deal_id = ru.deal_id
        LEFT JOIN lt_crm_leasingorder lo ON lo.leasing_order_id = ru.leasing_order_id
    where
        ru.source = 'LEASING_ORDER'
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
        source,
        ru.YM_START,
        ru.YM_END,
        ru.percentage,
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
        ) -- join lt_crm_deal_quote_relevant dqr on dqr.QUOTEID = ru.quote_id
)
--select * from lt_rental_unit_pivot; --1,3M
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
        fact.deal_SIGNATURE_DATE_RPT,
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
        ru.old_contract_code,
        fact.startdate
    from
        lt_deal_and_lo_pivot as fact
        JOIN lt_crm_deal_quote_relevant dqr ON fact.quote_id = dqr.QUOTEID --on vire les not relevant par inner join
        LEFT JOIN lt_crm_leasingorder lo ON lo.leasing_order_id = fact.leasing_order_id
        left join lt_rental_unit as ru on ru.leasing_order_id = fact.leasing_order_id
        and ru.deal_guid = dqr.opportunityid
        and ru.quote_id = dqr.QUOTEID
        and ru.percentage = dqr.FACT_PROG 
    union all
    select
        ru.PC_ID,
        ru.buextref,
        null        YM_START_END,
        null        YM_START,
        null        YM_END,
        fact.DEAL_CREATION,
        fact.DEAL_FINANCIALPROPOSALAGREED,
        fact.DEAL_DATEVALIDATED,
        fact.DEAL_SIGNATUREDATE,
        fact.DEAL_LANDLORDSIGNATUREDATE,
        fact.deal_SIGNATURE_DATE_RPT,
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
        0 as percentage,
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
        ru.old_contract_code,
        fact.startdate
    from
        lt_deal_and_lo_pivot as fact --  JOIN lt_crm_deal_quote_relevant dqr ON fact.quote_id = dqr.QUOTEID  or  -- on recup les LO sans DEAL --on vire les not relevant par inner join
        LEFT JOIN lt_crm_leasingorder lo ON lo.leasing_order_id = fact.leasing_order_id
        left join lt_rental_unit as ru on ru.leasing_order_id = fact.leasing_order_id --  and ru.deal_guid = fact.deal_id
        --   and ru.quote_id = fact.QUOTE_ID
        --and ru.percentage= fact.FACT_PROG
    where
        --fact.RU_USAGE_CALC <> 'None'
        -- and
        fact.object_type = 'LO' -- LO SANS DEAL
        and ru.deal_ID is null
)
--select * from  lt_deal_lo_fact;--1,4M
---on alloue les montants en  fonction de la gla
,
lt_dl_lo_deag_comp as (
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
        deal_SIGNATURE_DATE_RPT,
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
        /*cas des date*/
            /*else 0*/ --aho 20250906
            else indicator_amount
        end as indicator_amount,
        case
            when RU_USAGE_CALC = 'None' then ''
            else currency
        end as currency,
        '' as unit,
        indicator_amount as before_calc_indicator_amount,
        allocation_gla,
        allocation_gla_usage,
        ru_usage_calc,
        startdate --                      , enddate
,
        percentage,
        old_contract_code
    from
        lt_deal_lo_fact
)
-- select * from lt_dl_lo_deag_comp; --2,7M
--premiere aggregation des faits deal avec rental unit
,
lt_fact_consolidation_flat as (
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
        deal_SIGNATURE_DATE_RPT,
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
        startdate,
        -- enddate,
        percentage
    from
        lt_dl_lo_deag_comp --where         abs(indicator_amount) + abs(indicator_min_amount) + abs(indicator_numberofmonths) <> 0
    union all
    select
        PC_ID,
        buextref,
        YM_START || '-' || YM_END as YM_START_END,
        --on met a vide car on n'a pas les infos deal au niveau rental unit
        YM_START,
        YM_END,
        null as DEAL_CREATION,
        null as DEAL_FINANCIALPROPOSALAGREED,
        null as DEAL_DATEVALIDATED,
        null DEAL_SIGNATUREDATE,
        null as DEAL_LANDLORDSIGNATUREDATE,
        null deal_SIGNATURE_DATE_RPT,
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
        null as startdate,
        -- null as enddate,
        percentage --        0 as indicator_min_amount,        0 as indicator_numberofmonths,        null as frequency,
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
) --select * from lt_fact_consolidation_flat;--4M
--ajout des slots de currency
,
lt_result_lc as (
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
        deal_SIGNATURE_DATE_RPT,
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
        ifnull(mn.currency, '') as currency,
        ifnull(mn.unit, '') as unit --,
        ,        startdate
        ,        mn.percentage
    from
        lt_fact_consolidation_flat as mn --where indicator_amount <> 0
)
-- select    * from    lt_result_lc;
--6,2M
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
    ) --select * from lt_fx_rate;
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
            deal_SIGNATURE_DATE_RPT,
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
            startdate,
            percentage 
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
            deal_SIGNATURE_DATE_RPT,
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
            'GC' as currency,
            unit,
            startdate,
            percentage
        from
            lt_result_lc as mn
            inner join lt_fx_rate as fx on fx.curr_from = mn.currency
    ) --on recuperre tous les lo des deal
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
    ) --select * from lt_final_currency;
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
    -- select * from    lt_fact_w_main_lo;
    --'7,3M'
    -------------------------------------------------------------deal à zero
    --on cherche le planit op code des lo sans deal
,
    lt_lo_wo_deal as (
        select
            distinct lo.leasing_order_id LO_GUID,
            lo.id AS LO_ID,
            lo.planitoperationcode,
            -- lo.CREATEDON,
            --Deal.CREATEDON,
            case
                when lo.planitoperationcode like 'BP%' then right(left(lo.planitoperationcode, 6), 4) || '0101'
                when lo.planitoperationcode like 'FQ1%' then right(left(lo.planitoperationcode, 7), 4) || '0101'
                when lo.planitoperationcode like 'FQ2%' then right(left(lo.planitoperationcode, 7), 4) || '0401'
                when lo.planitoperationcode like 'FQ3%' then right(left(lo.planitoperationcode, 7), 4) || '0701'
                else null
            end LO_START_YM,
            min(to_char(Deal.CREATEDON, 'YYYYMMDD')) LO_END_YM,
        from
            lt_crm_leasingorder lo
            left join LT_CRM_DEAL Deal on deal.leasing_order_id = lo.leasing_order_id -- left join lt_crm_lo_dl_link lk on lo.leasing_order_id = lk.leasing_order_id
        where
            -- Deal.leasing_order_id is  null --doit etre unique
            -- and
            lo.statuscode = 1 -- LO ACTIF
            and (
                lo.PLANITOPERATIONCODE like 'BP%'
                or lo.PLANITOPERATIONCODE like 'FQ%'
            )
        group by
            lo.leasing_order_id,
            lo.id,
            lo.planitoperationcode,
            --Deal.CREATEDON,
            LO_START_YM
        having
            ifnull(LO_END_YM, '999912') > LO_START_YM
    ) --select * from lt_lo_wo_deal where 'LO-00035389'
    --select * from lt_fact_w_main_lo ;
    --on rattache le planit operation code au deal/on change l'object type
,
    lt_fact_deal as (
        select
            -- DEAL PRODUCT
            --null MAIN_LEASING_ORDER,
            --1 AS COL,
            mn.PC_ID,
            mn.buextref,
            RENTAL_UNIT_common_id AS PRODUCT_EXT_REF,
            mn.YM_START_END,
            mn.YM_START,
            mn.YM_END,
            -- mn.DEAL_CREATION,
            -- mn.DEAL_FINANCIALPROPOSALAGREED,
            -- mn.DEAL_DATEVALIDATED,
            max(mn.DEAL_SIGNATUREDATE) over (partition by mn.deal_id) DEAL_SIGNATUREDATE,
            max(mn.deal_SIGNATURE_DATE_RPT) over (partition by mn.deal_id) deal_SIGNATURE_DATE_RPT,
            -- mn.DEAL_LANDLORDSIGNATUREDATE,
            /*null AHO 20250906 c'etait à null pourquoi ? */
            mn.LEASING_ORDER_ID
            AS LO_GUID,
            /*null AHO 20250906 c'etait à null pourquoi ? */
            mn.LO_ID 
            AS LO_ID,
            mn.deal_id as DEAL_ID,
            mn.DEAL_GUID,
            mn.BRAND_GUID,
            mn.BRAND_ID,
            mn.LESSEE_GUID,
            mn.LESSEE_ID,
            mn.QUOTE_ID,
            mn.INDICATOR_NAME,
            mn.INDICATOR_AMOUNT,
            mn.CURRENCY,
            mn.UNIT,
            mn.PERCENTAGE::number as percentage,
            --mn.OLD_CONTRACT_CODE,
            'DEAL_PRODUCT' as FACT_TYPE,
            NULL AS CONTRACT_ID,
            mn.startdate as INDICATOR_DATE
        from
            lt_fact_w_main_lo mn --  left join lt_lo_wo_deal lo on mn.leasing_order_id = lo.LO_GUID
        where
            mn.Object_Type = 'DL' -- que les dEAL
            and --  (mn.DEAL_GUID is not null or
            length(mn.DEAL_GUID) > 1 --lo.LO_GUID is null
            --   left join lt_lo_wo_deal lo on mn.leasing_order_id = lo.LO_GUID
        UNION ALL
            -- ajoutet des LO au format deal a 0%
        select
            --2 AS COL,
            --null MAIN_LEASING_ORDER,
            mn.PC_ID,
            mn.buextref,
            RENTAL_UNIT_common_id AS PRODUCT_EXT_REF,
            lo.LO_START_YM || '-' || ifnull(lo.LO_END_YM, '99991231') YM_START_END,
            lo.LO_START_YM YM_START,
            ifnull(lo.LO_END_YM, '99991231') YM_END,
            -- mn.DEAL_CREATION,
            -- mn.DEAL_FINANCIALPROPOSALAGREED,
            -- mn.DEAL_DATEVALIDATED,
            mn.DEAL_SIGNATUREDATE,
            mn.deal_SIGNATURE_DATE_RPT,
            -- mn.DEAL_LANDLORDSIGNATUREDATE,
            mn.LEASING_ORDER_ID
            /*null*/
            AS LO_GUID,
            --je remappe car sinon pas de lien avec dim deal aho 20250902
            lo.LO_ID
            /*null*/
            AS LO_ID,
            --je remappe car sinon pas de lien avec dim deal aho 20250902
            lo.planitoperationcode as DEAL_ID,
            lo.planitoperationcode as DEAL_GUID,
            --je remplace LO_ID par le planitoperation code pour lien avec DIM_DEAL 20250902)
            mn.BRAND_GUID,
            mn.BRAND_ID,
            mn.LESSEE_GUID,
            mn.LESSEE_ID,
            null QUOTE_ID,
            -- mn.RENTAL_UNIT_ID,
            -- mn.RENTAL_UNIT_REF_ID,
            -- mn.RENTAL_UNIT_COMMON_ID,
            mn.INDICATOR_NAME,
            mn.INDICATOR_AMOUNT,
            --mn.INDICATOR_MIN_AMOUNT,        mn.INDICATOR_NUMBEROFMONTHS,        mn.FREQUENCY,        mn.CALC_PARAMETER_1,        mn.CALC_PARAMETER_2,        mn.ISLUMPSUM,        mn.SOURCE,
            mn.CURRENCY,
            mn.UNIT,
            0 as percentage,
            --mn.OLD_CONTRACT_CODE,
            'DEAL_PRODUCT' FACT_TYPE,
            NULL AS CONTRACT_ID,
            mn.startdate as INDICATOR_DATE
        from
            lt_fact_w_main_lo mn
            join lt_lo_wo_deal lo on mn.leasing_order_id = lo.LO_GUID
        where
            mn.Object_Type = 'LO' -- que les dEAL
        union all
        select
            -- LISTE les leasing order product
            -- null MAIN_LEASING_ORDER,
            --3 AS COL,
            mn.PC_ID,
            mn.buextref,
            RENTAL_UNIT_common_id AS PRODUCT_EXT_REF,
            case
                when lo.planitoperationcode like 'BP%' then right(left(lo.planitoperationcode, 6), 4) || '0101-' || right(left(lo.planitoperationcode, 6), 4) || '1231'
                when lo.planitoperationcode like 'FQ1%' then right(left(lo.planitoperationcode, 7), 4) || '0101-' || right(left(lo.planitoperationcode, 6), 4) || '0331'
                when lo.planitoperationcode like 'FQ2%' then right(left(lo.planitoperationcode, 7), 4) || '0401-' || right(left(lo.planitoperationcode, 6), 4) || '0630'
                when lo.planitoperationcode like 'FQ3%' then right(left(lo.planitoperationcode, 7), 4) || '0701-' || right(left(lo.planitoperationcode, 6), 4) || '0930'
                else to_char(CURRENT_DATE, 'YYYY') || '0101-99991231'
            end YM_START_END,
            case
                when lo.planitoperationcode like 'BP%' then right(left(lo.planitoperationcode, 6), 4) || '0101'
                when lo.planitoperationcode like 'FQ1%' then right(left(lo.planitoperationcode, 7), 4) || '0101'
                when lo.planitoperationcode like 'FQ2%' then right(left(lo.planitoperationcode, 7), 4) || '0401'
                when lo.planitoperationcode like 'FQ3%' then right(left(lo.planitoperationcode, 7), 4) || '0701'
                else to_char(CURRENT_DATE, 'YYYY') || '0101'
            end YM_START,
            case
                when lo.planitoperationcode like 'BP%' then right(left(lo.planitoperationcode, 6), 4) || '1231'
                when lo.planitoperationcode like 'FQ1%' then right(left(lo.planitoperationcode, 7), 4) || '0331'
                when lo.planitoperationcode like 'FQ2%' then right(left(lo.planitoperationcode, 7), 4) || '0630'
                when lo.planitoperationcode like 'FQ3%' then right(left(lo.planitoperationcode, 7), 4) || '0930'
                else '99991231'
            end YM_END,
            -- mn.DEAL_CREATION,
            -- mn.DEAL_FINANCIALPROPOSALAGREED,
            -- mn.DEAL_DATEVALIDATED,
            mn.DEAL_SIGNATUREDATE,
            mn.deal_SIGNATURE_DATE_RPT,
            -- mn.DEAL_LANDLORDSIGNATUREDATE,
            mn.LEASING_ORDER_ID AS LO_GUID,
            mn.LO_ID AS LO_ID,
            null as DEAL_ID,
            null DEAL_GUID,
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
            null as percentage,
            --mn.OLD_CONTRACT_CODE,
            'LO_PRODUCT' FACT_TYPE,
            NULL AS CONTRACT_ID,
            mn.startdate as INDICATOR_DATE
        from
            lt_fact_w_main_lo mn
            join lt_crm_leasingorder lo on mn.leasing_order_id = lo.leasing_order_id
        where
            mn.Object_Type = 'LO' -- tout les LO
        union all
            -- SGE202506: vacancy planit du rent roll pour reporting *PBI
        SELECT
            -- LISTE les contrat product Planit
            -- NULL AS MAIN_LEASING_ORDER,
            --4 AS COL,
            PC_ID,
            bu_extref,
            PRODUCT_EXT_REF,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN year(CURRENT_DATE) || '0101' || '-' || year(CURRENT_DATE) || '1231'
                ELSE year(DATEADD(year, 1, CURRENT_DATE)) || '0101' || '-' || year(DATEADD(year, 1, CURRENT_DATE)) || '0131'
            END AS YM_START_END,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN year(CURRENT_DATE) || '0101'
                ELSE year(DATEADD(year, 1, CURRENT_DATE)) || '0101'
            END AS YM_START,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN year(CURRENT_DATE) || '1231'
                ELSE year(DATEADD(year, 1, CURRENT_DATE)) || '1231'
            END AS YM_END,
            -- NULL AS DEAL_CREATION,
            -- NULL AS DEAL_FINANCIALPROPOSALAGREED,
            -- NULL AS DEAL_DATEVALIDATED,
            NULL AS DEAL_SIGNATUREDATE,
            -- NULL AS DEAL_LANDLORDSIGNATUREDATE,
            null as deal_SIGNATURE_DATE_RPT,
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
            null as percentage,
            --NULL AS OLD_CONTRACT_CODE,
            'CONTRACT_PRODUCT' AS FACT_TYPE,
            -- PRODUCT_EXT_REF,
            CONTRACT_ID,
            null INDICATOR_DATE
        FROM
            EXPOSE.DPERFMGT.V_DIM_RENT_ROLL_FOR_VACANCY dim
            INNER JOIN LT_PLANIT_OPERATION_CUBE_RENT_ROLL_FOR_VACANCY_W_CURRENCY_AGG kpi ON kpi.key_r = dim.key_r
        WHERE
            dim.status = 'Occupied' --    and 1=0
        UNION ALL
            --SGE202506: vacancy panit du rent roll pour reporting PBI :
        SELECT
            -- LISTE les product Planit not finished
            -- NULL AS MAIN_LEASING_ORDER,
            --5 AS COL,
            PC_ID,
            bu_extref,
            PRODUCT_EXT_REF,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN year(CURRENT_DATE) || '0101' || '-' || year(CURRENT_DATE) || '1231'
                ELSE year(DATEADD(year, 1, CURRENT_DATE)) || '0101' || '-' || year(DATEADD(year, 1, CURRENT_DATE)) || '0131'
            END AS YM_START_END,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN year(CURRENT_DATE) || '0101'
                ELSE year(DATEADD(year, 1, CURRENT_DATE)) || '0101'
            END AS YM_START,
            CASE
                WHEN SCENARIOS_RR LIKE 'CLOSING4_%' THEN year(CURRENT_DATE) || '1231'
                ELSE year(DATEADD(year, 1, CURRENT_DATE)) || '1231'
            END AS YM_END,
            -- NULL AS DEAL_CREATION,
            -- NULL AS DEAL_FINANCIALPROPOSALAGREED,
            -- NULL AS DEAL_DATEVALIDATED,
            NULL AS DEAL_SIGNATUREDATE,
            -- NULL AS DEAL_LANDLORDSIGNATUREDATE,
            null as deal_SIGNATURE_DATE_RPT,
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
            null as percentage,
            --NULL AS OLD_CONTRACT_CODE,
            'PRODUCT' AS FACT_TYPE,
            CONTRACT_ID,
            null as INDICATOR_DATE
        FROM
            EXPOSE.DPERFMGT.V_DIM_RENT_ROLL_FOR_VACANCY dim
            INNER JOIN LT_PLANIT_OPERATION_CUBE_RENT_ROLL_FOR_VACANCY_W_CURRENCY_AGG kpi ON kpi.key_r = dim.key_r -- where  1=0
    )
    --select * from lt_fact_deal where lo_id is not null and length(deal_id)<2 ;--5,8M
    ----SGE 2025 on s'occupe des expiry vacancy
,
    lt_expvac as (
        select
            product_ext_ref,
            occupancystartdate,
            occupancyenddate,
            TERMDATE,
            status,
            ym_start,
            ym_end --max(occupancystartdate)
        from
            EXPOSE.DPERFMGT.V_DIM_RENT_ROLL_FOR_VACANCY
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
            --tf.MAIN_LEASING_ORDER,
            tf.PC_ID,
            tf.product_ext_ref as PRODUCT_EXT_REF,
            tf.buextref,
            tf.YM_START_END as YM_START_END,
            tf.ym_start as ym_start,
            tf.ym_end as ym_end,
            -- tf.DEAL_CREATION,
            -- tf.DEAL_FINANCIALPROPOSALAGREED,
            -- tf.DEAL_DATEVALIDATED,
            case
                when tf.DEAL_SIGNATUREDATE is null then tf.deal_SIGNATURE_DATE_RPT
                else tf.DEAL_SIGNATUREDATE
            end DEAL_SIGNATUREDATE,
            case
                when tf.DEAL_SIGNATUREDATE is not null then 'Reel'
                else 'Estimated'
            end as SIGNATUREDATE_TYPE,
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
            max(
                case
                    when s.status = 'Occupied'
                    and left(s.TERMDATE, 4) = left(s.ym_end, 4) then 'EXPIRY' -- tu es expiry si c'est sur la meme année
                    when s.status = 'Occupied' then 'PROACTIVE' -- tu es expiry si c'est sur la meme année
                    else 'VACANT'
                end
            ) over (partition by tf.DEAL_ID) as EXPIRY_VACANCY_TYPE,
            max(
                case
                    when s.status = 'Occupied' then left(s.TERMDATE, 6)
                    else left(s.occupancystartdate, 6)
                end
            ) over (partition by tf.DEAL_ID) as YM_EXPIRY_VACANCY,
            tf.INDICATOR_DATE
        from
            lt_fact_deal tf
            left join lt_expvac s on s.product_ext_ref = tf.product_ext_ref --and
            --case when tf.DEAL_SIGNATUREDATE is null then tf.deal_SIGNATURE_DATE_RPT else tf.DEAL_SIGNATUREDATE end between to_date(s.YM_START||'01','YYYYMMDD') and LAST_DAY(to_date(s.YM_END || '01','YYYYMMDD'),year)
            -- and left(TO_CHAR(tf.deal_signaturedate,'YYYYMM'), 6) between s.YM_START and s.YM_END
            left join lt_expvac_max sm on sm.product_ext_ref = s.product_ext_ref
            and sm.status = s.status
            and sm.ym_start = s.ym_start
    )
    --select * from lt_full_w_expvac
    ---flag active
,
    lt_percentage as (
        select
            distinct percentage,
            deal_guid --,ym_start
        from
            lt_full_w_expvac
    ),
    lt_active as (
        select
            max(percentage) as maxpct,
            deal_guid,
            --ym_start
        from
            lt_percentage s --join lt_dim_calendar t on t.cal_id between s.ym_start and s.ym_end
        group by
            deal_guid --,ym_start
    ),
    lt_flag_active as (
        select
            case
                when a.maxpct is not null then 'Active'
                else null
            end as Active,
            fact.*
        from
            lt_full_w_expvac fact
            left join lt_active a on a.deal_guid = fact.deal_guid
            and a.maxpct = fact.percentage
    )
select
    *
from
    lt_flag_active;