{{ config(materialized='view') }}


with

lt_planit_operation_cube_raw_data_leasing_w_currency_agg as  (select * from  expose.dperfmgt.planit_operation_cube_raw_data_leasing_w_currency_agg),

lt_planit_operation_dim_raw_data_leasing_agg as (select * from  expose.dperfmgt.planit_operation_dim_raw_data_leasing_agg),

lt_PLAN_IT_RDL_AMOUNT           as (select key_r
                                         , sum(case when indicator_name = 'YEARLY_SBR' then indicator_num else 0 end) as YEARLY_SBR
                                         , sum(case when indicator_name = 'BCOM'       then indicator_num else 0 end) as BCOM
                                    from lt_planit_operation_cube_raw_data_leasing_w_currency_agg
                                    where indicator_name in ('YEARLY_SBR','BCOM') 
                                      and currency = 'LC'
                                    group by key_r ),
lt_result as (
    select     mn.KEY_R
            ,  mn.cscenario || ' - ' || mn.cquarter || ' - ' || mn.cversion as KEY_FILTER
            , mn.cscenario, mn.cversion, mn.cquarter
            , mn.coperat
            , ifnull(link.deal_id,mn.coperat) as operation_w_pulse_id
            , mn.csrcsys
            
            , decode(mn.qfr_deal,'Yes', TRUE,FALSE) as qfr_deal
            , mn.perf_cat 
            
            --, row_number() over ( partition by cscenario, cversion, operation_w_pulse_id order by mn.cquarter desc, mn.csrcsys asc, qfr_deal desc , PERF_CAT desc) as _order
            , row_number() over ( partition by scenario_mapp.PULSE_SCENARIO, operation_w_pulse_id 
                                      order by scenario_mapp.pulse_year desc, scenario_mapp.pulse_quarter desc
                                             , mn.csrcsys asc
                                             , qfr_deal   desc 
                                             , PERF_CAT   desc) as _order

            , count(*) over ( partition by scenario_mapp.PULSE_SCENARIO, operation_w_pulse_id ) as _count
            , mn.CSDEADLINE
            , mn.DISC_PREC_COND
            , mn.FLGSHIP_REG
            , mn.CSHEAVY
            , mn.CNEWCONCP
            , mn.PERIMETER
            , mn.CS
            , mn.PROJECT_CODE
            , mn.SC_CATEGORY
            , '' as CSector
            , mn.SHOPPING_CENTER_AFFILIATES
            , mn.CSTYPE
            , scenario_mapp.PULSE_SCENARIO --'SIGNED_DEALS'
            , scenario_mapp.pulse_year
            , scenario_mapp.pulse_quarter
            
         
    from lt_planit_operation_dim_raw_data_leasing_agg as mn
    left outer join DPULSE.PLAN_IT_LINK_OPERATION_DEAL                                                  as link         on link.coperat = mn.coperat
    left outer join expose.DPERFMGT.PLANIT_TRANSVERSE_TABLE_REPORT_KEY_WITH_PULSE_QUARTER_DISTRIBUTION  as scenario_mapp
            on  scenario_mapp.service               = 'RAW_DATA_LEASING' 
            --and scenario_mapp.PULSE_SCENARIO        = 'SIGNED_DEALS'
            and scenario_mapp.scenario_extracted    =  mn.cscenario
            and scenario_mapp.version_extracted     =  mn.cversion 
            and scenario_mapp.pulse_year || '-' || scenario_mapp.pulse_quarter = mn.cquarter
   --where mn.qfr_deal = 'Yes'
    --where cscenario in ('WORKING_SCENARIO','ACTUAL') and cversion  = 'V_REPORTING'
    
) 

select mn.*, ifnull(amnt.YEARLY_SBR,0) as YEARLY_SBR
       , ifnull(amnt.BCOM,0)        as BCOM
from lt_result    as mn
left outer join lt_PLAN_IT_RDL_AMOUNT as amnt on amnt.key_r = mn.key_r;
;
