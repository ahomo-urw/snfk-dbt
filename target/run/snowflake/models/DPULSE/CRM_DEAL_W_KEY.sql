
  create or replace   view EXPOSE_DEV.DPULSE.CRM_DEAL_W_KEY
  
   as (
    
with lt_crm_deal as (
    select
        *
    from
        DPULSE.CRM_DEAL --where deal_id='dc477580-d5c5-ee11-9079-000d3ab4bcd9'
),
lt_crm_quote as (
    select
        *
    from
        DPULSE.crm_quote  --where quote_id= '726b512c-d8c5-ee11-9079-000d3ab00f0f'
),
lt_crm_transactioncurrency as (
    select
        *
    from
        DPULSE.crm_transactioncurrency
),
lt_crm_territory as (
    select
        *
    from
        DPULSE.crm_territory
       -- where mdmid='FR-SC-150'--reduire le scope pour le dev a retirer
),
lt_crm_account as (
    select
        *
    from
        DPULSE.crm_account
),
lt_crm_order as (
    select
        *
    from
        DPULSE.crm_order
),
lt_PLAN_IT_RDL_ATTRIBUTES as (
    select
        *
    from
        DPULSE.PLAN_IT_RDL_ATTRIBUTES
    where
        PULSE_SCENARIO = 'SIGNED_DEALS' 
        and _order=1
),

lt_CRM_MASTER_DATA_GENERAL_FREQUENCY as (
    select
        *
    from
        EXPOSE.DPULSE.CRM_MASTER_DATA_GENERAL_FREQUENCY
)

select scope
     , dl.leasing_order_id
     , NULLIF(dl.deal_id , NULL) AS DEAL_ID-----OK
     , dl.id as deal_no
     , qu.name                                        as quote_name
     , ifnull(qu.quote_id, dl.deal_id) as quote_id
     , case when dl.quote_id_valid = ''             then 'Yes'
            when qu.quote_id = dl.quote_id_valid    then 'Yes' else 'No' end as is_last_quote

     , dl.name                                        as deal_desc-----OK
     , ter.mdmid                                      as pc_code
     --attributes
     , ifnull(dl.progressiontech,0)  / 100            as Progression_percentage-----OK
     
     , case when Progression_percentage = 1        then dl.signaturedate
            --when dl.signaturedate is not null then dl.signaturedate 
            --when 0.90 > Progression_percentage 
            when Progression_percentage <= 0.90  
            then dl.estimatedclosedate--dl.estimatedsignaturedate 
            else dl.actualclosedate --dl.SIGNATUREDATE 
            end as Signature_Date_RPT -----OK


     , dl.leasingordertypeid                          as type -----OK
     , dl.leasingordertypeidname                      as typename -----OK
     , dl.leasingordersubtypeid                       as DEALSUBTYPE -----OK
     , dl.leasingordersubtypeidname                   as DEALSUBTYPENAME -----OK
     , dl.tenant_id                                   as Tenant_ID ----- Tenant OK
     , dl.account_id                                  as Brand_ID ----- Brand OK
     , ifnull(dl.account_id,'')                       as KEY_ACCOUNT_ID-----OK
     , ifnull(acn.name,'')                            as Tenant
     , cnt.extref                                     as prev_lease
    
     , dl.owneridname                                 as Leasing_Manager-----OK
     , dl.createdon                                         as Created_On -----
     , dl.modifiedon                                        as modified_on
     , ifnull(qu.estimatedhandoverdate, dl.occupancystartdate)  as Estimated_handover_date-----sera utilisé dans Quote
     
     , dl.actualclosedate                                   as SIGNATUREDATE
     , dl.estimatedclosedate                                as estimatedsignaturedate
     , dl.LEASEITREFERENCE
     , dl.statuscode-----à utiliser dans SAP
     , dl.statuscodename                                    as Status_Reason-----à utiliser dans SAP
     , (ifnull(qu.firmperiodyearstech::number,0) 
     + (ifnull(qu.firmperiodmonthstech::number,0)/12 ) 
     + (ifnull(qu.firmperioddaystech::number,0) / 365))             as firmperiod
     
     , qu.nextbreakoptiondate
     /*dateadd(day,-1,
        dateadd(year,ifnull(qu.firmperiodyearstech,0),
         dateadd(month,ifnull(qu.firmperiodmonthstech,0)
                      ,dateadd(day, ifnull(qu.firmperioddaystech,0) ,qu.estimatedhandoverdate)))) SGE */  as breakoption 
                      
     ,(ifnull(qu.contracttermyearstech,0) 
     + (ifnull(qu.contracttermmonthstech,0)/12) 
     + (ifnull(qu.contracttermdaystech,0)/365))                                              as Contract_terms/* SGE*/
     , /*case when type = '7bb26612-4c6f-ed11-9562-000d3adf7b71' then qu.estimatedhandoverdate
            when qu.contracttermyearstech > 250                then to_date('99991231','YYYYMMDD') 
            else dateadd(day,-1,
                   dateadd(year,ifnull(qu.contracttermyearstech,0),
                     dateadd(month,ifnull(qu.contracttermmonthstech,0)
                        ,dateadd(day, ifnull(qu.contracttermdaystech,0) ,qu.estimatedhandoverdate)))) end  SGE*/
       ifnull(qu.estimated_contract_end_date,dl.occupancyenddate)                                   as enddate 
                                                                                          
     , case when ifnull(dl.showindashboard,false)                            --only true is considered as Yes so null and false are considered as No
            then 'Yes' else 'No' end                                                                as SHOW_IN_DASHBOARD 
     
     --SBR
     , case when qu.fullsbr then 'Yes' else 'No' end                                            as fullsbr -----QUOTE
     , decode(qu.sbrtypecode,'809020000','Classic','809020001','Step', '809020002','Level','Other') as sbrtype -----QUOTE
     , case when qu.standardcomplementary then 'Complementary' else 'Standard' end                  as standardcomplementary -----QUOTE
     
     --Fields process 
     , dl.datefinancialproposalagreed
     , dl.datevalidated
     , dl.datelegalagreement
     , dl.signaturedate as signaturedate_process
     
     --kpis
     --wo currency
     --added

     --with currency
     , ifnull(curr.isocurrencycode,'')          as currency -----QUOTE
     
     , ifnull(qu.retailmgrytech::number,0)                            as RETAILMGRYTECH
     , ifnull(qu.sstmgrytech::number,0)                               as SSTMGRYTECH
     , ifnull(qu.parkingmgrytech::number,0)                           as PARKINGMGRYTECH
     , qu.newretailmgrannual::number                                  as New_Retail_MGR -----A supprimer ?
     , qu.mgrperformance::number                                      as mgr_performance
     , ifnull(qu.mgrperflfl::number,0)                                as mgrperflfl
     , ifnull(qu.mgrlfl::number,0)                                    as mgrlfl
     , qu.economicrent::number                                        as economic_rent
     , qu.effectiverent::number                                       as effective_rent
     , qu.parkingmgrannual::number                                    as parking_mgr
     , qu.resiliationcost::number                                     as INDEMNITIES_PAID_BY_TENANT  
     , ifnull(qu.expectedsales,0)::number                             as expectedsales
     , ifnull(qu.ecorentperfperc::number,0)                           as ECORENTPERFPERC
     , ifnull(qu.ecorentperfperclfl::number,0)                        as ECORENTPERFPERCLFL
     --, ifnull(qu.economicrentm::number,0)                             as ECONOMICRENTM
     , ifnull(qu.economicrentperformance::number,0)                   as ECONOMICRENTPERFORMANCE
     , ifnull(qu.effrentperc::number,0)                               as EFFRENTPERC
     , ifnull(qu.effectiverentlfl::number,0)                          as effectiverentlfl
     --, ifnull(qu.EFFECTIVERENTM2LFL::number,0)                        as EFFECTIVERENTM2LFL
     , ifnull(qu.effectiverentperflfl::number,0)                      as effectiverentperflfl
     
     , ifnull(qu.ECONOMICRENTLFL::number,0)                           as ECONOMICRENTLFL
     --, ifnull(qu.ECONOMICRENTM2LFL::number,0)                         as ECONOMICRENTM2LFL
     , ifnull(qu.ECONOMICRENTPERFLFL::number,0)                       as ECONOMICRENTPERFLFL
     
     , ifnull(qu.effrentperclfl::number,0)                            as EFFRENTPERCLFL
     --, ifnull(qu.effectiverentm::number,0)                            as EFFECTIVERENTM
     , ifnull(qu.effectiverentperformance::number,0)                  as EFFECTIVERENTPERFORMANCE
     , ifnull(qu.evictioncosttech::number,0)                          as EVICTIONCOSTTECH
     , ifnull(qu.resiliationcost::number,0)                           as RESILIATIONCOST
     , ifnull(qu.foctech::number,0)                                   as FOCTECH
     , ifnull(qu.keymoneytech::number,0)                              as KEYMONEYTECH
     , ifnull(qu.relettingworkstech::number,0)                        as RELETTINGWORKSTECH
     , ifnull(qu.mgrperfperc::number,0)                               as MGRPERFPERC
     , ifnull(qu.mgrperfoperclfl::number,0)                           as MGRPERFOPERCLFL
     , ifnull(qu.mgrvsbpperf::number,0)                               as MGRVSBPPERF
     --REMOVED, ifnull(qu.mgrvservperf::number,0)                              as MGRVSERVPERF
     , 0::number as MGRVSERVPERF
     , ifnull(qu.parkingmgrm2calc::number,0)                          as PARKINGMGRM2CALC
     , ifnull(qu.parkingmgrm2mcalc::number,0)                         as PARKINGMGRM2MCALC
     , ifnull(qu.newretailmgrbym2::number,0)                          as NEWRETAILMGRBYM2
     , ifnull(qu.sstmgrm2calc::number,0)                              as SSTMGRM2CALC
     , ifnull(qu.sstmgrm2mcalc::number,0)                             as SSTMGRM2MCALC
     , ifnull(qu.statereasoncode::number,0)                           as STATEREASONCODE
     , ifnull(qu.totalli::number,0)                                   as TOTALLI
     , ifnull(qu.totallimoofmgr::number,0)                            as TOTALLIMOOFMGR
     
     , 10::number                                   as available_indicator_deal


     , qu.caponindexationofmgr
     , qu.hascaponservicecharges
     , '' as cotenancyclausecodes
     , qu.hasdeviationstogreenappendix
     , case when  qu.hasexclusivityclause  = 1 then true else false end hasexclusivityclause
     --, qu.hasfocllworksorindemnity
     , qu.hasfreeservicechargesperiod
     , qu.hasfullgreenelectricitystore
     , qu.isfullledstore
     ,  qu.isgreenappendix
     , qu.hasnocrystallizationbeforeoption
     , case when qu.haspreferentialrights = 1 then true else false end  haspreferentialrights
     , qu.hasturnoverclausedeviation
     , '' as vacancyclausecodes
     ,dl.ISQFR
     ,dl.STATUSCODEGROUPNAME
     ,dl.FIRSTLAUNCHVALIDATIONDATE
     ,dl.ownerid
     ,dl.OWNERIDNAME
     ,dl.is_media_partner

     --, ifnull( attr1.pulse_quarter_original,attr2.pulse_quarter_original)   as pulse_quarter_original
     , ifnull( attr1.key_filter,attr2.key_filter)                                   as key_filter
     , ifnull( attr1.PULSE_SCENARIO,attr2.PULSE_SCENARIO)                           as PULSE_SCENARIO
     , ifnull( attr1.pulse_year, attr2.pulse_year)                                  as pulse_year
     , ifnull( attr1.pulse_quarter,attr2.pulse_quarter)                             as pulse_quarter
     , ifnull( attr1.qfr_deal, attr2.qfr_deal)                                      as is_qfr_deal_planit_new
    , ifnull( attr1.perf_cat, attr2.perf_cat)                                      as perf_cat_planit_new
    , ifnull(ifnull( attr1.YEARLY_SBR::number, attr2.YEARLY_SBR::number),0)        as YEARLY_SBR_RDL
     , ifnull(ifnull( attr1.BCOM::number, attr2.BCOM::number),0)                    as BCOM_RDL

     , case when attr1.qfr_deal is not null then true
            when attr2.qfr_deal is not null then true
            else false end as rdl_attr_has_been_found
     , ifnull( attr1._count, attr2._count)                                          as _count
     , ifnull( attr1.key_r,attr2.key_r)                                             as key_r

     , ifnull(qu.firmperioddaystech::number,0)                         as firmperioddaystech
     , ifnull(qu.firmperiodmonthstech::number,0)                       as firmperiodmonthstech
     , ifnull(qu.firmperiodyearstech::number,0)                        as firmperiodyearstech
     , dl.ismain
     , case when qu.conditionprecedentstatus is null                   then FALSE
            when qu.conditionprecedentstatus in (809020000,809020002)  then FALSE
            else TRUE end is_condition_precedent_blocking
     , isegenterable as has_real_effective_date
     --, ifnull(qu.statereasoncode::number,0)                            as statereasoncode
     , case when freq.MD_FREQUENCY is null and left(ter.mdmid,2) = 'FR' then 'EVERY_3_MONTHS'
            when freq.MD_FREQUENCY is null                              then 'EVERY_1_MONTH'
            else freq.MD_FREQUENCY end as MD_FREQUENCY
     , case when freq.NO_MONTHS    is null and left(ter.mdmid,2) = 'FR' then 3
            when freq.NO_MONTHS    is null                              then 1
            else freq.NO_MONTHS end as NO_MONTHS

------------------------------------------------------ASECK NRI 04022025
    ,ifnull(qu.nriimpactn::number,0)                                    as qu_nriimpactn
    ,ifnull(qu.nriimpactnplus1::number,0)                               as qu_nriimpactnplus1
    ,ifnull(qu.nrivslon::number,0)                                      as qu_nrivslon
    ,ifnull(qu.nrivslonplus1::number,0)                                 as qu_nrivslonplus1
    ,ifnull(qu.nrivsoldn::number,0)                                     as qu_nrivsoldn
    ,ifnull(qu.nrivsoldnplus1::number,0)                                as qu_nrivsoldnplus1
    --,ifnull(qu.oldnrilfln::number,0)                                    as qu_oldnrilfln
    --,ifnull(qu.oldnrilflnplus1::number,0)                               as qu_oldnrilflnplus1
-------------------------------------------------------------------------------------------------------------


------------------------------------------------------ASECK new_colonne 04022025

    ,qu.isdnvb
    ,qu.isfirstinashoppingcenter
    ,qu.ismarketentry
    ,qu.hasflagshipformat
    ,qu.isupsizing
    ,qu.isinnovativeconcept 
------------------------------------------------------------------------------------------------------------
    , dl.egmanuallyexcluded ------ASECK 14022025

    -------------ASECK 17032025       
    , dl.invoicingcontactid
    , dl.purchaseorder
    , dl.comment 
from
    lt_crm_deal as dl
    left outer join lt_crm_quote as qu on qu.deal_id = dl.deal_id --and dl.quote_id_valid=qu.quote_id aho202505 finalement on enleve la jointure car elle sera faite dans PBI avec table dimension DEAL
    left outer join lt_crm_transactioncurrency as curr on curr.transactioncurrency_id = dl.transactioncurrency_id
    left outer join lt_crm_territory as ter on ter.territory_id = dl.territory_id
    left outer join lt_crm_account as acn on acn.account_id = dl.account_id
    left outer join lt_crm_order as cnt on cnt.contract_id = dl.contract_id
    left outer join lt_PLAN_IT_RDL_ATTRIBUTES as attr1 on attr1.OPERATION_W_PULSE_ID = dl.deal_id 
    left outer join lt_PLAN_IT_RDL_ATTRIBUTES as attr2 on attr2.OPERATION_W_PULSE_ID = 'OL' || dl.LEASEITREFERENCE 
    left outer join lt_CRM_MASTER_DATA_GENERAL_FREQUENCY as freq on freq.MD_PULSE_ID = qu.mgrpaymentschedulecode
    ;
  );

