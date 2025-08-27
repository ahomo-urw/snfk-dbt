
  create or replace   view EXPOSE_DEV.DPULSE.CRM_LEASINGORDER_W_KEY
  
   as (
    
 ---informations compte qui permet de recuperer brand et lessee
with lt_account_id_for_lo as (
        select leasing_order_id, max(key_account_id) as key_account_id 
        from dpulse.dim_deal 
        where show_in_dashboard = 'Yes' 
          and leasing_order_id <> ''
        group by leasing_order_id    ),

--infos leasing order
     lt_leasing_order as (
    select lo.leasing_order_id
         , ifnull(loACN.key_account_id,'')      as key_account_id
         , lo.name                              as leasing_order_desc
         , ter.mdmid                            as pc_code
         --, lo.*
         --attributes
         , lo.progressionpercentagerf           as progressionpercentage
         , lo.targetsignaturedate               as TARGET_SIGNATURE_DATE
         , lo.targeteffectivedate
         
         , lo.leasingordertypeid                as TYPE
         , lo.leasingordertypeidname            as TYPE_NAME
         , lo.leasingordersubtypeid             as SUB_TYPE
         , lo.leasingordersubtypeidname         as SUB_TYPE_NAME
         
         , lo.ownerid                           as LEASING_MANAGER
         
         , lo.createdon                         as CREATED_ON
         --, lo.modifiedon                        as MODIFIED_ON --to be deleted
         
         ----, lo.contractid                        as OLD_CONTRACT_ID --to be added after refresh
         , '' as OLD_CONTRACT_ID
         , lo.tenantoldcontract                 as TENANT_OLD_CONTRACT
         , lo.transactioncurrency_id            as CURRENCY_ID
         , lo.transactioncurrencyidname         as CURRENCY_NAME
         , lo.statecode                         as STATUS_REASON
         , lo.statecodename                     as STATUS_REASON_NAME
         , lo.statuscode                        as STATUSCODE
         , lo.statuscodename                    as STATUSCODE_NAME
         , case lo.ismain when false then 'No' else 'Yes' end as IS_MAIN -- null is considered as Yes
         , lo.planitoperationcode               as PLAN_IT_OPERATION_CODE

         
         --KPIs Without Currency
         , lo.targetcontracttermyear::Number                  as TARGET_CONTRACT_TERM
         , lo.targetfirmperiodyears::Number                   as TARGET_FIRM_PERIOD --to be added after refresh
         , lo.TOTAL_GLA::Number                                   as TOTAL_GLA
         ------, 0 as TOTAL_GLA
         --KPIs
         , ifnull(curr.isocurrencycode,'')                    as currency
         
         --, lo.totaloldmgrrentalunit::Number                   as MGR_OLD_CONTRACT --to be replaced ???
         , lo.targetmgrannual::Number                         as TARGET_MGR
         , lo.targetparkingmgrannual::Number                  as TARGET_PARKING_MGR
         , lo.targetseparatedstoragemgramountannual::Number   as TARGET_SEPARATED_STORAGE_MGR
        -- , 0, lo.targetrentfreeperiodweeks::Number  SGE_TO_DO              as TARGET_RENT_FREE_PERIOD
         , lo.targetfoc::Number                               as TARGET_FOC
         , lo.targetsteprent::Number                          as TARGET_STEP_RENT
         , lo.targetrelettingworks::Number                    as TARGET_RELETTING_WORKS
         --, lo.totaltargetmgrannual::Number                    as TOTAL_TARGET_MGR --to be replaced ???
         , lo.targetkeymoney::Number                          as TARGET_KEY_MONEY
         --, lo.targetevictioncost::Number                      as TARGET_EVICTION_COST
         
         --, lo.mgrperformance::Number                          as MGRPERFORMANCE--to be replaced ???
         --, lo.economicrent::Number                            as ECONOMICRENT--to be replaced ???
         --, lo.effectiverent::Number                           as EFFECTIVERENT--to be replaced ???
         , lo.isqfr
    from dpulse.crm_leasingorder as lo
    left outer join dpulse.crm_transactioncurrency as curr  on curr.transactioncurrency_id = lo.transactioncurrency_id
    left outer join dpulse.crm_territory           as ter   on ter.territory_id            = lo.territory_id
    left outer join lt_account_id_for_lo           as loACN on loACN.leasing_order_id      = lo.leasing_order_id
    where lo.leasing_order_id <> ''
)

select leasing_order_id
     , leasing_order_desc
     , pc_code
     
     --attributes
     ,progressionpercentage / 100 as Progression_percentage
     , key_account_id
     ,TARGET_SIGNATURE_DATE
     , targeteffectivedate
     ,TYPE
     ,TYPE_NAME
     ,SUB_TYPE
     ,SUB_TYPE_NAME
     ,LEASING_MANAGER
     
     ,CREATED_ON
     --,MODIFIED_ON
     ,OLD_CONTRACT_ID
     ,TENANT_OLD_CONTRACT
     ,CURRENCY_ID
     ,CURRENCY_NAME
     ,STATUS_REASON
     ,STATUS_REASON_NAME
     ,STATUSCODE
     ,STATUSCODE_NAME
     ,IS_MAIN
     ,PLAN_IT_OPERATION_CODE
     
     --kpis without currency
     ,TARGET_CONTRACT_TERM
     ,TARGET_FIRM_PERIOD
     ,TOTAL_GLA
     --kpis
     , currency
     --,MGR_OLD_CONTRACT
     ,TARGET_MGR,TARGET_PARKING_MGR,TARGET_SEPARATED_STORAGE_MGR--,TARGET_RENT_FREE_PERIOD
     ,TARGET_FOC,TARGET_STEP_RENT
     ,TARGET_RELETTING_WORKS
     --,TOTAL_TARGET_MGR
     ,TARGET_KEY_MONEY
     --,TARGET_EVICTION_COST
     --,MGRPERFORMANCE
     --,ECONOMICRENT
     --,EFFECTIVERENT
     , 10::number                               as available_indicator_lo
     , isqfr
from lt_leasing_order;
  );

