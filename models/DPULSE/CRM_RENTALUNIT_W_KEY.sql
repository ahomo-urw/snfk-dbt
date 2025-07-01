--create or replace view EXPOSE_DEV.DPULSE.CRM_RENTALUNIT_W_KEY
{{ config(materialized='view') }}


with lt_crm_rental_unit_quote as (
select
*
from
DPULSE.crm_rental_unit_quote
),
lt_crm_rental_unit_lo as (
select
*
from
DPULSE.crm_rental_unit_lo
),
lt_crm_rental_unit_deal_cp as (
select
*
from
DPULSE.crm_rental_unit_deal_cp
),
lt_crm_product as (
select
*
from
DPULSE.crm_product
),
lt_crm_territory as (
select
*
from
DPULSE.crm_territory
),
lt_crm_transactioncurrency as (
select
*
from
DPULSE.crm_transactioncurrency
),
lt_crm_deal_w_key as (
select
*
from
DPULSE.crm_deal_w_key
),
lt_crm_orderproduct as (
select
*
from
DPULSE.crm_orderproduct
),
lt_crm_order as (
select
*
from
DPULSE.crm_order
),
lt_crm_account as (
select
*
from
DPULSE.crm_account
),
lt_crm_leasingorder_w_key as (
select
*
from
DPULSE.crm_leasingorder_w_key
),
lt_planit_finance_dim_entity as (
select
*
from
expose.dperfmgt.planit_finance_dim_entity
),
lt_deagregation_driver as (
select
*
from
DPulse.MAPPING_RENTALUNIT_TYPE_CODE
),
lt_entity as (
select
p_cpx as pc_code,
entity,
primary_building
from
lt_planit_finance_dim_entity
),
lt_default_entity as (
select
pc_code,
min(entity) as entity
from
lt_entity
where
primary_building = 'Y'
group by
pc_code
),
lt_lo_dl_qu_ru                                      as ( select 'QUOTE' as source
                         , LEASING_ORDER_ID,DEAL_ID,QUOTE_ID,RENTAL_UNIT_REF_ID,RENTAL_UNIT_ID
                         , TRANSACTIONCURRENCY_ID,TERRITORY_ID
                         , to_char(LEASINGPRODUCTTYPECODE) as LEASINGPRODUCTTYPECODE,to_char(LEASINGPRODUCTTYPECODENAME) as LEASINGPRODUCTTYPECODENAME
                         , OLDGLA,OLDMGR
                         , OLDSBR -----ASECK 26/05/2025
                         ,GLA
                         , NAME
                         , 0 as TARGETDURATIONSHORTTERM
                         , 0 as TARGETEVICTIONCOST
                         , 0 as TARGETINDEMNITIESRECEIVEDLESSEE
                         , 0 as TARGETMGRSHORTTERM
                         , null as TARGETTERMINATIONDATE 
                         , null as CP_start_date
                         , null as CP_end_date
                         , profit_center_sap as CP_profit_center_sap
                         , null as CP_mgr
                         , null as CP_marketing
                         , null as CP_service_charges
                         , null as CP_network_fees
                         , null as CP_md_frequency
                         , null as CP_MD_NO_MONTHS
                    from lt_crm_rental_unit_quote union all 
                    select 'LEASING_ORDER' as source
                         , LEASING_ORDER_ID,DEAL_ID,QUOTE_ID,RENTAL_UNIT_REF_ID,RENTAL_UNIT_ID
                         , TRANSACTIONCURRENCY_ID,TERRITORY_ID
                         , to_char(LEASINGPRODUCTTYPECODE) as LEASINGPRODUCTTYPECODE,to_char(LEASINGPRODUCTTYPECODENAME) as LEASINGPRODUCTTYPECODENAME
                         , OLDGLA,OLDMGR
                         , OLDSBR -----ASECK 26/05/2025
                         ,GLA,NAME
                         , TARGETDURATIONSHORTTERM
                         , TARGETEVICTIONCOST
                         , TARGETINDEMNITIESRECEIVEDLESSEE
                         , TARGETMGRSHORTTERM
                         , TARGETTERMINATIONDATE 
                         , null as CP_start_date
                         , null as CP_end_date
                         , null as CP_profit_center_sap
                         , null as CP_mgr
                         , null as CP_marketing
                         , null as CP_service_charges
                         , null as CP_network_fees
                         , null as CP_md_frequency
                         , null as CP_MD_NO_MONTHS
                     from lt_crm_rental_unit_lo  union all
                     select 'DEAL_CP' as source
                         , ''       as LEASING_ORDER_ID
                         , DEAL_ID
                         , DEAL_ID  as QUOTE_ID
                         , RENTAL_UNIT_REF_ID,RENTAL_UNIT_ID
                         , TRANSACTIONCURRENCY_ID,TERRITORY_ID
                         , 'CP_RISE' as LEASINGPRODUCTTYPECODE
                         , 'CP_RISE' as LEASINGPRODUCTTYPECODENAME
                         , 0 as OLDGLA
                         , 0 as OLDMGR
                         , 0 as OLDSBR -----ASECK 26/05/2025
                         , 0 as GLA,'' as NAME
                         , 0 as TARGETDURATIONSHORTTERM
                         , 0 as TARGETEVICTIONCOST
                         , 0 as TARGETINDEMNITIESRECEIVEDLESSEE
                         , 0 as TARGETMGRSHORTTERM
                         , null as TARGETTERMINATIONDATE 
                         , start_date               as CP_start_date
                         , end_date                 as CP_end_date
                         , profit_center_sap        as CP_profit_center_sap
                         , mgr                      as CP_mgr
                         , marketing                as CP_marketing
                         , service_charges          as CP_service_charges
                         , network_fees             as CP_network_fees
                         , md_frequency             as CP_md_frequency
                         , MD_NO_MONTHS             as CP_MD_NO_MONTHS
                        
                    from lt_crm_rental_unit_deal_cp )
                    
,lt_rental_unit_flat                                 as ( select source,
                         case when mapping_lo_deal.leasing_order_id is not null 
                              then mapping_lo_deal.leasing_order_id 
                              else ifnull(ru.leasing_order_id,'') end  as leasing_order_id_prep
                     , leasing_order_id_prep                           as leasing_order_id
                     , ru.deal_id
                     , ru.quote_id
                     
                     , ru.rental_unit_id
                     , ru.RENTAL_UNIT_REF_ID
                     , ter.territory_id
                     
                     
                     , ifnull(ru.name,'')                                           as rental_unit_desc -- voir si les nouveaux sont au même endroit --name
            
            
                     --attributs
                     , replace(ifnull(ter.mdmid,ru.territory_id),'/','-')          as buextref_init
                     
                     , case when pdt.rental_unit_ref_id is null 
                            then 'Yes' else 'No' end                                as NEW_RENTALUNIT
            
                            --------------------------
                     , case when pdt.rental_unit_ref_id is null then 'Yes' 
                            when pdt.isvacant = false           then 'No'
                            else 'Yes' end                                          as IS_VACANT -- + le champ 
                     
                     --, case when VACANT = false then 'No' else 'Yes' end            as IS_VACANT --
                     --, VACANT --------
                     
                     , ifnull(pdt.rental_unit_extref,ru.name)                            as RENTALUNIT_CODE
                     , ru.LEASINGPRODUCTTYPECODENAME                                as RENTALUNIT_TYPE --changer avec le text à la place du code
            
                     --, ifnull(CATEGORYNAME,'Standing')                              as CATEGORY_NAME --building territory id --is_category
                     , case when ter.iscategory then 'Pipeline' else 'Standing' end as CATEGORY_NAME
                     , acn.name                                                     as OLD_TENANT
                     , _order.extref                                                as OLD_CONTRACT_CODE
                     , _order.name                                                  as OLD_CONTRACT_NAME
                     , _order.enddate                                               as OLD_CONTRACT_END_DATE
                     , case when _order.extref is not null then 1 else 0 end        as has_RE_order_Cat
                     
                     --, row_number()                     over ( partition by ru.deal_id, ru.quote_id, ru.rental_unit_id order by _order.startdate desc ) as OLD_CONTRACT_ORDER
                     , decode(max(has_RE_order_Cat) over ( partition by ru.deal_id, ru.quote_id, ru.rental_unit_id ),1,true,false) as HAS_OLD_CONTRACT
                     , row_number() over ( partition by ru.deal_id, ru.quote_id, ru.rental_unit_id, has_RE_order_Cat order by _order.startdate desc, _order.enddate asc ) as OLD_CONTRACT_ORDER_BY_CAT
            
                     
                     --kpi without currency
                     --, ifnull(ru.unittypename,'GLA')                                as unit --default assignement to GLA for now 
                     , 'GLA'                                                        as unit
                     , ru.gla
                     , ru.OLDGLA                                                    as old_gla
                     --, decode(ifnull(ru.gla,0), 0, ru.gla, ru.OLDGLA)               as gla_resized
                     , decode(ifnull(ru.gla,0), 0, 0, ru.gla)               as gla_resized
                     --attribute with currency
                     , ifnull(curr.isocurrencycode,'')                              as currency
                     , ifnull(pdt.erv,0)                                            as erv
                     , ifnull(pdt.rrv,0)                                            as rrv
                     , ifnull(pdt.trv,0)                                            as trv
                     --, OLDMGR_MONEY_BASE                                            as old_mgr_base
                     , ifnull(OLDMGR,0)                                              as old_mgr
                     , ifnull(OLDSBR,0)                                              as old_sbr -----ASECK 26/05/2025    
                     , ifnull(TARGETDURATIONSHORTTERM,0)                            as TARGETDURATIONSHORTTERM
                     , ifnull(TARGETEVICTIONCOST, 0)                                as TARGETEVICTIONCOST
                     , ifnull(TARGETINDEMNITIESRECEIVEDLESSEE, 0)                   as TARGETINDEMNITIESRECEIVEDLESSEE
                     , ifnull(TARGETMGRSHORTTERM, 0)                                as TARGETMGRSHORTTERM
                     , TARGETTERMINATIONDATE
                     , CP_start_date
                     , CP_end_date
                     , CP_profit_center_sap
                     , CP_mgr
                     , CP_marketing
                     , CP_service_charges
                     , CP_network_fees
                     , CP_md_frequency
                     , CP_MD_NO_MONTHS
                
                    from lt_lo_dl_qu_ru             as ru
                    left outer join lt_crm_product  as pdt  on pdt.rental_unit_ref_id = ru.rental_unit_ref_id
                    left outer join lt_crm_territory             as ter  on ter.territory_id   = ru.territory_id
                    left outer join lt_crm_transactioncurrency   as curr on curr.transactioncurrency_id = ru.transactioncurrency_id
                    left outer join lt_crm_deal_w_key            as mapping_lo_deal
                        on  mapping_lo_deal.quote_id = ru.quote_id
                        and mapping_lo_deal.leasing_order_id <> ''
                    left outer join lt_crm_orderproduct          as order_pdt   on order_pdt.rental_unit_ref_id = pdt.rental_unit_ref_id--RENTALUNIT_CODE --
                    left outer join lt_crm_order                 as _order      on  _order.contract_id = order_pdt.contract_id 
                                                                                    and _order.startdate   < mapping_lo_deal.estimated_handover_date
                                                                                    and _order.statuscode  not in (100001)
                    --left outer join expose.dpulse.crm_order          as _order on _order.contract_id     = pdt.order_id 
                                                                              --and mapping_lo_deal.estimated_handover_date between _order.startdate and _order.enddate
                    left outer join lt_crm_account        as acn    on acn.account_id         = _order.customer_id ),



lt_rental_unit_conso                                as ( select source
                                 , leasing_order_id--, leasing_order_id_high_level
                                 , deal_id, rental_unit_id
                                 , RENTAL_UNIT_REF_ID ---aseck
                                 , quote_id, rental_unit_desc
                                 --attributes
                                , buextref_init
                                ,NEW_RENTALUNIT
                                ,IS_VACANT
                                ,RENTALUNIT_CODE
                                ,RENTALUNIT_TYPE
                                ,CATEGORY_NAME
                                ,OLD_TENANT
                                ,OLD_CONTRACT_CODE
                                ,OLD_CONTRACT_NAME
                                ,OLD_CONTRACT_END_DATE
                                 , CP_profit_center_sap
                                 --kpis with currency
                                 , currency ,erv, rrv, trv, old_mgr
                                 , old_sbr -----aseck 25/05/2025         
                                 , CP_mgr
                                 , CP_marketing
                                 , CP_service_charges
                                 , CP_network_fees
                                 --kpis with unit
                                 , unit, gla, old_gla, gla_resized
                                 , CP_md_frequency
                                 , CP_MD_NO_MONTHS
                                 , TARGETDURATIONSHORTTERM, TARGETEVICTIONCOST, TARGETINDEMNITIESRECEIVEDLESSEE, TARGETMGRSHORTTERM, TARGETTERMINATIONDATE
                                 , CP_start_date
                                 , CP_end_date
                            from lt_rental_unit_flat
                            where ( HAS_OLD_CONTRACT = true  and has_RE_order_Cat = 1 and OLD_CONTRACT_ORDER_BY_CAT = 1 )
                               or ( HAS_OLD_CONTRACT = false                          and ifnull(OLD_CONTRACT_ORDER_BY_CAT,1) = 1 ))
                            

select     mn.* exclude(RENTALUNIT_CODE, unit)
, ifnull(dfltBu.entity,mn.buextref_init) as buextref
, case when RENTALUNIT_CODE is null then ifnull(buextref,'') || '_C'
else RENTALUNIT_CODE         end as RENTALUNIT_CODE
, drv_cat.usage_split
, ifnull(drv_cat.RENTALUNIT_TYPE_CODE,'RET')                                                                          as RENTALUNIT_TYPE_CODE
, ifnull(drv_cat.unit,'GLA')                                                                                          as unit

, sum(ifnull(mn.gla_resized,0)) over (partition by mn.leasing_order_id, mn.deal_id, mn.quote_id)                      as tot_gla
, sum(ifnull(mn.gla_resized,0)) over (partition by mn.leasing_order_id, mn.deal_id, mn.quote_id, drv_cat.usage_split) as tot_gla_usage

, div0(mn.gla_resized, tot_gla)                                               as allocation_gla
, div0(mn.gla_resized, tot_gla_usage)                                         as allocation_gla_usage
, case when dl.scope in ('Westfield Rise','PMPS') then ifnull(buextref,'') || '_' || ifnull(mn.CP_profit_center_sap,'')
when mn.NEW_RENTALUNIT = 'Yes'
then ifnull(rental_unit_desc,'') || '_' || ifnull(buextref,'') || '_' || ifnull(mn.RENTALUNIT_TYPE,'') 
else RENTALUNIT_CODE end as rental_unit_common_id

from lt_rental_unit_conso                            as mn
left outer join lt_crm_deal_w_key                    as dl     on dl.quote_id              = mn.quote_id
left outer join lt_crm_leasingorder_w_key            as lo     on lo.leasing_order_id      = mn.leasing_order_id
left outer join lt_entity                            as entity on entity.entity            = mn.buextref_init
left outer join lt_default_entity                    as dfltBu on ( entity.entity is null and dl.scope     in ('Specialty Leasing','Westfield Rise') and 1=2 )
                                   or ( entity.entity is null and dl.scope not in ('Specialty Leasing','Westfield Rise') --and 1=2
                                                                                                                         and dl.quote_id is not null                                     and dfltBu.pc_code = dl.pc_code) --remove and 1=2 
                                   or ( entity.entity is null and dl.scope not in ('Specialty Leasing','Westfield Rise') --and 1=2 
                                                                                                                         and dl.quote_id is null     and lo.leasing_order_id is not null and dfltBu.pc_code = lo.pc_code) --remove and 1=2 
left outer join lt_deagregation_driver               as drv_cat on drv_cat.RENTALUNIT_TYPE = mn.RENTALUNIT_TYPE;



--where     buextref = 'F103-FBEAERO-FBU1340' tentative mise en place de filtre pour optimiser traitement en  tests;