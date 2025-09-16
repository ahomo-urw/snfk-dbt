create or replace view DPULSE.CRM_RENTAL_UNIT_LO as 
with 
lt_crm_leasingorderproduct                      as ( select * from crm_leasingorderproduct), 
lt_crm_leasingorder                             as ( select * from crm_leasingorder)
select lo.leasing_order_id
     , '''' as deal_id 
     , '''' as quote_id
     , lo_pdt.rental_unit_ref_id
     , lo_pdt.rental_unit_id
     , lo.transactioncurrency_id
     
     , lo_pdt.territory_id
     
     , lo_pdt.LEASINGPRODUCTTYPECODE
     , lo_pdt.LEASINGPRODUCTTYPECODENAME
     
     , lo_pdt.OLDGLA
     , lo_pdt.OLDMGR
     , 0 as OLDSBR -----ASECK 26/05/2025
     , lo_pdt.GLA 
     , lo_pdt.name 
     , 0 as targetdurationshortterm
     , 0 as targetevictioncost
     , 0 as targetindemnitiesreceivedlessee
     , 0 as targetmgrshortterm
     , lo_pdt.targetterminationdate
     , null as profit_center_sap
     
from       lt_crm_leasingorderproduct  as lo_pdt
inner join lt_crm_leasingorder         as lo     on lo.leasing_order_id = lo_pdt.leasing_order_id
where lo_pdt.rental_unit_id is not null;