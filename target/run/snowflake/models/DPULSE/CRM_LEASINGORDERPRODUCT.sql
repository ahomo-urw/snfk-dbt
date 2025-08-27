
  create or replace   view EXPOSE_DEV.DPULSE.CRM_LEASINGORDERPRODUCT
  
   as (
    
select ifnull(leasingorderid,'')                                        as leasing_order_id
     , lower(leasingorderproductid)                                     as rental_unit_id
     , lower(productid)                                                 as rental_unit_ref_id
     , lower(buildingterritoryid)                                       as territory_id
     , * exclude(leasingorderid, productid, buildingterritoryid)
from finops.dwh.crm_leasingorderproduct
  );

