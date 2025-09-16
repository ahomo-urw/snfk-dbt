create or replace view DPULSE.CRM_LEASINGORDERPRODUCT(
LEASING_ORDER_ID,
RENTAL_UNIT_ID,
RENTAL_UNIT_REF_ID,
TERRITORY_ID,
BUILDINGTERRITORYIDNAME,
GLA,
ISPRODUCTOVERRIDDEN,
LEASINGORDERIDNAME,
LEASINGORDERPRODUCTID,
LEASINGPRODUCTTYPECODE,
LEASINGPRODUCTTYPECODENAME,
NAME,
OLDGLA,
OLDMGR,
PRODUCTIDNAME,
PRODUCTTYPEIDNAME,
TERRITORYID,
TERRITORYIDNAME,
TARGETTERMINATIONDATE
)  comment='AHOMO modif 202505'
as 
select ifnull(leasingorderid,'')                                        as leasing_order_id
     , lower(leasingorderproductid)                                     as rental_unit_id
     , lower(productid)                                                 as rental_unit_ref_id
     , lower(buildingterritoryid)                                       as territory_id
     , * exclude(leasingorderid, productid, buildingterritoryid)
from finops.dwh.crm_leasingorderproduct;
