create or replace view EXPOSE_DEV.DPULSE.CRM_PRODUCT
	--( RENTAL_UNIT_REF_ID,
	-- RENTAL_UNIT_EXTREF,
	-- TERRITORY_ID,
	-- ORDER_ID,
 --    product_sub_type_id, 
 --    product_detail_id,
	-- BILLINGORDERELEMENT3ID,
	-- BILLINGORDERELEMENT3IDNAME,
	-- COMPANYID,
	-- COMPANYIDNAME,
	-- ERV,
	-- FREQUENCYCODE,
	-- FREQUENCYCODENAME,
	-- ISVACANT,
	-- ISVACANTNAME,
	-- LEASINGPRODUCTTYPECODE,
	-- LEASINGPRODUCTTYPECODENAME,
	-- LESSEE,
	-- MAXIMUMSTAFFNUMBER,
	-- MGR,
	-- ORDERIDNAME,
	-- PRODUCTDETAILIDNAME,
	-- PRODUCTSIZE,
	-- PRODUCTSUBTYPEIDNAME,
	-- PRODUCTTYPEID,
	-- PRODUCTTYPEIDNAME,
	-- RATESCURRENTYEAR,
	-- RRV,
	-- SCOPEID,
	-- SCOPEIDNAME,
	-- SERVICECHARGES,
	-- STORECODE,
	-- TERRITORYIDNAME,
	-- TRV,
	-- VISIOGLOBECODE,
	-- ZONETYPECODE,
	-- ZONETYPECODENAME,
	-- DESCRIPTION,
	-- HIERARCHYPATH,
	-- NAME,
	-- PARENTPRODUCTID,
	-- PARENTPRODUCTIDNAME,
	-- PRICE,
	-- PRICELEVELID,
	-- PRODUCTSTRUCTURE,
	-- PRODUCTSTRUCTURENAME,
	-- STANDARDCOST,
	-- STATECODE,
	-- STATECODENAME,
	-- STATUSCODE,
	-- STATUSCODENAME,
	-- BU_EXTERNALREFERENCE,
	-- DIGEIZID)
    as 
select  
lower(productid)                                                as rental_unit_ref_id
      , productnumber                                                   as rental_unit_extref
      , lower(territoryid)                                              as territory_id
      , lower(orderid)                                                  as order_id
      , ifnull(productsubtypeid,'')                                     as product_sub_type_id 
      , ifnull(productdetailid,'')                                      as product_detail_id
      , * exclude(productid, territoryid, orderid, productnumber, productsubtypeid,productdetailid) 
      from finops.dwh.crm_product;