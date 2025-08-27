
    select ifnull(opportunityid,'')                                         as deal_id
         , ifnull(opportunityproductid,'')                                  as rental_unit_id
         , ifnull(productid,'')                                             as rental_unit_ref_id
         , ifnull(territoryid,'')                                           as territory_id
         , ifnull(producttypeid,'')                                         as product_type_id
         , ifnull(productsubtypeid,'')                                      as product_sub_type_id 
         , ifnull(productdetailid,'')                                       as product_detail_id --TODO
         , ifnull(transactioncurrencyid,'')                                 as transactioncurrency_id
         , * exclude(opportunityid, opportunityproductid, productid,territoryid,producttypeid,productsubtypeid, productdetailid,
         transactioncurrencyid)--aho 202506 
    from finops.dwh.crm_dealcpproduct;