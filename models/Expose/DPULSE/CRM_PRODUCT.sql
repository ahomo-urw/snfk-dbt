{{ config(materialized='view') }}
select  
lower(productid)                                                as rental_unit_ref_id
      , productnumber                                                   as rental_unit_extref
      , lower(territoryid)                                              as territory_id
      , lower(orderid)                                                  as order_id
      , ifnull(productsubtypeid,'')                                     as product_sub_type_id 
      , ifnull(productdetailid,'')                                      as product_detail_id
      , * exclude(productid, territoryid, orderid, productnumber, productsubtypeid,productdetailid) 
      from finops.dwh.crm_product;