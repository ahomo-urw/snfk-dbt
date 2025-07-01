{{ config(materialized='view') }}
select  lower(quoteid)                  as quote_id
      , lower(quotedetailid)            as rental_unit_id
      , lower(productid)                as rental_unit_ref_id
      , lower(territorybuildingid)      as territory_id --territorybuildingid
    , * exclude(quoteid, quotedetailid, productid, territoryid)
    from finops.dwh.crm_quoteproduct;