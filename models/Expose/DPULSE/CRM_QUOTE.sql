{{ config(materialized='view') }}
select lower(opportunityid)            as deal_id
      ,lower(quoteid)                  as quote_id
      ,lower(ifnull(customerid,''))    as account_id,
      * exclude (opportunityid,quoteid,customerid)
from finops.dwh.crm_quote;