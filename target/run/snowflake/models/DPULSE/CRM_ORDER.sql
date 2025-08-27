
  create or replace   view EXPOSE_DEV.DPULSE.CRM_ORDER
  
   as (
    
select   
        ifnull(accountlesseeid,'')             as lessee_id
       ,enddate
       ,startdate
       ,extref
       , ifnull(customerid,'')                  as customer_id
       ,name
        ,ifnull(salesorderid,'')                as contract_id
       , ifnull(transactioncurrencyid,'')       as transactioncurrency_id
       , * exclude(accountlesseeid,enddate,startdate,extref ,customerid,name,salesorderid,transactioncurrencyid) 
from finops.dwh.crm_order;
  );

