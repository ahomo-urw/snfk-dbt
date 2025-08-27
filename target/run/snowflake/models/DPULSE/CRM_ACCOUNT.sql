
  create or replace   view EXPOSE_DEV.DPULSE.CRM_ACCOUNT
  
   as (
    
select ifnull(accountid,'')      as account_id
     , * exclude(accountid)
from finops.dwh.crm_account;
  );

