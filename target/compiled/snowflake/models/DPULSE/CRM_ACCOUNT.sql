
select ifnull(accountid,'')      as account_id
     , * exclude(accountid)
from finops.dwh.crm_account;