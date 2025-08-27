{{ config(materialized='view') }}
select
    distinct a.account_id lessee_guid,
    a.name Name,
    a.parent_account,
    aparent.name GROUP_NAME,
    a.dunsnumber,
    a.country
from
   DPULSE.T_DIM_ACCOUNT a
    left join DPULSE.T_DIM_ACCOUNT aparent on aparent.ACCOUNT_ID = a.parent_account  
where
    a.category = 'Lessee' ;
