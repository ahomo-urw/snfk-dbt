{{ config(materialized='view') }}
select
    distinct
    a.parent_account,
    aparent.name GROUP_NAME,
    aparent.dunsnumber,
    aparent.country
from
   DPULSE.T_DIM_ACCOUNT a
    left join DPULSE.T_DIM_ACCOUNT aparent on aparent.ACCOUNT_ID = a.parent_account  
where
    a.category = 'Lessee' and  a.parent_account is not null ;