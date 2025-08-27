{{ config(materialized='view') }}
select
    distinct a.parent_account,
    aparent.name GROUP_NAME
from
    DPULSE.T_DIM_ACCOUNT a
    join DPULSE.T_DIM_ACCOUNT aparent on aparent.ACCOUNT_ID = a.parent_account
where
    a.category = 'Brand';