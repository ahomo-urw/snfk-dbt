{{ config(materialized='view') }}
select
    distinct a.account_id brand_guid,
    a.name Name,
    a.parent_account,
    aparent.name GROUP_NAME,
    a.id_mercury,
    a.branch_level_1,
    a.branch_level_1_id,
    a.branch_level_2,
    a.branch_level_2_id,
    a.branch_level_3,
    a.branch_level_3_id
from
   DPULSE.T_DIM_ACCOUNT a
    left join DPULSE.T_DIM_ACCOUNT aparent on aparent.ACCOUNT_ID = a.parent_account  
where
    a.category = 'Brand' ;