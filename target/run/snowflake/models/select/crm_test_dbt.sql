
  create or replace   view EXPOSE_DEV.DPULSE.crm_test_dbt
  
   as (
    select * from finops_dev.dwh.crm_quote limit 1000
  );

