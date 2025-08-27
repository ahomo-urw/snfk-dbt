
  create or replace   view EXPOSE_DEV.DPULSE.test_view
  
   as (
    select * from expose_dev.dpulse.crm_quote limit 1000
  );

