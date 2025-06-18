
  create or replace   view expose_dev.dpulse.my_second_dbt_model
  
   as (
    -- Use the `ref` function to select from other models

select *
from expose_dev.dpulse.my_first_dbt_model
where id = 1
  );

