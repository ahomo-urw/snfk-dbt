{{ config(materialized='view') }}
select extref CONTRACT_ID, name CONTRACT_NAME, startdate, enddate  , signing_date,next_break_option
from dpulse.t_dim_contract;