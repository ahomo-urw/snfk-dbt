{{ config(materialized='view') }}

select 
 PC_ID,
 PRODUCT_EXT_REF,
 CONTRACT_ID,
BU_EXTREF ,
    case  when  SCENARIOS_RR like 'CLOSING4_%'
       then '01/01/'||year(current_date) 
     else    '01/01/'||year(dateadd(year,1,current_date)) end YM_START ,
    case  when  SCENARIOS_RR like 'CLOSING4_%'
       then '31/12/'||year(current_date) 
     else   '31/12/'||year(dateadd(year,1,current_date)) end   YM_END ,
         currency, 
        indicator_name KPI_TYPE, 
        --indicator_denum,
        indicator_num  KPI_AMOUNT,
        'CONTRACT_PRODUCT' FACT_TYPE 
--*  exclude (key_r,deleted,timestamp) 
from DPERFMGT.V_DIM_RENT_ROLL_FOR_VACANCY dim 
join (
    select  key_r, 
        currency, 
        indicator_name, 
        indicator_denum,
        indicator_num
from EXPOSE.DPERFMGT.PLANIT_OPERATION_CUBE_RENT_ROLL_FOR_VACANCY_W_CURRENCY_AGG
union all
select key_r, 
        null currency, 
        indicator_name, 
        indicator_denum,
        indicator_num 
from EXPOSE.DPERFMGT.PLANIT_OPERATION_CUBE_RENT_ROLL_FOR_VACANCY_Wo_CURRENCY_AGG
) kpi on kpi.key_r=dim.key_r
 where 
 dim.status =  'Occupied'
--where --cscenario='FORECAST2' and  --cpropc='FR-SC-150' 

--cutoffcalmonth='202506'
--and clease='F106/11000186_SC'
