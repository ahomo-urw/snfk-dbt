{{ config(materialized='view') }}

with lt_init as (
        SELECT
            DISTINCT CAL_ID
        FROM
            EXPOSE_DEV.DPERFMGT.DIM_PIVOT_CALENDAR_GLOBAL
        order by
            CAL_ID DESC
    )
select
    cal_id,
    year(to_date(cal_id || '01', 'YYYYMMDD')) YEAR,
    month(to_date(cal_id || '01', 'YYYYMMDD')) MONTH,
    quarter(to_date(cal_id || '01', 'YYYYMMDD')) QUARTER
from
    lt_init;
