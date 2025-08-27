{{ config(materialized='view') }}
select
    distinct upper(product_ext_ref) product_ext_ref,
    upper(RENTAL_UNIT_DESC) RENTAL_UNIT_DESC,
    nvl(com.BUEXTREF,fact.buextref) BUEXTREF,
    NEW_RENTALUNIT,
    upper(RENTALUNIT_CODE) RENTALUNIT_CODE,
    upper(RENTALUNIT_TYPE) RENTALUNIT_TYPE,
    upper(RENTALUNIT_TYPE_CODE) RENTALUNIT_TYPE_CODE,
    max(CATEGORY_NAME) over (partition by product_ext_ref) CATEGORY_NAME
from
    dperfmgt.T_FACT_PERFORMANCE_GLOBAL fact
    left join (
        select
            distinct rental_unit_common_id,
            RENTAL_UNIT_DESC,
            BUEXTREF,
            NEW_RENTALUNIT,
            RENTALUNIT_CODE,
            RENTALUNIT_TYPE,
            RENTALUNIT_TYPE_CODE,
            CATEGORY_NAME
        from
            dpulse.t_dim_rentalunit_common_referential
    ) com on fact.product_ext_ref = com.rental_unit_common_id;
