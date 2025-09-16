create or replace view DPULSE.CRM_RENTAL_UNIT_DEAL_CP
comment='aho correction 202506'
as 
with 
lt_CRM_DEAL_W_KEY                           as ( select * from DPULSE.CRM_DEAL_W_KEY)
,lt_crm_dealcpproduct                        as ( select * from DPULSE.crm_dealcpproduct) 
, lt_crm_product                              as ( select * from DPULSE.crm_product)
,lt_crm_territory                            as ( select * from DPULSE.crm_territory)
,lt_crm_productinformation                   as ( select * from DPULSE.crm_productinformation)
,lt_CRM_MASTER_DATA_CP_FREQUENCY             as ( select * from EXPOSE.DPULSE.CRM_MASTER_DATA_CP_FREQUENCY)
,lt_CRM_MASTER_DATA_GENERAL_FREQUENCY        as ( select * from EXPOSE.DPULSE.CRM_MASTER_DATA_GENERAL_FREQUENCY)

        select cp_pdt.deal_id
             , cp_pdt.rental_unit_id
             , cp_pdt.rental_unit_ref_id
             , cp_pdt.transactioncurrency_id
             --, cp_pdt.territory_id
             --, ter.mdmid                                as property_complex
             , ifnull(ter.territory_id,pdt.bu_externalreference)     as territory_id --to find the Building instead of the property complex
             , subtype.profit_center_sap
             , cp_pdt.occupancystartdate                as start_date
             , cp_pdt.occupancyenddate                  as end_date
             , ifnull(cp_pdt.totalamountofrent,0)::number(17,4)                   as mgr
             , ifnull(cp_pdt.totalamountmarketingcontribution,0)::number(17,4)    as marketing
             , ifnull(cp_pdt.totalamountservicecharges,0)::number(17,4)           as service_charges
             , ifnull(DIV0(cp_pdt.primereseau, 100),0)::number(17,7)              as network_fees
             , ifnull(freq.md_frequency,'')                         as md_frequency_CP
             , dl.MD_FREQUENCY                                      as md_frequency_GENERAL
             , dl.NO_MONTHS                                         as NO_MONTHS_GENERAL
             , pdt.bu_externalreference                             as BUILDING_external_ref
             , case when ifnull(freq_gen.md_frequency,'') = '' then dl.MD_FREQUENCY
                    else ifnull(freq_gen.md_frequency,'') end as MD_FREQUENCY
             , case when ifnull(freq_gen.md_frequency,'') = '' then dl.NO_MONTHS
                    else ifnull(freq_gen.NO_MONTHS,0) end as MD_NO_MONTHS
             , cp_pdt.rentfrequencycode
             , cp_pdt.rentfrequencyname
        from            lt_crm_dealcpproduct                    as cp_pdt
        left outer join lt_CRM_DEAL_W_KEY                       as dl       on dl.deal_id                       = cp_pdt.deal_id 
        left outer join lt_crm_product                          as pdt      on pdt.rental_unit_ref_id           = cp_pdt.rental_unit_ref_id
        left outer join lt_crm_territory                        as ter      on ter.mdmid                        = pdt.bu_externalreference --ter.territory_id             = cp_pdt.territory_id                                                               
        left outer join lt_crm_productinformation               as subtype  on subtype.product_information_id   = cp_pdt.product_detail_id --//TODO  product_sub_type_id // product_detail  
        left outer join lt_CRM_MASTER_DATA_CP_FREQUENCY         as freq     on freq.md_pulse_id                 = cp_pdt.rentfrequencycode
        left outer join lt_CRM_MASTER_DATA_GENERAL_FREQUENCY    as freq_gen on freq_gen.MD_FREQUENCY            = freq.md_frequency;
