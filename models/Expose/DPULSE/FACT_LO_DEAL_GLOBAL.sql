{{ config(materialized='view') }}

with lt_deal_referential     as (select * from dpulse.crm_deal_w_key ),

--récuperation des lo
lt_leasing_order_pivot  as ( select leasing_order_id, '' as deal_id, '' as quote_id, indicator_name, indicator_amount, currency
                                ,RU_USAGE_CALC, percentage
                                , 0              as indicator_min_amount
                                , 0              as indicator_numberofmonths
                                , startdate, enddate 
                                , null           as frequency
                                , null           as calc_parameter_1
                                , null           as calc_parameter_2
                                , null           as islumpsum
                                , 'GENERAL'      as source
                                , 0              as md_pulse_id
---aho202505 ajout des dates nécessaires au calcul du nouveau champs pivot date
                                ,null as deal_creation
                                ,null as deal_financialproposalagreed
                                ,null as  deal_datevalidated
                                ,null as  deal_signaturedate
                                ,null as deal_landlordsignaturedate
                          from dpulse.crm_leasingorder_unpivot --only target
                        ),  
                        
--pivotage des indecateurs de table deal                        
lt_deal_pivot           as ( select leasing_order_id, deal_id, quote_id, indicator_name, indicator_amount, currency, RU_USAGE_CALC
                               , PROGRESSION_PERCENTAGE as percentage
                               , 0              as indicator_min_amount
                               , 0              as indicator_numberofmonths
                               , startdate, enddate
                               , null           as frequency
                               , null           as calc_parameter_1
                               , null           as calc_parameter_2
                               , null           as islumpsum
                               , 'GENERAL'      as source
                               , 0              as md_pulse_id
---aho202505
                                ,year(CREATED_ON)*100+MONTH(CREATED_ON) deal_creation
                                ,year(DATEFINANCIALPROPOSALAGREED)*100+MONTH(DATEFINANCIALPROPOSALAGREED) deal_financialproposalagreed
                                ,year(datevalidated)*100+MONTH(datevalidated) deal_datevalidated
                                ,year(signaturedate)*100+MONTH(signaturedate) deal_signaturedate
                                ,year(SIGNATUREDATE_PROCESS)*100+MONTH(SIGNATUREDATE_PROCESS) deal_landlordsignaturedate
                          from dpulse.crm_deal_unpivot ),

                          
lt_other_cond           as ( select quote_id
                               , md_grp     as indicator_name
                               , amount     as indicator_amount 
                               , currency   
                               , 'All'          as RU_USAGE_CALC
                               , null           as percentage
                               , 0              as indicator_min_amount
                               , 0              as indicator_numberofmonths
                               , payment_date   as startdate
                               , payment_date   as enddate
                               , null           as frequency
                               , null           as calc_parameter_1
                               , null           as calc_parameter_2
                               , null           as islumpsum
                               , source
                               , md_pulse_id
---aho202505
                                ,null as deal_creation
                                ,null as deal_financialproposalagreed
                                ,null as  deal_datevalidated
                                ,null as  deal_signaturedate
                                ,null as deal_landlordsignaturedate
                          from dpulse.crm_fee_w_md
                union all select quote_id
                               , md_grp     as indicator_name
                               , amount     as indicator_amount
                               , currency
                               , RU_USAGE_CALC
                               , percentage
                               , min_amount as indicator_min_amount
                               , 0          as indicator_numberofmonths
                               , null       as startdate
                               , null       as enddate
                               , frequency
                               , calc_parameter_1
                               , case when calc_parameter_1 = 'PERC_MGR' then 'WITH_SBR' else null end as calc_parameter_2
                               , islumpsum
                               , source
                               , md_pulse_id
---aho202505
                                ,null as deal_creation
                                ,null as deal_financialproposalagreed
                                ,null as  deal_datevalidated
                                ,null as  deal_signaturedate
                                ,null as deal_landlordsignaturedate
                          from dpulse.crm_charge_w_md
                union all select quote_id
                               , md_grp     as indicator_name
                               , amount     as indicator_amount
                               , currency
                               , 'All' as RU_USAGE_CALC
                               , 0                  as percentage
                               , 0                  as indicator_min_amount
                               , numberofmonths     as indicator_numberofmonths
                               , null               as startdate
                               , null               as enddate
                               , null               as frequency
                               , md_calc_parameter1 as calc_parameter_1
                               , md_calc_parameter2 as calc_parameter_2
                               , null               as islumpsum
                               , source
                               , md_pulse_id
---aho202505
                                ,null as deal_creation
                                ,null as deal_financialproposalagreed
                                ,null as  deal_datevalidated
                                ,null as  deal_signaturedate
                                ,null as deal_landlordsignaturedate
                          from dpulse.crm_guarantee_w_md ),
                          
lt_fact_wo_dl_lo        as ( select  ref.leasing_order_id, ref.deal_id
                                , fact.quote_id
                                , fact.indicator_name
                                , fact.currency
                                , fact.indicator_amount
                                , fact.RU_USAGE_CALC
                                , fact.percentage
                                , fact.indicator_min_amount
                                , fact.indicator_numberofmonths
                                --, fact.startdate
                                , case when fact.frequency = 'SIGNING_DATE'  then ref.signature_date_rpt
                                       when fact.frequency = 'DELIVERY_DATE' then ref.estimated_handover_date
                                       when fact.startdate is null           then ref.estimated_handover_date else fact.startdate end as startdate
                                , ifnull(ref.enddate,ref.estimated_handover_date) as enddate
                                , case when fact.frequency in ('SIGNING_DATE','DELIVERY_DATE') then '' else ifnull(fact.frequency,'') end as frequency
                                , fact.calc_parameter_1
                                , fact.calc_parameter_2
                                , fact.islumpsum
                                , fact.source
                                , fact.md_pulse_id
---aho202505
                                ,null as deal_creation
                                ,null as deal_financialproposalagreed
                                ,null as  deal_datevalidated
                                ,null as  deal_signaturedate
                                ,null as deal_landlordsignaturedate
                          from       lt_other_cond       as fact 
                          inner join lt_deal_referential as ref  on ref.quote_id = fact.quote_id ),

lt_deal_and_lo_pivot    as (      
select 'LO' as Object_Type, leasing_order_id, deal_id , quote_id, indicator_name, indicator_amount
                , currency, RU_USAGE_CALC, percentage, indicator_min_amount, indicator_numberofmonths
                , startdate, enddate, frequency, calc_parameter_1, calc_parameter_2, islumpsum, source, md_pulse_id
---aho202505
                                ,deal_creation
                                ,deal_financialproposalagreed
                                ,deal_datevalidated
                                ,deal_signaturedate
                                ,deal_landlordsignaturedate
                from lt_leasing_order_pivot
 union all select 'DL' as Object_Type, leasing_order_id, deal_id , quote_id, indicator_name, indicator_amount
                , currency, RU_USAGE_CALC, percentage, indicator_min_amount, indicator_numberofmonths
                , startdate, enddate, frequency, calc_parameter_1, calc_parameter_2, islumpsum, source, md_pulse_id
---aho202505
                                ,deal_creation
                                ,deal_financialproposalagreed
                                ,deal_datevalidated
                                ,deal_signaturedate
                                ,deal_landlordsignaturedate
           from lt_deal_pivot
 union all select 'DL' as Object_Type, leasing_order_id, deal_id , quote_id, indicator_name, indicator_amount
                , currency, RU_USAGE_CALC, percentage, indicator_min_amount, indicator_numberofmonths
                , startdate, enddate, frequency, calc_parameter_1, calc_parameter_2, islumpsum, source, md_pulse_id
---aho202505
                                ,deal_creation
                                ,deal_financialproposalagreed
                                ,deal_datevalidated
                                ,deal_signaturedate
                                ,deal_landlordsignaturedate
           from lt_fact_wo_dl_lo)

select  * from lt_deal_and_lo_pivot;
