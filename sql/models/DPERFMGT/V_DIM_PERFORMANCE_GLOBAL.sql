--SELECT count(*),product_ext_ref FROM dperfmgt.v_dim_rentalunit_performance_global group by product_ext_ref having count(*) >1;

--create or replace table dperfmgt.T_FACT_PERFORMANCE_GLOBAL as select * from  dperfmgt.V_FACT_PERFORMANCE_GLOBAL;


create or replace view dperfmgt.V_DIM_RENTALUNIT_PERFORMANCE_GLOBAL as
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


    
create     or replace view dperfmgt.V_DIM_BRAND_PERFORMANCE_GLOBAL as
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


create    or replace view dperfmgt.V_DIM_GROUP_BRAND_PERFORMANCE_GLOBAL as
select
    distinct a.parent_account,
    aparent.name GROUP_NAME
from
    DPULSE.T_DIM_ACCOUNT a
    join DPULSE.T_DIM_ACCOUNT aparent on aparent.ACCOUNT_ID = a.parent_account
where
    a.category = 'Brand';

create    or replace view dperfmgt.V_DIM_LESSEE_PERFORMANCE_GLOBAL as
select
    distinct a.account_id lessee_guid,
    a.name Name,
    a.parent_account,
    aparent.name GROUP_NAME,
    a.dunsnumber,
    a.country
from
   DPULSE.T_DIM_ACCOUNT a
    left join DPULSE.T_DIM_ACCOUNT aparent on aparent.ACCOUNT_ID = a.parent_account  
where
    a.category = 'Lessee' ;


create    or replace view dperfmgt.V_DIM_PARENT_LESSEE_PERFORMANCE_GLOBAL as
select
    distinct
    a.parent_account,
    aparent.name GROUP_NAME,
    aparent.dunsnumber,
    aparent.country
from
   DPULSE.T_DIM_ACCOUNT a
    left join DPULSE.T_DIM_ACCOUNT aparent on aparent.ACCOUNT_ID = a.parent_account  
where
    a.category = 'Lessee' and  a.parent_account is not null ;


create or replace view DPERFMGT.V_DIM_DEAL(
	SCOPE,
	PC_CODE,
	LEASING_ORDER_ID,
	DEAL_ID,
	DEAL_NO,
	QUOTE_NAME,
	QUOTE_ID,
	IS_LAST_QUOTE,
	DEAL_DESC,
	KEY_ACCOUNT_ID,
	TENANT_ID,
	BRAND_ID,
	PROGRESSION_PERCENTAGE,
	TYPE,
	TYPENAME,
	DEALSUBTYPE,
	DEALSUBTYPENAME,
	TENANT,
	PREV_LEASE,
	LEASING_MANAGER,
	CREATED_ON,
	SHOW_IN_DASHBOARD,
	SIGNATURE_DATE_RPT,
	ESTIMATED_HANDOVER_DATE,
	SIGNATUREDATE,
	ESTIMATEDSIGNATUREDATE,
	LEASEITREFERENCE,
	FIRMPERIOD,
	STATUSCODE,
	STATUS_REASON,
	BREAKOPTION,
	CONTRACT_TERMS,
	ENDDATE,
	FULLSBR,
	SBRTYPE,
	STANDARDCOMPLEMENTARY,
	DATEFINANCIALPROPOSALAGREED,
	DATEVALIDATED,
	DATELEGALAGREEMENT,
	SIGNATUREDATE_PROCESS,
	CAPONINDEXATIONOFMGR,
	HASCAPONSERVICECHARGES,
	COTENANCYCLAUSECODES,
	HASDEVIATIONSTOGREENAPPENDIX,
	HASEXCLUSIVITYCLAUSE,
	HASFREESERVICECHARGESPERIOD,
	HASFULLGREENELECTRICITYSTORE,
	ISFULLLEDSTORE,
	ISGREENAPPENDIX,
	HASNOCRYSTALLIZATIONBEFOREOPTION,
	HASPREFERENTIALRIGHTS,
	HASTURNOVERCLAUSEDEVIATION,
	VACANCYCLAUSECODES,
	KEY_FILTER_RDL,
	PULSE_SCENARIO,
	PULSE_QUARTER,
	PULSE_YEAR,
	IS_QFR_DEAL_PLANIT_NEW,
	PERF_CAT_PLANIT_NEW,
	RDL_ATTR_HAS_BEEN_FOUND,
	COUNT_RDL,
	IS_QFR_DEAL_PLANIT,
	PERF_CAT_PLANIT,
	IS_QFR_PULSE,
	ISQFR,
	STATUSCODEGROUPNAME,
	FIRSTLAUNCHVALIDATIONDATE,
	IS_MEDIA_PARTNER,
	CSDEADLINE,
	DISC_PREC_COND,
	FLGSHIP_REG,
	CSHEAVY,
	CNEWCONCP,
	PERIMETER,
	CS,
	PROJECT_CODE,
	SC_CATEGORY,
	SHOPPING_CENTER_AFFILIATES,
	CSTYPE,
	FIRMPERIODDAYSTECH,
	FIRMPERIODMONTHSTECH,
	FIRMPERIODYEARSTECH,
	ISMAIN,
	STATEREASONCODE,
	IS_CONDITION_PRECEDENT_BLOCKING,
	HAS_REAL_EFFECTIVE_DATE,
	ISDNVB,
	ISFIRSTINASHOPPINGCENTER,
	ISMARKETENTRY,
	HASFLAGSHIPFORMAT,
	ISUPSIZING,
	ISINNOVATIVECONCEPT,
	EGMANUALLYEXCLUDED,
	INVOICINGCONTACTID,
	PURCHASEORDER,
	COMMENT
) as 
select * from dpulse.t_dim_deal
union all
select
    distinct 
    'Leasing' SCOPE,
	f.PC_ID PC_CODE,
	f.LO_ID /*null*/ LEASING_ORDER_ID, --aho20250902
	f.deal_id DEAL_ID,
	f.deal_id DEAL_NO,
 

	null QUOTE_NAME,
	null QUOTE_ID,
	null IS_LAST_QUOTE,
	null DEAL_DESC,
	null KEY_ACCOUNT_ID,
	null TENANT_ID,
	null BRAND_ID,
    0 PROGRESSION_PERCENTAGE,
	lo.TYPE TYPE,--aho20250902
	lo.TYPE_NAME TYPENAME,--aho20250902
	lo.SUB_TYPE DEALSUBTYPE,--aho20250902
	lo.SUB_TYPE_NAME DEALSUBTYPENAME,--aho20250902
	null TENANT,
	null PREV_LEASE,
	null LEASING_MANAGER,
	null CREATED_ON,
    'Yes' SHOW_IN_DASHBOARD,
	f.deal_signaturedate SIGNATURE_DATE_RPT,
	null ESTIMATED_HANDOVER_DATE,
	null SIGNATUREDATE,
	f.deal_signaturedate ESTIMATEDSIGNATUREDATE,
	null LEASEITREFERENCE,
	null FIRMPERIOD,
	lo.STATUSCODE STATUSCODE, --aho20250902
	lo.STATUS_REASON_NAME STATUS_REASON, --aho20250902
	null BREAKOPTION,
	null CONTRACT_TERMS,
	null ENDDATE,
	null FULLSBR,
	null SBRTYPE,
	null STANDARDCOMPLEMENTARY,
	null DATEFINANCIALPROPOSALAGREED,
	null DATEVALIDATED,
	null DATELEGALAGREEMENT,
	null SIGNATUREDATE_PROCESS,
	null CAPONINDEXATIONOFMGR,
	null HASCAPONSERVICECHARGES,
	null COTENANCYCLAUSECODES,
	null HASDEVIATIONSTOGREENAPPENDIX,
	null HASEXCLUSIVITYCLAUSE,
	null HASFREESERVICECHARGESPERIOD,
	null HASFULLGREENELECTRICITYSTORE,
	null ISFULLLEDSTORE,
	null ISGREENAPPENDIX,
	null HASNOCRYSTALLIZATIONBEFOREOPTION,
	null HASPREFERENTIALRIGHTS,
	null HASTURNOVERCLAUSEDEVIATION,
	null VACANCYCLAUSECODES,
	null KEY_FILTER_RDL,
	null PULSE_SCENARIO,
	null PULSE_QUARTER,
	null PULSE_YEAR,
	null IS_QFR_DEAL_PLANIT_NEW,
	null PERF_CAT_PLANIT_NEW,
	null RDL_ATTR_HAS_BEEN_FOUND,
	null COUNT_RDL,
	null IS_QFR_DEAL_PLANIT,
	null PERF_CAT_PLANIT,
	null IS_QFR_PULSE,
	lo.ISQFR ISQFR,
	null STATUSCODEGROUPNAME,
	null FIRSTLAUNCHVALIDATIONDATE,
	null IS_MEDIA_PARTNER,
	null CSDEADLINE,
	null DISC_PREC_COND,
	null FLGSHIP_REG,
	null CSHEAVY,
	null CNEWCONCP,
	lo.PERIMETER PERIMETER,
	null CS,
	null PROJECT_CODE,
	null SC_CATEGORY,
	null SHOPPING_CENTER_AFFILIATES,
	null CSTYPE,
	null FIRMPERIODDAYSTECH,
	null FIRMPERIODMONTHSTECH,
	null FIRMPERIODYEARSTECH,
	null ISMAIN,
	null STATEREASONCODE,
	null IS_CONDITION_PRECEDENT_BLOCKING,
	null HAS_REAL_EFFECTIVE_DATE,
	null ISDNVB,
	null ISFIRSTINASHOPPINGCENTER,
	null ISMARKETENTRY,
	null HASFLAGSHIPFORMAT,
	null ISUPSIZING,
	null ISINNOVATIVECONCEPT,
    null EGMANUALLYEXCLUDED,
	null INVOICINGCONTACTID,
	null PURCHASEORDER,
    null 	COMMENT

from
    dperfmgt.T_FACT_PERFORMANCE_GLOBAL f
    left join DPULSE.T_DIM_LEASING_ORDER lo on f.LO_GUID= lo.LEASING_ORDER_ID --aho lien pour recuperer des carac de deal a zero
    where percentage=0

;


create    or replace view dperfmgt.V_DIM_LO_PERFORMANCE_GLOBAL as    
select
     LEASING_ORDER_ID,
	KEY_ACCOUNT_ID,
	LEASING_ORDER_DESC,
	LO_NO,
	PROGRESSION_PERCENTAGE,
	TARGET_SIGNATURE_DATE,
	TARGETEFFECTIVEDATE,
	TYPE,
	TYPE_NAME,
	SUB_TYPE,
	SUB_TYPE_NAME,
	LEASING_MANAGER,
	CREATED_ON,
	TENANT_OLD_CONTRACT,
	CURRENCY_ID,
	CURRENCY_NAME,
	STATUS_REASON,
	STATUS_REASON_NAME,
	STATUSCODE,
	STATUSCODE_NAME,
	IS_MAIN,
	PLAN_IT_OPERATION_CODE,
	OPERATION_ID,
	TARGET_CONTRACT_TERM,
	TARGET_FIRM_PERIOD,
	HAS_DEALS,
	KEY_FILTER_RDL,
	PULSE_SCENARIO,
	PULSE_YEAR,
	PULSE_QUARTER,
	IS_QFR_PULSE,
	IS_QFR_DEAL_PLANIT,
	IS_QFR_DEAL_PLANIT_NEW,
	ISQFR,
	RDL_ATTR_HAS_BEEN_FOUND,
	PERF_CAT_PLANIT,
	PERF_CAT_PLANIT_NEW,
	COUNT_RDL,
	CSDEADLINE,
	DISC_PREC_COND,
	FLGSHIP_REG,
	CSHEAVY,
	CNEWCONCP,
	PERIMETER,
	CS,
	PROJECT_CODE,
	SC_CATEGORY,
	SHOPPING_CENTER_AFFILIATES,
	CSTYPE,
	MAIN_LO,
	POTENTIELRETAILERID,
	POTENTIALRETAILERNAME,
	OWNERIDNAME
from
  -- dperfmgt.T_FACT_PERFORMANCE_GLOBAL fact
 DPULSE.T_DIM_LEASING_ORDER lo ;


create    or replace view dperfmgt.V_DIM_CONTRACT_PERFORMANCE_GLOBAL as    
select extref CONTRACT_ID, name CONTRACT_NAME, startdate, enddate  , signing_date,next_break_option
from dpulse.t_dim_contract;


create    or replace view dperfmgt.V_DIM_PC_PERFORMANCE_GLOBAL as   
select   distinct pat.pc_code pc_ID
            , nvl(pc.pc_commercial_name,pc.pc_name) NAME
            , pat.country_code country_code 
            , (PCCAT_LABEL) CATEGORY 
            , pat.pc_code CURRENCY
             , (pc.pc_address) ADDRESS
             , (pc.pc_city) CITY
             , (pc.id_urwconnect) CONNECT_ID,
             pat.sector_code sector_code
            , (pat.PC_EXTREF)  PC_EXTREF
             , (case when is3rd=1 then true else false end) isthirdparty
        from finops.CRM.td_MDM_PULSE_PATRIMONY_ALIVE_ONETOOL_v2 pat 
        left join FINOPS.DWH.VD_PROPERTYCOMPLEX_3RDPARTY PARTY on pat.pc_id=party.pc_id
        left join rawdata.mercury.shoppingcentre sc on current_date between datdebmdt and nvl(datfinmdt,current_date) and pat.pc_extref=sc.pc_extref and pat.si_id = sc.es_id
        left join eudwh_prod.odsdwh_user.mdm_propertycomplex pc on pat.pc_id = pc.pc_id


--GRANT ROLE rsch_ro_crm_finops TO ROLE RUSER_SVC_POWERBI_PROD;
--GRANT ROLE RSCH_RO_WORKDAY_RAWDATA_DEV TO ROLE RSCH_RW_CPULSE_EXPOSE_DEV;

--desc   FINOPS.DWH;