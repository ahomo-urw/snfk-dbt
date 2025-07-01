create or replace view EXPOSE_DEV.DPULSE.CRM_DEAL(
	scope
 ,leasing_order_id
 , deal_id
 ,quote_id_valid
 , transactioncurrency_id
 ,territory_id
 , account_id --brand
 ,tenant_id  --tenant/lessee
 , contract_id
        , ACTUALCLOSEDATE,
            ACTUALVALUE,
            DATEFINANCIALPROPOSALAGREED,
            DATELEGALAGREEMENT,
            DATEVALIDATED,
            ID,
            ISMAIN,
            LANDLORDSIGNATUREDATE,
            LEASEITREFERENCE,
            LEASINGORDERIDNAME,
            LEASINGORDERSUBTYPEID,
            LEASINGORDERSUBTYPEIDNAME,
            LEASINGORDERTYPEID,
            LEASINGORDERTYPEIDNAME,
            LEGALASSIGNMENTDATE,
            LEGALFIRSTDRAFTDATE,
            PROGRESSIONTECH,
            SCOPEID,
            SCOPEIDNAME,
            SHOWINDASHBOARD,
            SIGNATUREDATE,
            STATUSDATE,
            TERRITORYCOUNTRYID,
            UNITNAME,
            URLRECORD,
            CREATEDBYNAME,
            CREATEDON,
            CUSTOMERNEED,
            ESTIMATEDCLOSEDATE,
            ESTIMATEDVALUE,
            MODIFIEDON,
            MSDYN_FORECASTCATEGORY,
            NAME,
            ORIGINATINGLEADID,
            OWNERID,
            OWNERIDNAME,
            STAGEID,
            STATECODE,
            STATECODENAME,
            STATUSCODE,
            STATUSCODENAME,
            STEPNAME,
            ISQFR,
            STATUSCODEGROUPNAME,
            FIRSTLAUNCHVALIDATIONDATE,
            occupancystartdate,
            occupancyenddate,
            is_media_partner,
            isegenterable
            , egmanuallyexcluded  -------ASECK 14022025
            ,invoicingcontactid  -------ASECK 17032025
            ,purchaseorder  -------ASECK 17032025
            ,comment   -------ASECK 17032025) as
) comment='AHOMO 202505'
as 
select 
scopeidname as scope
 , ifnull(leasingorderid,'')            as leasing_order_id
 , ifnull(opportunityid,'')             as deal_id
 , ifnull(quoteid,'')                   as quote_id_valid
 , ifnull(transactioncurrencyid,'')     as transactioncurrency_id
 , ifnull(territoryid,'')               as territory_id
 , ifnull(customerid,'')                as account_id --brand
 , ifnull(tenantid,'')                  as tenant_id  --tenant/lessee
 , ifnull(orderid,'')                   as contract_id
, ACTUALCLOSEDATE,
    ACTUALVALUE,
    DATEFINANCIALPROPOSALAGREED,
    DATELEGALAGREEMENT,
    DATEVALIDATED,
    ID,
    ISMAIN,
    LANDLORDSIGNATUREDATE,
    LEASEITREFERENCE,
    LEASINGORDERIDNAME,
    LEASINGORDERSUBTYPEID,
    LEASINGORDERSUBTYPEIDNAME,
    LEASINGORDERTYPEID,
    LEASINGORDERTYPEIDNAME,
    LEGALASSIGNMENTDATE,
    LEGALFIRSTDRAFTDATE,
    PROGRESSIONTECH,
    SCOPEID,
    SCOPEIDNAME,
    SHOWINDASHBOARD,
    SIGNATUREDATE,
    STATUSDATE,
    TERRITORYCOUNTRYID,
    UNITNAME,
    URLRECORD,
    CREATEDBYNAME,
    CREATEDON,
    CUSTOMERNEED,
    ESTIMATEDCLOSEDATE,
    ESTIMATEDVALUE,
    MODIFIEDON,
    MSDYN_FORECASTCATEGORY,
    NAME,
    ORIGINATINGLEADID,
    OWNERID,
    OWNERIDNAME,
    STAGEID,
    STATECODE,
    STATECODENAME,
    STATUSCODE,
    STATUSCODENAME,
    STEPNAME,
    ISQFR,
    STATUSCODEGROUPNAME,
    FIRSTLAUNCHVALIDATIONDATE,
    occupancystartdate,
    occupancyenddate,
    ifnull(ismediapartner,FALSE) as is_media_partner,
    ifnull(isegenterable,TRUE)   as isegenterable
    ,ifnull(egmanuallyexcluded,FALSE) as egmanuallyexcluded  -------ASECK 14022025
    ,invoicingcontactid  -------ASECK 17032025
    ,purchaseorder  -------ASECK 17032025
    ,comment   -------ASECK 17032025
 
 --, * exclude(leasingorderid,opportunityid, quoteid, transactioncurrencyid, territoryid, customerid, orderid, tenantid)
 --, orderid
from finops.dwh.crm_deal_all;