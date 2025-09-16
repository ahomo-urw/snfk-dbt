create or replace view DPULSE.CRM_ORDER(
	lessee_id,
	ENDDATE,
	STARTDATE,
	EXTREF,
	customer_id,
	NAME,
	contract_id,
	transactioncurrency_id,
	TRANSACTIONCURRENCYIDNAME,
	SIGNATUREDATE,
	OPTIONDATE,
	ACCOUNTLESSEEIDNAME,
	STOREID,
	STOREIDNAME,
	TERRITORYID,
	TERRITORYIDNAME,
	PARKINGMGR,
	RETAILMGR,
	SEPARATEDSTORAGEMGR,
	INVOICEDSBR,
	CUSTOMERIDNAME,
	EXPIRYDATE,
	RENTREVIEWDATE,
	ENDOCCUPANCYDATE,
	SCOPEIDNAME,
	UNITNAME,
	STATECODE,
	STATECODENAME,
	STATUSCODE,
	STATUSCODENAME,
	OPPORTUNITYID,
	DOOHCAMPAIGNID,
	ISDRIVETOSTORE
) as 
select   
        ifnull(accountlesseeid,'')             as lessee_id
       ,enddate
       ,startdate
       ,extref
       , ifnull(customerid,'')                  as customer_id
       ,name
        ,ifnull(salesorderid,'')                as contract_id
       , ifnull(transactioncurrencyid,'')       as transactioncurrency_id
       , * exclude(accountlesseeid,enddate,startdate,extref ,customerid,name,salesorderid,transactioncurrencyid) 
from finops.dwh.crm_order;