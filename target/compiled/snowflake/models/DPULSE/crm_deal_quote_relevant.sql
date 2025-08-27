

WITH LT_10 AS (
	SELECT 
		d.opportunityid, 
		d.id,
		q.quoteid,
		'10' fact_prog,
		q.name,
		q.createdon quote_createon,
		CASE WHEN MAX(q.createdon) OVER (PARTITION BY d.opportunityid) = q.createdon 
			THEN true 
			ELSE false 
		END filter,
		TO_CHAR(quote_createon,'YYYYMM') YM_START,
		NVL(TO_CHAR(datefinancialproposalagreed,'YYYYMM'),'999912') YM_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	LEFT JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid
	WHERE --d.opportunityid='8602e810-0cb6-ee11-a569-000d3ab4b3e6' --'f1b05667-7d41-4bbc-9925-58e99d03ec9c' 
    --and 
    q.typecode ='809020000' --quote fin
    AND 
		(q.createdon< DATEADD(minute, 59, datefinancialproposalagreed ) 
		OR datefinancialproposalagreed IS NULL)
	QUALIFY filter = true
)
, LT_50 AS (
	SELECT 
		d.opportunityid, 
		d.id,
		q.quoteid,
		'50' fact_prog,
		q.name,
		q.createdon quote_createon,
		CASE WHEN min(q.createdon) OVER (PARTITION BY d.opportunityid) = q.createdon
			THEN true 
			ELSE false 
		END filter,
		TO_CHAR(quote_createon,'YYYYMM') YM_START,
		NVL(TO_CHAR(datevalidated,'YYYYMM'),'999912') YM_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	LEFT JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid
	WHERE-- d.opportunityid='f1b05667-7d41-4bbc-9925-58e99d03ec9c'--'5b6b7053-7c73-4c7c-bd57-3fbf65bf46e9' AND
		(q.createdon< DATEADD(minute, 59, datevalidated)
		OR datevalidated IS NULL)
		AND progressiontech >= 50
		AND statereasoncode IN ('809020006','809020008') -- premiere quote valié
        and  q.typecode !='809020000' -- pas quote fin quote fin
	QUALIFY filter = true
)
, LT_70 AS (
	SELECT 
		d.opportunityid, 
		d.id,
		q.quoteid,
		'70' fact_prog,
		q.name,
		q.createdon quote_createon,
		CASE WHEN max(q.createdon) OVER (PARTITION BY d.opportunityid) = q.createdon 
			THEN true 
			ELSE false 
		END filter,
		TO_CHAR(quote_createon,'YYYYMM') YM_START,
		NVL(TO_CHAR(datelegalagreement,'YYYYMM'),'999912') YM_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	LEFT JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid
	WHERE --d.opportunityid='f1b05667-7d41-4bbc-9925-58e99d03ec9c'--'5b6b7053-7c73-4c7c-bd57-3fbf65bf46e9' AND
		(q.createdon< DATEADD(minute, 59, datelegalagreement)
		OR datelegalagreement IS NULL)
		AND progressiontech >= 70
           and  q.typecode !='809020000' -- pas quote fin quote fin
	QUALIFY filter = true
)
, LT_90 AS (
	SELECT 
		d.opportunityid, 
		d.id,
		q.quoteid,
		'90' fact_prog,
		q.name,
		q.createdon quote_createon,
		CASE WHEN max(q.createdon) OVER (PARTITION BY d.opportunityid) = q.createdon 
			THEN true 
			ELSE false 
		END filter,
		TO_CHAR(quote_createon,'YYYYMM') YM_START,
		NVL(TO_CHAR(signaturedate,'YYYYMM'),'999912') YM_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	LEFT JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid
	WHERE --d.opportunityid='11f976ee-4999-4f6c-beb9-12ee458a8df6'--'5b6b7053-7c73-4c7c-bd57-3fbf65bf46e9' AND 
		(q.createdon< DATEADD(minute, -1, signaturedate)
		OR signaturedate IS NULL)
		AND progressiontech >= 90
           and  q.typecode !='809020000' -- pas quote fin quote fin
	QUALIFY filter = true
)
, LT_100 AS (
	SELECT 
		d.opportunityid, 
		d.id,
		q.quoteid,
		'100' fact_prog,
		q.name,
		q.createdon quote_createon,
		true filter,
		TO_CHAR(quote_createon,'YYYYMM') YM_START,
		NVL(TO_CHAR(signaturedate,'YYYYMM'),'999912') YM_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	 left JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid AND 
    d.quoteid = q.quoteid
	WHERE --d.opportunityid='6d4a9d80-bd0b-ef11-9f89-000d3abaebc4' AND
		progressiontech = 100
)
SELECT * exclude(filter)
FROM
(
	SELECT * FROM LT_10
	UNION ALL
	SELECT * FROM LT_50
	UNION ALL
	SELECT * FROM LT_70
	UNION ALL
	SELECT * FROM LT_90
	UNION ALL
	SELECT * FROM LT_100)