{{ config(materialized='view') }}
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
		TO_CHAR(quote_createon,'YYYYMMDD') YMD_START,
		NVL(TO_CHAR(datefinancialproposalagreed,'YYYYMMDD'),'99991231') YMD_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	LEFT JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid
	WHERE --d.opportunityid='8602e810-0cb6-ee11-a569-000d3ab4b3e6' --'f1b05667-7d41-4bbc-9925-58e99d03ec9c' 
   -- d.opportunityid='8c00365f-2cc2-4ab2-98a5-de5dc4f2ce3a'
  --  d.STATECODE in ('1','0') --won et open
  -- AND
   -- and 
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
		TO_CHAR(quote_createon,'YYYYMMDD') YMD_START,
		NVL(TO_CHAR(datevalidated,'YYYYMMDD'),'99991231') YMD_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	LEFT JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid
	WHERE-- d.opportunityid='f1b05667-7d41-4bbc-9925-58e99d03ec9c'--'5b6b7053-7c73-4c7c-bd57-3fbf65bf46e9' AND
		(q.createdon< DATEADD(minute, 59, datevalidated)
		OR datevalidated IS NULL)
		AND progressiontech >= 50
		AND (statereasoncode IN ('809020006','809020008') or datevalidated IS NULL) -- premiere quote valié
        and  q.typecode !='809020000' -- pas quote fin quote fin
    --   and  d.STATECODE in ('1','0') --won et open
   
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
		TO_CHAR(quote_createon,'YYYYMMDD') YMD_START,
		NVL(TO_CHAR(datelegalagreement,'YYYYMMDD'),'99991231') YMD_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	LEFT JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid
	WHERE --d.opportunityid='f1b05667-7d41-4bbc-9925-58e99d03ec9c'--'5b6b7053-7c73-4c7c-bd57-3fbf65bf46e9' AND
		(q.createdon< DATEADD(minute, 59, datelegalagreement)
		OR datelegalagreement IS NULL)
		AND progressiontech >= 70
           and  (q.typecode not in ('809020000','809020002') OR datelegalagreement IS NULL) -- pas quote fin quote fin ni de check
      --       and  d.STATECODE in ('1','0') --won et open
	QUALIFY filter = true
    --select distinct q.name,q.typecode  from  finops.dwh.crm_quote q 
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
		TO_CHAR(quote_createon,'YYYYMMDD') YMD_START,
		NVL(TO_CHAR(signaturedate,'YYYYMMDD'),'99991231') YMD_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	LEFT JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid
	WHERE --d.opportunityid='11f976ee-4999-4f6c-beb9-12ee458a8df6'--'5b6b7053-7c73-4c7c-bd57-3fbf65bf46e9' AND 
		(q.createdon< DATEADD(minute, -1, signaturedate)
		OR signaturedate IS NULL)
		AND progressiontech >= 90
           and  (q.typecode !='809020000' or  signaturedate IS NULL) -- pas quote fin quote fin
           --   and  d.STATECODE in ('1','0') --won et open
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
		case 
            when signaturedate<quote_createon then 
            TO_CHAR(signaturedate,'YYYYMMDD')
            else 
            TO_CHAR(quote_createon,'YYYYMMDD') end YMD_START,
		NVL(TO_CHAR(LAST_DAY(signaturedate),'YYYYMMDD'),'99991231') YMD_END
		/*,progressiontech, datefinancialproposalagreed, datevalidated, datelegalagreement, d.createdon deal_createon, landlordsignaturedate, signaturedate,*/
	FROM finops.dwh.crm_deal d 
	 left JOIN finops.dwh.crm_quote q ON q.opportunityid = d.opportunityid AND 
    d.quoteid = q.quoteid
	WHERE --d.opportunityid='6d4a9d80-bd0b-ef11-9f89-000d3abaebc4' AND
		progressiontech = 100
     --     and  d.STATECODE in ('1','0') --won et open 
)
/*select 
opportunityid, ID, quoteid,fact_prog, name,quote_createon,
    YM_START,
    case when YM_END<YM_START then YM_START
    else YM_END end as YM_END,
    S_old,
    E_old
from
(*/
, GROUP_LT AS (
SELECT opportunityid, 
    ID, 
    quoteid,
    fact_prog, 
    name,
    quote_createon,
    YMD_START,
    /*case when fact_prog= 100 and lead(YMD_START) over (partition by opportunityid order by fact_prog::integer ) is null 
            then TO_CHAR(LAST_DAY(to_date(YMD_START,'YYYYMMDD')) ,'YYYYMMDD')-- dernier jour du mois pour la singature 
        else ifnull(lead(YMD_START) over (partition by opportunityid order by fact_prog::integer ),'99991231')
    end YMD_END,
    */
    YMD_END

--YM_END as E_old,
--lead(YM_START) over (partition by opportunityid order by fact_prog::integer ) next_s, 
--lag(YM_START) over (partition by opportunityid order by fact_prog::integer ) pre_s, 
/*case 
    when 
        lag(YM_END) over (partition by opportunityid order by fact_prog::integer )    is not null 
        and  lag(YM_END) over (partition by opportunityid order by fact_prog::integer ) !='999912' 
        and lag(YM_END) over (partition by opportunityid order by fact_prog::integer )>YM_START
        then lag(YM_END) over (partition by opportunityid order by fact_prog::integer ) else YM_START
    end YM_START,
case 
    when 
        lead(YM_START) over (partition by opportunityid order by fact_prog::integer )    is not null 
        and  lead(YM_START) over (partition by opportunityid order by fact_prog::integer ) !='999912' 
    then lead(YM_START) over (partition by opportunityid order by fact_prog::integer ) else YM_END
    
    end YM_END*/
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
	SELECT * FROM LT_100))
, MODIF_YMD_END AS (
SELECT q.opportunityid, 
    q.ID, 
    q.quoteid,
    fact_prog, 
    q.name,
    quote_createon,
    YMD_START,
   case 
  -- gestion des lost
        when d.STATECODE not in ('1','0') and YMD_END='99991231' and  fact_prog < max(fact_prog::number)  over (partition by d.opportunityid)
         then lead(YMD_START) over (partition by q.opportunityid order by fact_prog::integer )
        when d.STATECODE not in ('1','0') and  fact_prog = max(fact_prog::number)  over (partition by d.opportunityid) -- lost 
            then TO_CHAR(DATEADD(day,1,TO_date(YMD_START, 'YYYYMMDD')), 'YYYYMMDD')
-- other 
      when  TO_date(YMD_END, 'YYYYMMDD') > lead(YMD_START) over (partition by q.opportunityid order by fact_prog::integer ) -- fin supp au praochain start
         then lead(YMD_START) over (partition by q.opportunityid order by fact_prog::integer )
       
         when TO_date(YMD_END, 'YYYYMMDD')<to_date(YMD_START, 'YYYYMMDD') -- fin avant debut 
                  then TO_CHAR(DATEADD(day,1,YMD_START), 'YYYYMMDD')
              
     else  YMD_END
     end 
    --IFF(YMD_START = YMD_END, YMD_END, TO_CHAR(DATEADD(DAY, -1, TO_DATE(YMD_END, 'YYYYMMDD')), 'YYYYMMDD')) 
    AS YMD_END,
    /* case 
        when d.STATECODE not in ('1','0') and  fact_prog = max(fact_prog::number)  over (partition by d.opportunityid) -- lost 
            then 1
         when TO_date(YMD_END, 'YYYYMMDD')<to_date(YMD_START, 'YYYYMMDD') -- fin avant debut 
              then   2
         else  3
     end */
FROM GROUP_LT q 
join finops.dwh.crm_deal d ON q.opportunityid = d.opportunityid
) 
select 
opportunityid, 
    ID, 
    quoteid,
    fact_prog, 
    name,
    quote_createon,
    YMD_START, 
    case when to_date(YMD_END, 'YYYYMMDD')>=current_date+365 then  year(current_date)+1||'1231' else YMD_END end YMD_END
from MODIF_YMD_END;

--where q.id='Deal-00009707'
--where d.opportunityid='e5a40338-a09d-4eed-8b8c-00076b146878'
 ;

--    d.opportunityid='8602e810-0cb6-ee11-a569-000d3ab4b3e6' --'f1b05667-7d41-4bbc-9925-58e99d03ec9c' 
   -- d.opportunityid='8c00365f-2cc2-4ab2-98a5-de5dc4f2ce3a' -- e5a40338-a09d-4eed-8b8c-00076b146878