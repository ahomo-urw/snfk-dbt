create or replace view DPULSE.CRM_TERRITORY
as 
select ifnull(territoryid,'')      as territory_id
     , * exclude(territoryid)
from finops.dwh.crm_territory;