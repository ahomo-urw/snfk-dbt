
select ifnull(territoryid,'')      as territory_id
     , * exclude(territoryid)
from finops.dwh.crm_territory;