
  create or replace   view EXPOSE_DEV.DPULSE.CRM_PRODUCTINFORMATION
  
   as (
    

SELECT
  IFNULL(id, '') AS product_information_id,
  IFNULL(profitcentersapidname, '') AS profit_center_sap,
  *
EXCLUDE(id, profitcentersapidname)
FROM finops.dwh.CRM_PRODUCTINFORMATION
  );

