
  create or replace   view EXPOSE_DEV.DPULSE.CRM_PRODUCTINFORMATION
  
   as (
    
    select ifnull(id,'')                                                    as product_information_id --product_sub_type_id 
         , ifnull(profitcentersapidname,'')                                 as profit_center_sap
         , * exclude(id, profitcentersapidname) 
    from finops.dwh.crm_productinformation;
  );

