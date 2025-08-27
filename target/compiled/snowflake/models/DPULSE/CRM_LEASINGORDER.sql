
    select ifnull(leasingorderid,'')                                        as leasing_order_id
     , ifnull(transactioncurrencyid,'')                                     as transactioncurrency_id
     , ifnull(territoryid,'')                                               as territory_id
     , * exclude(leasingorderid, transactioncurrencyid, territoryid)  
    from finops.dwh.crm_leasingorder;