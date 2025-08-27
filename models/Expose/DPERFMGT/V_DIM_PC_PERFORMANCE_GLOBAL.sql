{{ config(materialized='view') }}
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
