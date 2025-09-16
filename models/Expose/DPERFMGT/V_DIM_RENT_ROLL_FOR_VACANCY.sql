{{ config(materialized='view') }}
select
scenarios_rr,
key_r,
dim.cpropc PC_ID,
replace(rebuild,'-','/') BU_EXTREF,
replace ( ifnull(CRENTSPAC_O,dim.crentspac) ,'#','-') PRODUCT_EXT_REF,
    left(dim.cutoffcalmonth+100 ,4)||'01' YM_START ,
    left(dim.cutoffcalmonth+100 ,4)||'12'  YM_END ,
--clease,
clease1_text Contract_ID,
	CUTOFFCALMONTH,
	IS_VALIDATED,
	SOURCEOFLEASERS,
	STATUS,
	CONSOPCT,
	OLD_CTENANT,
	CTENANT,
	dim.COPERAT,
	COPETYPE,
	OCCUPANCYSTARTDATE,
	OCCUPANCYENDDATE,
	dim.CUSAGETYP,
	CONTRSTART,
	TERMDATE,
	CDNXBKOP,
	PREVREVDATE,
	NEXTREVDATE,
	CURRIDXTYPE,
	PERIODREF,
	VACTYPE,
	COMMENTS,
	RSSTRUCTSTATUS,
	RSORIGIN,
	POTTRANS,
	AREACHGEL,
	GLAUNIT,
	KGLA,
	TERMDURATION,
	FIRMTERMDURATION,
	CURRINDEXVAL,
	COUNTED,
	NEXT_OPE  
from EXPOSE.DPERFMGT.PLANIT_OPERATION_DIM_RENT_ROLL_FOR_VACANCY_AGG dim 
 join  expose.DPERFMGT.planit_transverse_table_refresh_scope  scope 
    on dim.cscenario=scope.cscenario 
    and dim.cversion=scope.cversion
    and service='WORKLOAD'
    and dim.cutoffcalmonth = to_char(to_date(datefrom,'YYYYMMDD'),'YYYYMM')
    and is_validated='X'
left join 
    (select ru_ref.cscenario , ru_ref.cversion,  ru_ref.crentspac  crentspac_N,ru_old.crentspac crentspac_O
    from EXPOSE.DPERFMGT.PLANIT_OPERATION_TABLE_RENTAL_UNIT ru_ref 
     join  EXPOSE.DPERFMGT.PLANIT_OPERATION_TABLE_RENTAL_UNIT ru_old
        on ru_old.coperat=ru_ref.coperat2
        and ru_old.cscenario=ru_ref.cscenario 
        and ru_old.cversion=ru_ref.cversion
         and ru_old.cpropc=ru_ref.cpropc
    join  expose.DPERFMGT.planit_transverse_table_refresh_scope  scope 
        on ru_ref.cscenario=scope.cscenario 
        and ru_ref.cversion=scope.cversion
        and service='WORKLOAD'
    where ru_ref.crentspac!=ru_old.crentspac
    and ru_ref.coperat2 !=''
    --and ru_ref.crentspac in ('RSC27219')
    ) RS_ORI on RS_ORI.crentspac_N=dim.crentspac
            and RS_ORI.cscenario=dim.cscenario
            and RS_ORI.cversion=dim.cversion
    AND COUNTED ='Yes';
;