
  create or replace   view EXPOSE_DEV.DPULSE.PLANIT_FINANCE_DIM_ENTITY
  
   as (
    


with lt_entity 			as ( select * from rawdata.planit.planit_finance_dim_entity ),
     lt_property        as ( select * from EXPOSE.DPERFMGT.PLANIT_FINANCE_HIER_FLATTENED_AND_PROPERTIES where dim = 'ENTITY' and type in ('P') ),
     lt_regional_scope  as ( select base_level, level1 as SPLIT_AIRPORT, level2 as continent, level3 as sub_continent, level4 as supra_region_qfr, level5 as region_qfr, level6 as country
                             from lt_property where hier_id = 'REGION_SECTION' ),
     lt_regional_4      as ( select base_level, level3 as supra_region_4  
                             from lt_property where hier_id = 'REGION_SECTION_4' ),
     lt_asset_grouping  as ( select base_level, level1 as CONSO_AFFILIATES, level2 as FLAGSHIP_REGIONAL, level3 as CBD_US  
                             from lt_property where hier_id = 'ASSET_GROUPING' ),
	 lt_additional_properties as ( 
        select obj_def.appset_id
        	 , obj_def.application_id
             , add_prop.dim
             , add_prop.name
             , add_prop.object_name
             , obj_baseL.base_level
             , add_prop.text
             , add_prop.caldate
             , add_prop.amount 					
        from 		expose.dperfmgt.PLANIT_TRANSVERSE_TABLE_ADD_PROPERTIES  			as add_prop
        inner join 	expose.dperfmgt.PLANIT_FINANCE_TABLE_OBJECT_DEFINITION				as obj_def
        	on  obj_def.dimension   = add_prop.dim
            and obj_def.object_name = add_prop.object_name
        inner join 	expose.dperfmgt.PLANIT_FINANCE_TABLE_OBJECT_DEFINITION_BASE_LEVEL	as obj_baseL
        	on  obj_baseL.appset_id			= obj_def.appset_id
            --and obj_baseL.application_id 	= obj_def.application_id
            and obj_baseL.dimension_name	= obj_def.dimension
            and obj_baseL.seq_def			= obj_def.seq
        where add_prop.dim = 'ENTITY'
    ),
    lt_add_prop_text    as ( select appset_id, name, base_level, text    from lt_additional_properties where text <> ''            ),
    lt_add_prop_caldate as ( select appset_id, name, base_level, caldate from lt_additional_properties where caldate not in ('00000000','') ),
    lt_add_prop_amount  as ( select appset_id, name, base_level, amount  from lt_additional_properties where amount <> 0 		  ),

    lt_add_prop_text_pivot as (
		select  appset_id, base_level
            ,"'5YBP_2023_2027 primary Grouping'" as "5YBP_2023_2027_primary_Grouping_TXT"
            ,"'A22 vs A19'" as "A22_vs_A19_TXT"
            ,"'A22 vs A21'" as "A22_vs_A21_TXT"
            ,"'A22 vs B22'" as "A22_vs_B22_TXT"
            ,"'A22 vs FC3 22'" as "A22_vs_FC3_22_TXT"
            ,"'A23 Q1 vs A22 Q1'" as "A23_Q1_vs_A22_Q1_TXT"
            ,"'B23 vs A19 Grouping'" as "B23_vs_A19_Grouping_TXT"
            ,"'B23 vs P23 Grouping'" as "B23_vs_P23_Grouping_TXT"
            ,"'Business Unit Grouping'" as "Business_Unit_Grouping_TXT"
            ,"'Consolidation Method After Disposal FC3 22 5YBP_2023_2027'" as "Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_TXT"
            ,"'Disposal Date FC3 22 5YBP_2023_2027'" as "Disposal_Date_FC3_22_5YBP_2023_2027_TXT"
            , "'FC1 23 vs B23'" as "FC1_23_vs_B23_TXT"
            ,"'Disposals BP23-27 vs BP22-26'" as "Disposals_BP23-27_vs_BP22-26_TXT"
            ,"'FC1 23 vs A22'" as "FC1_23_vs_A22_TXT"
            ,"'P24 vs A19 Grouping'" as "P24_vs_A19_Grouping_TXT"
            ,"'PIPELINE STATUS A22 Q4'" as "PIPELINE_STATUS_A22_Q4_TXT"
            ,"'PIPELINE STATUS FC1_23'" as "PIPELINE_STATUS_FC1_23_TXT"
            ,"'Disposal Leasing 2019_2023'"      as "Disposal_Leasing_2019_2023_TXT"
            ,"'Disposal Leasing 2019_2022'"      as "Disposal_Leasing_2019_2022_TXT"
            ,"'A23 H1 vs A22 H1'"                as "A23_H1_vs_A22_H1_TXT"
            ,"'A23 vs A19 Grouping'"             as "A23_vs_A19_Grouping_TXT"
            ,"'A23 vs A19 FY Grouping'"          as "A23_vs_A19_FY_Grouping_TXT"
            ,"'PIPELINE STATUS FC2_23'"          as "PIPELINE_STATUS_FC2_23_TXT"
            ,"'GE/AT Asset Segmentation'"        as "GE_AT_Asset_Segmentation_TXT"
            ,"'FC3 23 vs A19 FY Grouping'"       as "FC3_23_vs_A19_FY_Grouping_TXT"
            ,"'B24 vs A19 FY Grouping'"          as "B24_vs_A19_FY_Grouping_TXT"
            ,"'B24 vs P24 Grouping'"             as "B24_vs_P24_Grouping_TXT"
            ,"'B24 vs FC3 23 FY Grouping'"       as "B24_vs_FC3_23_FY_Grouping_TXT"
            ,"'P25 vs B24 FY Grouping'"          as "P25_vs_B24_FY_Grouping_TXT"
            ,"'FC3 23 vs A22 FY Grouping'"       as "FC3_23_vs_A22_FY_Grouping_TXT"
            ,"'5YBP_2024_2028 primary Grouping'" as "5YBP_2024_2028_primary_Grouping_TXT"
            ,"'CEE incl or excl Hamburg'"        as "CEE_incl_or_excl_Hamburg_TXT"
            ,"'Disposal Group'"                  as "Disposal_Group_TXT"

            
            
            
        from lt_add_prop_text
        pivot(max(TEXT) for name in ('5YBP_2023_2027 primary Grouping','A22 vs A19','A22 vs A21','A22 vs B22','A22 vs FC3 22','A23 Q1 vs A22 Q1','B23 vs A19 Grouping','B23 vs P23 Grouping','Business Unit Grouping','Consolidation Method After Disposal FC3 22 5YBP_2023_2027'
        ,'Disposal Date FC3 22 5YBP_2023_2027'
        , 'FC1 23 vs B23'
        ,'Disposals BP23-27 vs BP22-26','FC1 23 vs A22','P24 vs A19 Grouping','PIPELINE STATUS A22 Q4'
        ,'PIPELINE STATUS FC1_23'
        ,'Disposal Leasing 2019_2023'
        ,'Disposal Leasing 2019_2022'
        ,'A23 H1 vs A22 H1'
        ,'A23 vs A19 Grouping'
        ,'A23 vs A19 FY Grouping'
        ,'PIPELINE STATUS FC2_23'
        ,'GE/AT Asset Segmentation'
        ,'FC3 23 vs A19 FY Grouping'
        ,'B24 vs A19 FY Grouping'
        ,'B24 vs P24 Grouping'
        ,'B24 vs FC3 23 FY Grouping'
        ,'P25 vs B24 FY Grouping'
        ,'FC3 23 vs A22 FY Grouping'
        ,'5YBP_2024_2028 primary Grouping'
        ,'CEE incl or excl Hamburg'
        ,'Disposal Group'

))
    ),
	lt_add_prop_caldate_pivot as (
		select  appset_id, base_level
            ,"'5YBP_2023_2027 primary Grouping'" as "5YBP_2023_2027_primary_Grouping_DATE"
            ,"'A22 vs A19'" as "A22_vs_A19_DATE"
            ,"'A22 vs A21'" as "A22_vs_A21_DATE"
            ,"'A22 vs B22'" as "A22_vs_B22_DATE"
            ,"'A22 vs FC3 22'" as "A22_vs_FC3_22_DATE"
            ,"'A23 Q1 vs A22 Q1'" as "A23_Q1_vs_A22_Q1_DATE"
            ,"'B23 vs A19 Grouping'" as "B23_vs_A19_Grouping_DATE"
            ,"'B23 vs P23 Grouping'" as "B23_vs_P23_Grouping_DATE"
            ,"'Business Unit Grouping'" as "Business_Unit_Grouping_DATE"
            ,"'Consolidation Method After Disposal FC3 22 5YBP_2023_2027'" as "Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_DATE"
            ,"'Disposal Date FC3 22 5YBP_2023_2027'" as "Disposal_Date_FC3_22_5YBP_2023_2027_DATE"
            ,"'FC1 23 vs B23'" as "FC1_23_vs_B23_DATE"
            ,"'Disposals BP23-27 vs BP22-26'" as "Disposals_BP23-27_vs_BP22-26_DATE"
            ,"'FC1 23 vs A22'" as "FC1_23_vs_A22_DATE"
            ,"'P24 vs A19 Grouping'" as "P24_vs_A19_Grouping_DATE"
            ,"'PIPELINE STATUS A22 Q4'" as "PIPELINE_STATUS_A22_Q4_DATE"
            ,"'PIPELINE STATUS FC1_23'" as "PIPELINE_STATUS_FC1_23_DATE"
            ,"'PIPELINE STATUS FC2_23'" as "PIPELINE_STATUS_FC2_23_DATE"
        from lt_add_prop_caldate
        pivot(max(CALDATE) for name in ('5YBP_2023_2027 primary Grouping','A22 vs A19','A22 vs A21','A22 vs B22','A22 vs FC3 22','A23 Q1 vs A22 Q1','B23 vs A19 Grouping','B23 vs P23 Grouping','Business Unit Grouping','Consolidation Method After Disposal FC3 22 5YBP_2023_2027'
        ,'Disposal Date FC3 22 5YBP_2023_2027'
        ,'FC1 23 vs B23'
        ,'Disposals BP23-27 vs BP22-26','FC1 23 vs A22','P24 vs A19 Grouping','PIPELINE STATUS A22 Q4'
        ,'PIPELINE STATUS FC1_23'
        ,'PIPELINE STATUS FC2_23'
))
    ),
	lt_add_prop_amount_pivot as (
		select  appset_id, base_level
            ,"'5YBP_2023_2027 primary Grouping'" as "5YBP_2023_2027_primary_Grouping_AMNT"
            ,"'A22 vs A19'" as "A22_vs_A19_AMNT"
            ,"'A22 vs A21'" as "A22_vs_A21_AMNT"
            ,"'A22 vs B22'" as "A22_vs_B22_AMNT"
            ,"'A22 vs FC3 22'" as "A22_vs_FC3_22_AMNT"
            ,"'A23 Q1 vs A22 Q1'" as "A23_Q1_vs_A22_Q1_AMNT"
            ,"'B23 vs A19 Grouping'" as "B23_vs_A19_Grouping_AMNT"
            ,"'B23 vs P23 Grouping'" as "B23_vs_P23_Grouping_AMNT"
            ,"'Business Unit Grouping'" as "Business_Unit_Grouping_AMNT"
            ,"'Consolidation Method After Disposal FC3 22 5YBP_2023_2027'" as "Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_AMNT"
            ,"'Disposal Date FC3 22 5YBP_2023_2027'" as "Disposal_Date_FC3_22_5YBP_2023_2027_AMNT"
            ,"'FC1 23 vs B23'" as "FC1_23_vs_B23_AMNT"
            ,"'Disposals BP23-27 vs BP22-26'" as "Disposals_BP23-27_vs_BP22-26_AMNT"
            ,"'FC1 23 vs A22'" as "FC1_23_vs_A22_AMNT"
            ,"'P24 vs A19 Grouping'" as "P24_vs_A19_Grouping_AMNT"
            ,"'PIPELINE STATUS A22 Q4'" as "PIPELINE_STATUS_A22_Q4_AMNT"

        from lt_add_prop_amount
        pivot(max(amount) for name in ('5YBP_2023_2027 primary Grouping','A22 vs A19','A22 vs A21','A22 vs B22','A22 vs FC3 22','A23 Q1 vs A22 Q1','B23 vs A19 Grouping','B23 vs P23 Grouping','Business Unit Grouping','Consolidation Method After Disposal FC3 22 5YBP_2023_2027'
        ,'Disposal Date FC3 22 5YBP_2023_2027'
        ,'FC1 23 vs B23'
        ,'Disposals BP23-27 vs BP22-26','FC1 23 vs A22','P24 vs A19 Grouping','PIPELINE STATUS A22 Q4'
)) ) 

select ent.*
     , entity || ' - ' || evdescription as entity_id_desc
     , p_cpx || ' - ' || p_cpx_desc as property_complex_id_desc
     , ent.region || ' - ' || ent.region_desc as region_id_desc


     , asset_grp.CONSO_AFFILIATES
     , asset_grp.FLAGSHIP_REGIONAL
     , asset_grp.CBD_US
     --, asset_grouping.CONSO_AFFILIATES
     --, asset_grouping.FLAGSHIP_REGIONAL
     --, asset_grouping.CBD_US
     
     , ifnull(reg_scp.SPLIT_AIRPORT,'Other')        as URW_GROUP_AIR_SPLIT
     , ifnull(reg_scp.continent,'Other')            as CONTINENT
     , ifnull(reg_scp.sub_continent,'Other')        as SUBCONTINENT
     , ifnull(reg_scp.supra_region_qfr,'Other')     as SUPRA_REGION_QFR
--     , ifnull(reg_4.supra_region_4,'Other')         as SUPRA_REGION_4
     , ifnull(reg_scp.region_qfr,'Other')           as REGION_QFR
     , ifnull(reg_scp.country,'Other')              as country
     
     , asset_grp.CONSO_AFFILIATES                   as CONSO_AFFILIATES2
     , asset_grp.FLAGSHIP_REGIONAL                  as FLAGSHIP_REGIONAL2
     , asset_grp.CBD_US                             as CBD_US2

,ifnull(addprop_txt."5YBP_2023_2027_primary_Grouping_TXT",'Other') as "5YBP_2023_2027_primary_Grouping_TXT"
,decode(addprop_date."5YBP_2023_2027_primary_Grouping_DATE",'00000000',null,null,null,to_date(addprop_date."5YBP_2023_2027_primary_Grouping_DATE",'YYYYMMDD')) as "5YBP_2023_2027_primary_Grouping_DATE"
,ifnull(addprop_amnt."5YBP_2023_2027_primary_Grouping_AMNT",0) as "5YBP_2023_2027_primary_Grouping_AMNT"
,ifnull(addprop_txt."A22_vs_A19_TXT",'Other') as "A22_vs_A19_TXT"
,decode(addprop_date."A22_vs_A19_DATE",'00000000',null,null,null,to_date(addprop_date."A22_vs_A19_DATE",'YYYYMMDD')) as "A22_vs_A19_DATE"
,ifnull(addprop_amnt."A22_vs_A19_AMNT",0) as "A22_vs_A19_AMNT"
,ifnull(addprop_txt."A22_vs_A21_TXT",'Other') as "A22_vs_A21_TXT"
,decode(addprop_date."A22_vs_A21_DATE",'00000000',null,null,null,to_date(addprop_date."A22_vs_A21_DATE",'YYYYMMDD')) as "A22_vs_A21_DATE"
,ifnull(addprop_amnt."A22_vs_A21_AMNT",0) as "A22_vs_A21_AMNT"
,ifnull(addprop_txt."A22_vs_B22_TXT",'Other') as "A22_vs_B22_TXT"
,decode(addprop_date."A22_vs_B22_DATE",'00000000',null,null,null,to_date(addprop_date."A22_vs_B22_DATE",'YYYYMMDD')) as "A22_vs_B22_DATE"
,ifnull(addprop_amnt."A22_vs_B22_AMNT",0) as "A22_vs_B22_AMNT"
,ifnull(addprop_txt."A22_vs_FC3_22_TXT",'Other') as "A22_vs_FC3_22_TXT"
,decode(addprop_date."A22_vs_FC3_22_DATE",'00000000',null,null,null,to_date(addprop_date."A22_vs_FC3_22_DATE",'YYYYMMDD')) as "A22_vs_FC3_22_DATE"
,ifnull(addprop_amnt."A22_vs_FC3_22_AMNT",0) as "A22_vs_FC3_22_AMNT"
,ifnull(addprop_txt."A23_Q1_vs_A22_Q1_TXT",'Other') as "A23_Q1_vs_A22_Q1_TXT"
,decode(addprop_date."A23_Q1_vs_A22_Q1_DATE",'00000000',null,null,null,to_date(addprop_date."A23_Q1_vs_A22_Q1_DATE",'YYYYMMDD')) as "A23_Q1_vs_A22_Q1_DATE"
,ifnull(addprop_amnt."A23_Q1_vs_A22_Q1_AMNT",0) as "A23_Q1_vs_A22_Q1_AMNT"
,ifnull(addprop_txt."B23_vs_A19_Grouping_TXT",'Other') as "B23_vs_A19_Grouping_TXT"
,decode(addprop_date."B23_vs_A19_Grouping_DATE",'00000000',null,null,null,to_date(addprop_date."B23_vs_A19_Grouping_DATE",'YYYYMMDD')) as "B23_vs_A19_Grouping_DATE"
,ifnull(addprop_amnt."B23_vs_A19_Grouping_AMNT",0) as "B23_vs_A19_Grouping_AMNT"
,ifnull(addprop_txt."B23_vs_P23_Grouping_TXT",'Other') as "B23_vs_P23_Grouping_TXT"
,decode(addprop_date."B23_vs_P23_Grouping_DATE",'00000000',null,null,null,to_date(addprop_date."B23_vs_P23_Grouping_DATE",'YYYYMMDD')) as "B23_vs_P23_Grouping_DATE"
,ifnull(addprop_amnt."B23_vs_P23_Grouping_AMNT",0) as "B23_vs_P23_Grouping_AMNT"
,ifnull(addprop_txt."Business_Unit_Grouping_TXT",'Other') as "Business_Unit_Grouping_TXT"
,decode(addprop_date."Business_Unit_Grouping_DATE",'00000000',null,null,null,to_date(addprop_date."Business_Unit_Grouping_DATE",'YYYYMMDD')) as "Business_Unit_Grouping_DATE"
,ifnull(addprop_amnt."Business_Unit_Grouping_AMNT",0) as "Business_Unit_Grouping_AMNT"
,ifnull(addprop_txt."Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_TXT",'Other') as "Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_TXT"
,decode(addprop_date."Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_DATE",'00000000',null,null,null,to_date(addprop_date."Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_DATE",'YYYYMMDD')) as "Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_DATE"
,ifnull(addprop_amnt."Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_AMNT",0) as "Consolidation_Method_After_Disposal_FC3_22_5YBP_2023_2027_AMNT"

,ifnull(addprop_txt."Disposal_Date_FC3_22_5YBP_2023_2027_TXT",'Other') as "Disposal_Date_FC3_22_5YBP_2023_2027_TXT"
,decode(addprop_date."Disposal_Date_FC3_22_5YBP_2023_2027_DATE",'00000000',null,null,null,to_date(addprop_date."Disposal_Date_FC3_22_5YBP_2023_2027_DATE",'YYYYMMDD')) as "Disposal_Date_FC3_22_5YBP_2023_2027_DATE"
,ifnull(addprop_amnt."Disposal_Date_FC3_22_5YBP_2023_2027_AMNT",0) as "Disposal_Date_FC3_22_5YBP_2023_2027_AMNT"

,ifnull(addprop_txt."FC1_23_vs_B23_TXT",'Other') as "FC1_23_vs_B23_TXT"
,decode(addprop_date."FC1_23_vs_B23_DATE",'00000000',null,null,null,to_date(addprop_date."FC1_23_vs_B23_DATE",'YYYYMMDD')) as "FC1_23_vs_B23_DATE"
,ifnull(addprop_amnt."FC1_23_vs_B23_AMNT",0) as "FC1_23_vs_B23_AMNT"

,ifnull(addprop_txt."Disposals_BP23-27_vs_BP22-26_TXT",'Other') as "Disposals_BP23-27_vs_BP22-26_TXT"
,decode(addprop_date."Disposals_BP23-27_vs_BP22-26_DATE",'00000000',null,null,null,to_date(addprop_date."Disposals_BP23-27_vs_BP22-26_DATE",'YYYYMMDD')) as "Disposals_BP23-27_vs_BP22-26_DATE"
,ifnull(addprop_amnt."Disposals_BP23-27_vs_BP22-26_AMNT",0) as "Disposals_BP23-27_vs_BP22-26_AMNT"
,ifnull(addprop_txt."FC1_23_vs_A22_TXT",'Other') as "FC1_23_vs_A22_TXT"
,decode(addprop_date."FC1_23_vs_A22_DATE",'00000000',null,null,null,to_date(addprop_date."FC1_23_vs_A22_DATE",'YYYYMMDD')) as "FC1_23_vs_A22_DATE"
,ifnull(addprop_amnt."FC1_23_vs_A22_AMNT",0) as "FC1_23_vs_A22_AMNT"
,ifnull(addprop_txt."P24_vs_A19_Grouping_TXT",'Other') as "P24_vs_A19_Grouping_TXT"
,decode(addprop_date."P24_vs_A19_Grouping_DATE",'00000000',null,null,null,to_date(addprop_date."P24_vs_A19_Grouping_DATE",'YYYYMMDD')) as "P24_vs_A19_Grouping_DATE"
,ifnull(addprop_amnt."P24_vs_A19_Grouping_AMNT",0) as "P24_vs_A19_Grouping_AMNT"
,ifnull(addprop_txt."PIPELINE_STATUS_A22_Q4_TXT",'Other') as "PIPELINE_STATUS_A22_Q4_TXT"
,decode(addprop_date."PIPELINE_STATUS_A22_Q4_DATE",'00000000',null,null,null,to_date(addprop_date."PIPELINE_STATUS_A22_Q4_DATE",'YYYYMMDD')) as "PIPELINE_STATUS_A22_Q4_DATE"
,ifnull(addprop_amnt."PIPELINE_STATUS_A22_Q4_AMNT",0) as "PIPELINE_STATUS_A22_Q4_AMNT"

,ifnull(addprop_txt."PIPELINE_STATUS_FC1_23_TXT",'Other') as "PIPELINE_STATUS_FC1_23_TXT"
,decode(addprop_date."PIPELINE_STATUS_FC1_23_DATE",'00000000',null,null,null,to_date(addprop_date."PIPELINE_STATUS_FC1_23_DATE",'YYYYMMDD')) as "PIPELINE_STATUS_FC1_23_DATE"

,ifnull(addprop_txt."Disposal_Leasing_2019_2023_TXT",'No') as "Disposal_Leasing_2019_2023_TXT"
,ifnull(addprop_txt."Disposal_Leasing_2019_2022_TXT",'No') as "Disposal_Leasing_2019_2022_TXT"

,ifnull(addprop_txt."A23_H1_vs_A22_H1_TXT",'No') as "A23_H1_vs_A22_H1_TXT"
,ifnull(addprop_txt."A23_vs_A19_Grouping_TXT",'No') as "A23_vs_A19_Grouping_TXT"
,ifnull(addprop_txt."A23_vs_A19_FY_Grouping_TXT",'No') as "A23_vs_A19_FY_Grouping_TXT"

,ifnull(addprop_txt."PIPELINE_STATUS_FC2_23_TXT",'No') as "PIPELINE_STATUS_FC2_23_TXT"
,decode(addprop_date."PIPELINE_STATUS_FC2_23_DATE",'00000000',null,null,null,to_date(addprop_date."PIPELINE_STATUS_FC2_23_DATE",'YYYYMMDD')) as "PIPELINE_STATUS_FC2_23_DATE"

,ifnull(addprop_txt."GE_AT_Asset_Segmentation_TXT",'No')   as "GE_AT_Asset_Segmentation_TXT"
,ifnull(addprop_txt."FC3_23_vs_A19_FY_Grouping_TXT",'No')  as "FC3_23_vs_A19_FY_Grouping_TXT"

,ifnull(addprop_txt."B24_vs_A19_FY_Grouping_TXT",'No')     as "B24_vs_A19_FY_Grouping_TXT"
,ifnull(addprop_txt."B24_vs_P24_Grouping_TXT",'No')        as "B24_vs_P24_Grouping_TXT"

,ifnull(addprop_txt."B24_vs_FC3_23_FY_Grouping_TXT",'No')     as "B24_vs_FC3_23_FY_Grouping_TXT"
,ifnull(addprop_txt."P25_vs_B24_FY_Grouping_TXT",'No')        as "P25_vs_B24_FY_Grouping_TXT"

,ifnull(addprop_txt."FC3_23_vs_A22_FY_Grouping_TXT",'No')        as "FC3_23_vs_A22_FY_Grouping_TXT"
,ifnull(addprop_txt."5YBP_2024_2028_primary_Grouping_TXT",'No')  as "5YBP_2024_2028_primary_Grouping_TXT"
,ifnull(addprop_txt."CEE_incl_or_excl_Hamburg_TXT",'No')         as "CEE_incl_or_excl_Hamburg_TXT"
,ifnull(addprop_txt."Disposal_Group_TXT",'No')   as "Disposal_Group_TXT"



--     , addprop_txt.*  exclude(appset_id, application_id, base_level)
--     , addprop_date.* exclude(appset_id, application_id, base_level)
--     , addprop_amnt.* exclude(appset_id, application_id, base_level)

from rawdata.planit.planit_finance_dim_entity   as ent
--left outer join lt_table_pc                     as asset_grouping on asset_grouping.cpropc  = ent.p_cpx
left outer join lt_regional_scope               as reg_scp        on reg_scp.base_level     = ent.entity
--left outer join lt_regional_4               as reg_4        on reg_4.base_level     = ent.entity
left outer join lt_asset_grouping               as asset_grp      on asset_grp.base_level   = ent.entity
left outer join lt_add_prop_text_pivot			as addprop_txt	  on addprop_txt.base_level = ent.entity
left outer join lt_add_prop_caldate_pivot		as addprop_date	  on addprop_date.base_level = ent.entity
left outer join lt_add_prop_amount_pivot		as addprop_amnt	  on addprop_amnt.base_level = ent.entity;
  );

