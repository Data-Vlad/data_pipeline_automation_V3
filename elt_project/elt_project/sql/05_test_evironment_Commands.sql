
--Data Governance:
Select * from test.dbo.data_quality_rules
Select * from test.dbo.data_quality_run_logs
sp_execute_data_quality_checks



--Config Tables:
Select * from elt_pipeline_configs
Select * from etl_pipeline_run_logs


 --CUNYFirst_RPT  Pipeline: 
 sp_transform_CUNYFirst_RPT
--Staging Tables:
Select * from test.dbo.stg_cu_hcm_positions_in_dbt
Select * from test.dbo.stg_hcmdb_bud_lines
Select * from test.dbo.stg_ri_cf_userid
Select * from test.dbo.stg_ri_compensation
Select * from test.dbo.stg_ri_dbt
Select * from test.dbo.stg_ri_email
Select * from test.dbo.stg_ri_ethnicity
Select * from test.dbo.stg_ri_job
Select * from test.dbo.stg_ri_job_all
Select * from test.dbo.stg_ri_personaldata
Select * from test.dbo.stg_ri_phone
Select * from test.dbo.stg_ri_position
Select * from [Test].[dbo].[stg_ri_dailytransactions]
--Destination Tables:
Select * from test.dbo.tbl_CU_HCM_POSITIONS_IN_DBT
Select * from test.dbo.tbl_HCMDB_BUD_LINES
Select * from test.dbo.tbl_CUNY_FIRST_USER_ID
Select * from test.dbo.tbl_HCM_RI_Compensation
Select * from test.dbo.tbl_RI_DBT
Select * from test.dbo.tbl_RI_Email
Select * from test.dbo.tbl_RI_Ethnic
Select * from test.dbo.tbl_RI_JOB
Select * from test.dbo.tbl_RI_Job_All
Select * from test.dbo.tbl_RI_PERSONALDATA
Select * from test.dbo.tbl_RI_Phone
Select * from test.dbo.tbl_RI_Position
Select * from [Test].[dbo].tbl_RI_DailyTransactions
--Staging Tables:
Delete from test.dbo.stg_cu_hcm_positions_in_dbt
Delete  from test.dbo.stg_hcmdb_bud_lines
Delete  from test.dbo.stg_ri_cf_userid
Delete  from test.dbo.stg_ri_compensation
Delete from test.dbo.stg_ri_dbt
Delete  from test.dbo.stg_ri_email
Delete  from test.dbo.stg_ri_ethnicity
Delete  from test.dbo.stg_ri_job
Delete  from test.dbo.stg_ri_job_all
Delete  from test.dbo.stg_ri_personaldata
Delete  from test.dbo.stg_ri_phone
Delete  from test.dbo.stg_ri_position
Delete from [Test].[dbo].[stg_ri_dailytransactions]
--Destination Tables:
Delete from test.dbo.tbl_CU_HCM_POSITIONS_IN_DBT
Delete from test.dbo.tbl_HCMDB_BUD_LINES
Delete from test.dbo.tbl_CUNY_FIRST_USER_ID
Delete from test.dbo.tbl_HCM_RI_Compensation
Delete from test.dbo.tbl_RI_DBT
Delete from test.dbo.tbl_RI_Email
Delete from test.dbo.tbl_RI_Ethnic
Delete from test.dbo.tbl_RI_JOB
Delete from test.dbo.tbl_RI_Job_All
Delete from test.dbo.tbl_RI_PERSONALDATA
Delete from test.dbo.tbl_RI_Phone
Delete from test.dbo.tbl_RI_Position
Delete from [Test].[dbo].tbl_RI_DailyTransactions



 --CUNYFirst_FSC  Pipeline:











--NextGen_FA Pipeline:
sp_transform_dynamic_sap
Select * from test.dbo.stg_dynamic_sap
Select * from test.dbo.tbl_SFDB_DYNMC_SAP
Delete from test.dbo.stg_dynamic_sap
Delete from test.dbo.tbl_SFDB_DYNMC_SAP