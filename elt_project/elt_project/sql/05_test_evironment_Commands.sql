
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
 --CUNYFirst_FSC Pipeline:
 
 --Staging Tables:
 Select * from [Test].[dbo].[stg_CC_BudOverview]
 Select * from [Test].[dbo].[stg_CoA_OperatingUnit]
 Select * from [Test].[dbo].[stg_CU_BUDGET_OVERVIEW_ALL]
 Select * from [Test].[dbo].[stg_CC_ReconDataFull]
 Select * from [Test].[dbo].[stg_CU_REQUESTORS]
 Select * from [Test].[dbo].[stg_CFSE_TravelExpense]
 Select * from [Test].[dbo].[stg_CFSE_TravelExpenseProxies]
 Select * from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_01]
 Select * from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_02]
 Select * from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_03]
 Select * from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_04]
 Select * from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_15]
 Select * from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_55]
 Select * from [Test].[dbo].[stg_CC_Travel]
 Select * from [Test].[dbo].[stg_CFSE_SecurityRoles]
 --Destination Tables:
 Select * from [Test].[dbo].[tbl_CC_BudOverview]
 Select * from [Test].[dbo].[tbl_CoA_OperatingUnit]
 Select * from [Test].[dbo].[tbl_CU_BUDGET_OVERVIEW_ALL]
 Select * from [Test].[dbo].[tbl_CC_ReconDataFull]
 Select * from [Test].[dbo].[tbl_CU_REQUESTORS]
 Select * from [Test].[dbo].[tbl_CFSE_TravelExpense]
 Select * from [Test].[dbo].[tbl_CFSE_TravelExpenseProxies]
 Select * from [Test].[dbo].[tbl_CU_FSTE_VAL_DEPT_APPROVERS]
 Select * from [Test].[dbo].[tbl_CC_Travel]
 Select * from [Test].[dbo].[tbl_CFSE_SecurityRoles]
 --Staging Tables:
 Delete from [Test].[dbo].[stg_CC_BudOverview]
 Delete from [Test].[dbo].[stg_CoA_OperatingUnit]
 Delete from [Test].[dbo].[stg_CU_BUDGET_OVERVIEW_ALL]
 Delete from [Test].[dbo].[stg_CC_ReconDataFull]
 Delete from [Test].[dbo].[stg_CU_REQUESTORS]
 Delete from [Test].[dbo].[stg_CFSE_TravelExpense]
 Delete from [Test].[dbo].[stg_CFSE_TravelExpenseProxies]
 Delete from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_01]
 Delete from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_02]
 Delete from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_03]
 Delete from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_04]
 Delete from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_15]
 Delete from [Test].[dbo].[stg_CU_FSTE_VAL_DEPT_APPROVERS_55]
 Delete from [Test].[dbo].[stg_CC_Travel]
 Delete from [Test].[dbo].[stg_CFSE_SecurityRoles]
 --Destination Tables:
 Delete from [Test].[dbo].[tbl_CC_BudOverview]
 Delete from [Test].[dbo].[tbl_CoA_OperatingUnit]
 Delete from [Test].[dbo].[tbl_CU_BUDGET_OVERVIEW_ALL]
 Delete from[Test].[dbo].[tbl_CC_ReconDataFull]
 Delete from [Test].[dbo].[tbl_CU_REQUESTORS]
 Delete from [Test].[dbo].[tbl_CFSE_TravelExpense]
 Delete from [Test].[dbo].[tbl_CFSE_TravelExpenseProxies]
 Delete from [Test].[dbo].[tbl_CU_FSTE_VAL_DEPT_APPROVERS]
 Delete from [Test].[dbo].[tbl_CC_Travel]
 Delete from [Test].[dbo].[tbl_CFSE_SecurityRoles]










--NextGen_FA Pipeline:
sp_transform_dynamic_sap
Select * from test.dbo.stg_dynamic_sap
Select * from test.dbo.tbl_SFDB_DYNMC_SAP
Delete from test.dbo.stg_dynamic_sap
Delete from test.dbo.tbl_SFDB_DYNMC_SAP





