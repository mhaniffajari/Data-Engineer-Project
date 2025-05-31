CREATE EXTERNAL TABLE [dbo].[humanresources_employeedepartmenthistory]
(
	[businessentityid] [varchar](4000),
	[departmentid] [varchar](4000),
	[shiftid] [int],
	[startdate] [varchar](4000),
	[enddate] [varchar](4000),
	[modifiedutcdate] [varchar](4000),
	[curated_date] [varchar](4000)
)
WITH (DATA_SOURCE = [ADLS_DS_Bigdata],LOCATION = N'synapse/workspaces/datamart/AdventureWorks2022/humanresources_employeedepartmenthistory/*/*.parquet',FILE_FORMAT = [SynapseParquetFormat])
GO