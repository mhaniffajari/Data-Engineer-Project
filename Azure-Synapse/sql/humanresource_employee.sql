--CREATE EXTERNAL TABLE
CREATE EXTERNAL TABLE [dbo].[humanresources_employee]
(
[businessentityid] varchar(4000),
[nationalidnumber] varchar(4000),
[loginid] varchar(4000),
[organizationlevel] int,
[jobtitle] varchar(4000),
[birthdate] varchar(4000),
[maritalstatus] varchar(4000),
[gender] varchar(4000),
[hiredate] varchar(4000),
[salariedflag] varchar(4000),
[vacationhours] int,
[sickleavehours] int,
[currentflag] varchar(4000),
[rowguid] varchar(4000),
[modifiedutcdate] varchar(4000),
[curated_date] varchar(4000)
)
WITH (DATA_SOURCE = [ADLS_DS_Bigdata],LOCATION = N'synapse/workspaces/datamart/AdventureWorks2022/humanresources_employee/*/*.parquet',FILE_FORMAT = [SynapseParquetFormat])
GO