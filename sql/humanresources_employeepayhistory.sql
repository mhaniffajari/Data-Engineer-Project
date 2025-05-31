--CREATE EXTERNAL TABLE
CREATE EXTERNAL TABLE [dbo].[humanresources_employeepayhistory]
(
[businessentityid] varchar(4000),
[ratechangedate] varchar(4000),
[rate] varchar(4000),
[payfrequency] varchar(4000),
[modifiedutcdate] varchar(4000),
[curated_date] varchar(4000)
)
WITH (DATA_SOURCE = [ADLS_DS_Bigdata],LOCATION = N'synapse/workspaces/datamart/AdventureWorks2022/humanresources_employeepayhistory/*/*.parquet',FILE_FORMAT = [SynapseParquetFormat])
GO