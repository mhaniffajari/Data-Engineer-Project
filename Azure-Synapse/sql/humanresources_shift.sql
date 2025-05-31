--CREATE EXTERNAL TABLE
CREATE EXTERNAL TABLE [dbo].[humanresources_shift]
(
shiftid int,
[name] varchar(4000),
[starttime] varchar(4000),
[endtime] varchar(4000),
[modifiedutcdate] varchar(4000),
[curated_date] varchar(4000)
)
WITH (DATA_SOURCE = [ADLS_DS_Bigdata],LOCATION = N'synapse/workspaces/datamart/AdventureWorks2022/humanresources_shift/*/*.parquet',FILE_FORMAT = [SynapseParquetFormat])
GO