--CREATE EXTERNAL TABLE
CREATE EXTERNAL TABLE [dbo].[humanresources_department]
(
[name] varchar(4000),
[groupname] varchar(4000),
[modifieddate] varchar(4000),
[departementid] int,
[curated_date] varchar(4000)
)
WITH (DATA_SOURCE = [ADLS_DS_Bigdata],LOCATION = N'synapse/workspaces/datamart/AdventureWorks2022/humanresources_departement/*/*.parquet',FILE_FORMAT = [SynapseParquetFormat])
GO

SELECT * FROM sys.external_data_sources