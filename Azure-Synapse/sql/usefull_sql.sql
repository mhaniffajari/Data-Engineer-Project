-- check SP
SELECT OBJECT_DEFINITION (OBJECT_ID(N'usp_post_ironlake_merge_ehm_data'))

--check format columns table
SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'TABLE_NAME'
-- check row count in last modified date, week and month
WITH MaxModifiedDateCategory AS (
    SELECT MAX(modifieddate) AS max_modified_date,datepart(week,MAX(modifieddate)) as week, datepart(month,max(modifieddate)) as month,datepart(year,max(modifieddate)) as year
    FROM dbo.category
)
SELECT 
    'category' AS table_name,
    COUNT(*) AS row_count,
    MAX(modifieddate) AS last_modified_date,
    SUM(CASE WHEN modifieddate = mcc.max_modified_date THEN 1 ELSE 0 END) AS rows_in_last_modified_date,
    SUM(CASE WHEN datepart(week,modifieddate) = mcc.week and datepart(year,modifieddate)=mcc.year THEN 1 ELSE 0 END) AS rows_in_last_modified_date_week,
    SUM(CASE WHEN datepart(month,modifieddate) = mcc.month and datepart(year,modifieddate)=mcc.year THEN 1 ELSE 0 END) AS rows_in_last_modified_date_month
FROM 
    dbo.category
CROSS JOIN 
    MaxModifiedDateCategory mcc
