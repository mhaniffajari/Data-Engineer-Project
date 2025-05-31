-- Create a temporary table to store the result
CREATE TABLE dbo.query_table (
    TABLE_SCHEMA NVARCHAR(255),
    TABLE_NAME NVARCHAR(255),
    query NVARCHAR(MAX)
)

DECLARE @schema NVARCHAR(255)
DECLARE @table NVARCHAR(255)
DECLARE @sql NVARCHAR(MAX)

-- Define cursor to loop through ingest_table
DECLARE table_cursor CURSOR FOR
SELECT TABLE_SCHEMA, TABLE_NAME FROM ingest_table

OPEN table_cursor
FETCH NEXT FROM table_cursor INTO @schema, @table

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'SELECT * FROM ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table)
        
    INSERT INTO dbo.query_table (TABLE_SCHEMA, TABLE_NAME, query)
    VALUES (@schema, @table, @sql)

    FETCH NEXT FROM table_cursor INTO @schema, @table
END

CLOSE table_cursor
DEALLOCATE table_cursor

-- Now, you have the result in the temporary table #tempResult
SELECT * FROM dbo.query_table
