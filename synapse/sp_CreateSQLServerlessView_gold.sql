USE gold_db;
GO

-- Drop and recreate procedure with DATA_SOURCE
CREATE OR ALTER PROC CreateSQLServerlessView_gold @ViewName nvarchar(100)
AS
BEGIN
    DECLARE @statement VARCHAR(MAX)

    SET @statement = N'CREATE OR ALTER VIEW ' + @ViewName + ' AS
        SELECT *
        FROM
            OPENROWSET(
                BULK ''SalesLT/' + @ViewName + '/'',
                DATA_SOURCE = ''gold_source'',
                FORMAT = ''DELTA''
            ) AS [result]'

    EXEC (@statement)
END
GO

