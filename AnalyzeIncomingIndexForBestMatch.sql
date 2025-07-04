-- ========== INPUT ==========
DECLARE @SchemaName NVARCHAR(128)            = 'dbo';
DECLARE @TableName  NVARCHAR(128)            = 'table'; -- target table
DECLARE @KeyColumns NVARCHAR(MAX)            = 'col1, col2'; -- columns in your new index you want to check before creating
DECLARE @IncludedColumns NVARCHAR(MAX)       = 'col3, col4'; -- included columns in your new index you want to check before creating
DECLARE @FullMatchText NVARCHAR(10)          = N'✅ Full Match';
DECLARE @AddIncludedColumnsText NVARCHAR(50) = N'🛠️ Add Included Columns';
DECLARE @PartialKeyMatchText NVARCHAR(50)    = N'🛠️ Partial Key Match';
DECLARE @NoUsefulMatchText NVARCHAR(50)      = N'➕ No Useful Match';
DECLARE @IncludePrimaryKeyIndexes BIT        = 0;
DECLARE @ObjectId INT                        = OBJECT_ID(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName));

DECLARE @KeyCols TABLE (colname NVARCHAR(128));
DECLARE @InclCols TABLE (colname NVARCHAR(128));

INSERT INTO @KeyCols SELECT TRIM(value) FROM STRING_SPLIT(@KeyColumns, ',');
INSERT INTO @InclCols SELECT TRIM(value) FROM STRING_SPLIT(@IncludedColumns, ',');

-- ========== INDEX STRUCTURE ==========
WITH IndexCols AS (
    SELECT 
        i.name AS index_name,
        i.index_id,
        c.name AS column_name,
        ic.index_column_id,
        ic.is_included_column
    FROM sys.indexes i
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE i.object_id = @ObjectId AND i.type_desc <> 'HEAP'
	AND (@IncludePrimaryKeyIndexes = 1 OR i.is_primary_key = 0)
),
IndexAgg AS (
    SELECT 
        i.index_name,
        STRING_AGG(CASE WHEN is_included_column = 0 THEN column_name END, ', ') AS key_columns,
        STRING_AGG(CASE WHEN is_included_column = 1 THEN column_name END, ', ') AS included_columns,
        COUNT(*) AS total_columns,
        SUM(CASE WHEN is_included_column = 0 THEN 1 ELSE 0 END) AS key_count,
        SUM(CASE WHEN is_included_column = 1 THEN 1 ELSE 0 END) AS included_count
    FROM IndexCols i
    GROUP BY i.index_name
),
IndexUsage AS (
    SELECT 
        i.name AS index_name,
        COALESCE(us.user_seeks, 0) AS user_seeks,
        COALESCE(us.user_scans, 0) AS user_scans,
        COALESCE(us.user_lookups, 0) AS user_lookups,
        COALESCE(us.user_updates, 0) AS user_updates,
        COALESCE(us.user_seeks + us.user_scans + us.user_lookups, 0) AS total_reads,
        us.last_user_seek,
        us.last_user_scan
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats us 
        ON i.object_id = us.object_id AND i.index_id = us.index_id AND us.database_id = DB_ID()
    WHERE i.object_id = @ObjectId
),
MatchAnalysis AS (
    SELECT 
        a.index_name,
        a.key_columns,
        a.included_columns,
        a.key_count,
        a.included_count,
        a.total_columns,
        -- Count of matched key columns
        (SELECT COUNT(*) FROM @KeyCols kc
         WHERE EXISTS (
            SELECT 1 FROM IndexCols ic 
            WHERE ic.index_name = a.index_name AND ic.is_included_column = 0 
              AND LTRIM(RTRIM(ic.column_name)) = kc.colname
         )) AS matched_key_columns,
        -- Count of matched included columns
        (SELECT COUNT(*) FROM @InclCols icl
         WHERE EXISTS (
            SELECT 1 FROM IndexCols ic 
            WHERE ic.index_name = a.index_name AND ic.is_included_column = 1 
              AND LTRIM(RTRIM(ic.column_name)) = icl.colname
         )) AS matched_included_columns,
        (SELECT COUNT(*) FROM @KeyCols) AS total_key_columns,
        (SELECT COUNT(*) FROM @InclCols) AS total_included_columns
    FROM IndexAgg a
),
IndexIncludedCols AS (
    SELECT
        i.index_name,
        STRING_AGG(column_name, ', ') AS included_columns_existing
    FROM IndexCols i
    WHERE i.is_included_column = 1
    GROUP BY i.index_name
),
FinalAnalysis AS (
    SELECT 
        m.index_name,
        m.key_columns,
        m.included_columns,
        iic.included_columns_existing,
        m.matched_key_columns,
        m.total_key_columns,
        m.matched_included_columns,
        m.total_included_columns,
        u.total_reads,
        u.user_updates,
        m.total_columns,
        (
			SELECT STUFF((
				SELECT DISTINCT ', ' + TRIM(value)
				FROM STRING_SPLIT(
					CONCAT(
						COALESCE(m.included_columns, ''), ',', COALESCE(iic.included_columns_existing, '')
					), ','
				)
				WHERE value <> ''
				FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
			, 1, 2, '')
		) AS included_columns_union,
        CASE 
            WHEN m.matched_key_columns = m.total_key_columns 
                 AND m.matched_included_columns = m.total_included_columns THEN @FullMatchText
            WHEN m.matched_key_columns = m.total_key_columns THEN @AddIncludedColumnsText
            WHEN m.matched_key_columns > 0 THEN @PartialKeyMatchText
            ELSE @NoUsefulMatchText
        END AS match_result,
        (m.matched_key_columns * 1000 + m.matched_included_columns * 100 + u.total_reads * 2 - u.user_updates) AS match_score
    FROM MatchAnalysis m
    LEFT JOIN IndexUsage u ON m.index_name = u.index_name
    LEFT JOIN IndexIncludedCols iic ON m.index_name = iic.index_name
)

-- ====== FINAL OUTPUT ======
SELECT 
    *,
    CASE 
        WHEN match_result = @FullMatchText THEN '-- Index already exists and matches fully.'
        WHEN match_result IN (@AddIncludedColumnsText, @PartialKeyMatchText) THEN 
            'DROP INDEX IF EXISTS [' + index_name + '] ON [' + @SchemaName + '].[' + @TableName + '];' + CHAR(13) +
            'CREATE NONCLUSTERED INDEX [' + index_name + '] ON [' + @SchemaName + '].[' + @TableName + '] (' + @KeyColumns + ') ' +
            CASE WHEN included_columns_union IS NOT NULL AND LEN(included_columns_union) > 0 
                 THEN 'INCLUDE (' + included_columns_union + ')' ELSE '' END + ';'
        WHEN match_result = @NoUsefulMatchText THEN
            'CREATE NONCLUSTERED INDEX [IX_' + @TableName + '_' + REPLACE(REPLACE(@KeyColumns, ',', '_'), ' ', '') + '] ON [' + @SchemaName + '].[' + @TableName + '] (' + @KeyColumns + ')' +
            CASE WHEN LEN(@IncludedColumns) > 0 THEN ' INCLUDE (' + @IncludedColumns + ')' ELSE '' END + ';'
        ELSE '-- Unknown match type'
    END AS suggested_script
FROM FinalAnalysis
ORDER BY match_score DESC;
