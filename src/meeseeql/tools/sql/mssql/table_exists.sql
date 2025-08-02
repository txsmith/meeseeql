SELECT t.name FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE LOWER(t.name) = LOWER('{{table_name}}')
AND s.name NOT IN ('information_schema', 'sys')
AND LOWER(s.name) = LOWER('{{schema_name}}')