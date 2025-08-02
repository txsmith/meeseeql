SELECT c.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.columns c ON kcu.column_name = c.column_name
    AND kcu.table_name = c.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
AND LOWER(tc.table_name) = LOWER('{{table_name}}')
AND LOWER(tc.table_schema) = LOWER('{{schema_name}}')
ORDER BY kcu.ordinal_position