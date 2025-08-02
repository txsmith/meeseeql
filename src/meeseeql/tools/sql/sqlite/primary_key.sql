SELECT
    p.name as column_name
FROM pragma_table_info('{{table_name}}') p
WHERE p.pk = 1
ORDER BY p.pk