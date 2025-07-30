WITH all_matches AS (
  -- Table matches
  SELECT
    0 as result_type,
    'table' as object_type,
    table_schema as schema_name,
    table_name as object_name,
    CONCAT(table_schema, '.', table_name) as user_friendly_descriptor,
    NULL as data_type
  FROM information_schema.tables
  WHERE table_type = 'BASE TABLE'
    AND table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')

  UNION ALL

  -- Column matches
  SELECT
    1 as result_type,
    'column' as object_type,
    c.table_schema as schema_name,
    c.column_name as object_name,
    CONCAT(c.table_name, '.', c.column_name) as user_friendly_descriptor,
    c.data_type
  FROM information_schema.columns c
  WHERE c.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
)
SELECT
  object_type,
  schema_name,
  user_friendly_descriptor,
  data_type,
  CASE
      WHEN LOWER(object_name) = LOWER('{{search_term}}') THEN 100
      WHEN LOWER(object_name) LIKE LOWER('{{search_term}}%') THEN 95 - (CHAR_LENGTH(object_name) - CHAR_LENGTH('{{search_term}}')) * 0.4
      WHEN LOWER(object_name) LIKE LOWER('%{{search_term}}') THEN 90 - (CHAR_LENGTH(object_name) - CHAR_LENGTH('{{search_term}}')) * 0.2
      WHEN LOWER(object_name) LIKE LOWER('%{{search_term}}%') THEN 85 - (LOCATE(LOWER('{{search_term}}'), LOWER(object_name)) * 0.1) - (CHAR_LENGTH(object_name) - CHAR_LENGTH('{{search_term}}')) * 0.1
      ELSE 0
  END - 10*(result_type) as ranking_score
FROM all_matches
WHERE
    (LOWER(object_name) LIKE LOWER('%{{search_term}}%'))
  AND (
    '{{schema_filter}}' = '' OR schema_name = '{{schema_filter}}'
  )
ORDER BY
  ranking_score DESC,
  result_type ASC,
  object_name ASC
