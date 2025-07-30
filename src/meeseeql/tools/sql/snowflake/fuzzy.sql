WITH all_matches AS (
  -- Table matches
  SELECT
    0 as result_type,
    'table' as object_type,
    table_schema as schema_name,
    table_name as object_name,
    table_schema || '.' || table_name as user_friendly_descriptor,
    NULL as data_type
  FROM information_schema.tables
  WHERE table_schema NOT IN ('INFORMATION_SCHEMA')

  UNION ALL

  -- Column matches
  SELECT
    1 as result_type,
    'column' as object_type,
    c.table_schema as schema_name,
    c.column_name as object_name,
    c.table_name || '.' || c.column_name as user_friendly_descriptor,
    c.data_type
  FROM information_schema.columns c
  WHERE c.table_schema NOT IN ('INFORMATION_SCHEMA')
)
SELECT
  object_type,
  schema_name,
  user_friendly_descriptor,
  data_type,
  CASE
      WHEN LOWER(object_name) = LOWER('{{search_term}}') THEN 100
      WHEN object_name ILIKE '{{search_term}}' || '%' THEN 95 - (LENGTH(object_name) - LENGTH('{{search_term}}')) * 0.4
      WHEN object_name ILIKE '%' || '{{search_term}}' THEN 90 - (LENGTH(object_name) - LENGTH('{{search_term}}')) * 0.2
      WHEN object_name ILIKE '%' || '{{search_term}}' || '%' THEN 85 - (LENGTH(object_name) - LENGTH('{{search_term}}')) * 0.1
      ELSE GREATEST(5, 90 * (1.0 - CAST(EDITDISTANCE(LOWER(object_name), LOWER('{{search_term}}')) AS FLOAT) / GREATEST(LENGTH(object_name), LENGTH('{{search_term}}'))))
  END - 10*(result_type) as ranking_score
FROM all_matches
WHERE
    (object_name ILIKE '%' || '{{search_term}}' || '%'
     OR EDITDISTANCE(LOWER(object_name), LOWER('{{search_term}}')) <= 0.3*LENGTH('{{search_term}}'))
  AND (
    '{{schema_filter}}' = '' OR schema_name = '{{schema_filter}}'
  )
ORDER BY
  ranking_score DESC,
  result_type,
  object_name
