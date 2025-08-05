WITH all_matches AS (
  -- Table matches
  SELECT
    0 as result_type,
    'table' as object_type,
    s.name as schema_name,
    t.name as object_name,
    s.name + '.' + t.name as user_friendly_descriptor,
    NULL as data_type
  FROM sys.tables t
  INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
  WHERE s.name NOT IN ('information_schema', 'sys')

  UNION ALL

  -- Column matches
  SELECT
    1 as result_type,
    'column' as object_type,
    s.name as schema_name,
    c.name as object_name,
    t.name + '.' + c.name as user_friendly_descriptor,
    ty.name as data_type
  FROM sys.columns c
  INNER JOIN sys.tables t ON c.object_id = t.object_id
  INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
  INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
  WHERE s.name NOT IN ('information_schema', 'sys')
)
SELECT
  object_type,
  schema_name,
  user_friendly_descriptor,
  data_type,
  CASE
      WHEN LOWER(object_name) = LOWER('{{search_term}}') THEN 100
      WHEN LOWER(object_name) LIKE LOWER('{{search_term}}%') THEN 95 - (LEN(object_name) - LEN('{{search_term}}')) * 0.4
      WHEN LOWER(object_name) LIKE LOWER('%{{search_term}}') THEN 90 - (LEN(object_name) - LEN('{{search_term}}')) * 0.2
      WHEN LOWER(object_name) LIKE LOWER('%{{search_term}}%') THEN 85 - (CHARINDEX(LOWER('{{search_term}}'), LOWER(object_name)) * 0.1) - (LEN(object_name) - LEN('{{search_term}}')) * 0.1
      ELSE 0
  END - 10*(result_type) as ranking_score
FROM all_matches
WHERE
    (LOWER(object_name) LIKE LOWER('%{{search_term}}%'))
ORDER BY
  ranking_score DESC,
  result_type ASC,
  object_name ASC
