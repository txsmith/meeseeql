WITH all_matches AS (
  -- Table matches
  SELECT
    0 as result_type,
    'table' as object_type,
    schemaname as schema_name,
    tablename as object_name,
    schemaname || '.' || tablename as user_friendly_descriptor,
    NULL as data_type
  FROM pg_tables

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
  WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')

  UNION ALL

  -- Enum type matches
  SELECT
    2 as result_type,
    'enum' as object_type,
    n.nspname as schema_name,
    t.typname as object_name,
    t.typname || ' (' || string_agg(e.enumlabel, ', ' ORDER BY e.enumsortorder) || ')' as user_friendly_descriptor,
    'enum' as data_type
  FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace
  JOIN pg_enum e ON t.oid = e.enumtypid
  WHERE t.typtype = 'e'
  GROUP BY n.nspname, t.typname

  UNION ALL

  -- Enum value matches
  SELECT
    2 as result_type,
    'enum' as object_type,
    n.nspname as schema_name,
    e.enumlabel as object_name,
    t.typname || ' (' || string_agg(e2.enumlabel, ', ' ORDER BY e2.enumsortorder) || ')' as user_friendly_descriptor,
    'enum' as data_type
  FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace
  JOIN pg_enum e ON t.oid = e.enumtypid
  JOIN pg_enum e2 ON t.oid = e2.enumtypid
  WHERE t.typtype = 'e'
  GROUP BY n.nspname, t.typname, e.enumlabel
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
      ELSE GREATEST(5, 90 * (1.0 - CAST(levenshtein(LOWER(object_name), LOWER('{{search_term}}')) AS FLOAT) / GREATEST(LENGTH(object_name), LENGTH('{{search_term}}'))))
  END - 10*(result_type) as ranking_score
FROM all_matches
WHERE
    (object_name ILIKE '%' || '{{search_term}}' || '%'
     OR levenshtein(LOWER(object_name), LOWER('{{search_term}}')) <= 0.3*LENGTH('{{search_term}}'))
  AND (
    '{{schema_filter}}' = '' OR schema_name = '{{schema_filter}}'
  )
ORDER BY
  ranking_score DESC,
  result_type,
  object_name
