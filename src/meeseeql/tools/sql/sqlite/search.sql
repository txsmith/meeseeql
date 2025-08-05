-- SQLite version: simplified to tables only due to SQL transformer limitations with pragma syntax
SELECT
  'table' as object_type,
  'main' as schema_name,
  name as user_friendly_descriptor,
  NULL as data_type,
  CASE
      WHEN LOWER(name) = LOWER('{{search_term}}') THEN 100
      WHEN LOWER(name) LIKE LOWER('{{search_term}}%') THEN 95 - (LENGTH(name) - LENGTH('{{search_term}}')) * 0.4
      WHEN LOWER(name) LIKE LOWER('%{{search_term}}') THEN 90 - (LENGTH(name) - LENGTH('{{search_term}}')) * 0.2
      WHEN LOWER(name) LIKE LOWER('%{{search_term}}%') THEN 85 - (INSTR(LOWER(name), LOWER('{{search_term}}')) * 0.1) - (LENGTH(name) - LENGTH('{{search_term}}')) * 0.1
      ELSE 0
  END as ranking_score
FROM sqlite_master
WHERE type = 'table'
  AND name NOT LIKE 'sqlite_%'
  AND (LOWER(name) LIKE LOWER('%{{search_term}}%'))
ORDER BY
  ranking_score DESC,
  name ASC
