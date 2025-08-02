SELECT name FROM sqlite_master
WHERE type='table' AND LOWER(name) = LOWER('{{table_name}}')
LIMIT 1