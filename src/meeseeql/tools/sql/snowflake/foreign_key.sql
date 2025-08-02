SELECT
    fk_tco.table_schema as source_schema_name,
    fk_tco.table_name as source_table_name,
    NULL as source_column_name,
    pk_tco.table_schema as dest_schema_name,
    pk_tco.table_name as dest_table_name,
    NULL as dest_column_name,
    fk_tco.constraint_name
FROM information_schema.referential_constraints rco
JOIN information_schema.table_constraints fk_tco
    ON fk_tco.constraint_name = rco.constraint_name
    AND fk_tco.constraint_schema = rco.constraint_schema
JOIN information_schema.table_constraints pk_tco
    ON pk_tco.constraint_name = rco.unique_constraint_name
    AND pk_tco.constraint_schema = rco.unique_constraint_schema
WHERE (
    (LOWER(fk_tco.table_name) = LOWER('{{table_name}}') AND LOWER(fk_tco.table_schema) = LOWER('{{schema_name}}'))
    OR
    (LOWER(pk_tco.table_name) = LOWER('{{table_name}}') AND LOWER(pk_tco.table_schema) = LOWER('{{schema_name}}'))
)
ORDER BY
    CASE WHEN LOWER(fk_tco.table_name) = LOWER('{{table_name}}') THEN 0 ELSE 1 END,
    fk_tco.constraint_name