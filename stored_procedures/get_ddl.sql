CREATE OR REPLACE PROCEDURE
  util.get_table_ddl_us (IN fully_qualified_table_name string)
BEGIN
EXECUTE IMMEDIATE
  """
WITH Components AS (
  SELECT
    CONCAT("`", table_catalog, ".", table_schema, ".", table_name,"`") AS table_name,
    ARRAY_AGG(
      STRUCT(column_name, data_type, is_nullable, is_partitioning_column, clustering_ordinal_position)
      ORDER BY ordinal_position
    ) AS columns,
    (SELECT ARRAY_AGG(STRUCT(option_name, option_value))
     FROM `region-us.INFORMATION_SCHEMA.TABLE_OPTIONS` AS t2
     WHERE t.table_name = t2.table_name) AS options
  FROM `region-us.INFORMATION_SCHEMA.TABLES` AS t
  LEFT JOIN `region-us.INFORMATION_SCHEMA.COLUMNS`
  USING (table_catalog, table_schema, table_name)
  WHERE UPPER(table_catalog) = ?
  and UPPER(table_schema) = ?
  and UPPER(table_name) = ?
  GROUP BY table_catalog, table_schema, t.table_name
)
SELECT
  CONCAT(
    'CREATE OR REPLACE TABLE ',
    table_name,
    util.MakeColumnList(columns),
    util.MakePartitionByClause(columns),
    util.MakeClusterByClause(columns),
    util.MakeOptionList(options))
FROM Components
"""
USING
  UPPER(SPLIT(fully_qualified_table_name, '.')[ORDINAL(1)]),
  UPPER(SPLIT(fully_qualified_table_name, '.')[ORDINAL(2)]),
  UPPER(SPLIT(fully_qualified_table_name, '.')[ORDINAL(3)]);
END
