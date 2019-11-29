select
  constraint_name as name,
  concat(table_schema, ".", table_name) as fromTbl,
  group_concat(column_name separator ", ") as fromCols,
  concat(
    referenced_table_schema,
    ".",
    referenced_table_name
  ) as toTbl,
  group_concat(referenced_column_name separator ", ") as toCols
from (
    select
      constraint_name,
      table_schema,
      table_name,
      column_name,
      referenced_table_schema,
      referenced_table_name,
      referenced_column_name
    from information_schema.key_column_usage
    where
      (
        referenced_table_schema = "Assortiment"
        and referenced_table_name = "Plant"
      )
      or (
        table_schema = "Assortiment"
        and table_name = "Plant"
      )
      and constraint_name <> "PRIMARY"
  ) as relations
group by
  constraint_name,
  fromTbl,
  toTbl