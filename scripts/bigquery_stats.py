# to get view ids query "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA where schema_name < 'a'"
view_ids = []
# to get table sizes "SELECT   project_id,   dataset_id,   table_id,   row_count,   size_bytes,
# DATE_FROM_UNIX_DATE(SAFE_CAST(CEIL(last_modified_time/60/60/24/1000) AS INT64)) as last_modified
# FROM `104411629.__TABLES__` order by last_modified_time desc limit 1"

subqueries = "UNION ALL".join([""" (SELECT project_id, dataset_id, table_id, row_count,size_bytes,
 DATE_FROM_UNIX_DATE(SAFE_CAST(CEIL(last_modified_time/60/60/24/1000) AS INT64)) as last_modified FROM `{}.__TABLES__` 
 order by row_count desc limit 1)
        """.format(view_id) for view_id in view_ids])

print(subqueries)