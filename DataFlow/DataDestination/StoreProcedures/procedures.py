
# BASE PATHS
base_path_mysql = "DataFlow/DataDestination/StoreProcedures/Mysql/{procedure}"
base_path_post_gre = "DataFlow/DataDestination/StoreProcedures/PostgreSQL/{procedure}"

# STP
upsert_generic = base_path_mysql.format(procedure="upsert_generic.sql")
upsert_generic_post_gre = base_path_post_gre.format(procedure="upsert_generic.sql")
