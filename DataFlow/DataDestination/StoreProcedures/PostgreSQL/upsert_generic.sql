with generic_json (doc) as (
   values
    ('{data}'::json)
)
insert into {table} {columns}
select p.*
from generic_json l
  cross join lateral json_populate_recordset(null::{table}, doc) as p
on conflict ({primary_key}) do update
  set {duplicate_format};
