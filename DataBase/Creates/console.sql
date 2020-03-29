with generic_json (doc) as (
   values
    ('[{"product_id": "80a5ff3a65c388e63c8d73ec4aa581a2",
"ean": "23232323", "name": "Tornillo", "brand": "Ferretodo", "reference": "tor 1- 2",
"model": "2019", "description": "Es un tornillo de gran calidad en titanio",
"features": {"anch": " 2cm", "largo": "3cm"},
"images": ["https://images.pexels.com/photos/67636/rose-blue-flower-rose-blooms-67636.jpeg?auto=compress&cs=tinysrgb&dpr=2&w=500", "https://www.youtube.com/watch?v=r6FOgW450Ag"],
"videos": ["https://www.youtube.com/watch?v=r6FOgW450Ag", "https://www.youtube.com/watch?v=r6FOgW450Ag"]}]'::json)
)
insert into product.master (product_id,ean,name,brand,reference,model,description,features,images,videos)
select p.*
from generic_json l
  cross join lateral json_populate_recordset(null::product.master, doc) as p
on conflict (product_id) do update
  set product_id = excluded.product_id,
      ean = excluded.ean,name = excluded.name,
      brand = excluded.brand,reference = excluded.reference,model = excluded.model,
      description = excluded.description,features = excluded.features,
      images = excluded.images,videos = excluded.videos;


with generic_json (doc) as (
   values
    ('[{"product_id": "80a5ff3a65c388e63c8d73ec4aa581a2",
      "sku": "1", "ean": "23232323", "name": "Tornillo",
      "brand": "Ferretodo", "reference": "tor 1- 2", "model": "2019",
      "description": "Es un tornillo de gran calidad en titanio",
      "features": {"anch": " 2cm", "largo": "3cm"},
      "images": ["https://images.pexels.com/photos/67636/rose-blue-flower-rose-blooms-67636.jpeg?auto=compress&cs=tinysrgb&dpr=2&w=500", "https://www.youtube.com/watch?v=r6FOgW450Ag"],
      "videos": ["https://www.youtube.com/watch?v=r6FOgW450Ag", "https://www.youtube.com/watch?v=r6FOgW450Ag"]}]'::json)
)
insert into product.master (product_id,sku,ean,name,brand,reference,model,description,features,images,videos)
select p.*
from generic_json l
  cross join lateral json_populate_recordset(null::product.master, doc) as p
on conflict (product_id) do update
  set product_id = excluded.product_id,sku = excluded.sku,ean = excluded.ean,name = excluded.name,brand = excluded.brand,reference = excluded.reference,model = excluded.model,description = excluded.description,features = excluded.features,images = excluded.images,videos = excluded.videos;
