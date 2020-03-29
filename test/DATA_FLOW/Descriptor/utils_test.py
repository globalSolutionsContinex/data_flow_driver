import DataFlow.Descriptor.utils as utils

record = {
    "nombre": "nuñez buritica ana maira",
    "identidad": "30664743",
    "dv": "4"
}
s = lambda record: utils.return_format(record['nombre'] if utils.is_company(record['identidad'], record['dv']) else utils.get_name(record['nombre'], 0))
print(s(record))
print(utils.get_name(record['nombre'], 0))
print(utils.get_name(record['nombre'], 1))
print(utils.get_name(record['nombre'], 2))
print(utils.get_name(record['nombre'], 3))

