import hashlib
import datetime

mapper = {}


def return_format(n):
    return [n, None]


def is_company(id, dv):
    if len(id) == 9 and dv != '':
        return True
    return False


def get_name(n, pos):
    list = str(n).split(' ')
    name = []
    for i, n in enumerate(list):
        if i >= 4:
            name[3] += f' {n}'
        else:
            name.append(n)
    if (len(name)-1) < pos:
        return ""
    return name[pos]


def get_discount_percentage(price, special_price):
    discount_percentage = 0
    if special_price > 0 and price > 0:
        discount_percentage = (1 - (special_price / price)) * 100
    return discount_percentage


def get_hashmd5(value, encode):
    value_encode = str(value).encode(encode)
    return hashlib.md5(value_encode).hexdigest()


def get_list(value, separator):
    list_values = value.split(separator)
    return list_values


def get_objects(value, separator, separator_attr):
    list_values = value.split(separator)
    obj = {}
    for v in list_values:
        attr, val = v.split(separator_attr)
        obj[attr] = val
    return obj


now = lambda: datetime.datetime.utcnow()
