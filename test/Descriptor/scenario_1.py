import DataFlow.Descriptor.utils as utils
import DataFlow.Descriptor.master as master
import DataFlow.Descriptor.dictionary as dictionary

data = []
s1 = {'iva': '0', 'location_id': '03', 'retailer_price': '2000',
      'retailer_sku': '7702124840458.0', 'special_price': '0.0', 'stock': '2', 'price': '3900.0'}

s2 = {'iva': '0', 'location_id': '0.0', 'retailer_price': '2.0',
      'retailer_sku': '7702124840458.0', 'special_price': '0.0', 'stock': '2', 'price': '3900.0'}

s3 = {'iva': '0', 'location_id': '03', 'retailer_price': '2.0',
      'retailer_sku': '7702124840458.0.', 'special_price': '0.0', 'stock': '2', 'price': '3900.0'}

s4 = {'iva': '0', 'location_id': '03', 'retailer_price': '2.0',
      'retailer_sku': '7702124840458.0', 'special_price': '0.0', 'stock': '0', 'price': '3900.0'}

s5 = {'iva': '0', 'location_id': '03', 'retailer_price': '2.0',
      'retailer_sku': '7702124840458.0', 'special_price': '63000', 'stock': '2', 'price': '100000'}

data.append(s1)
data.append(s2)
data.append(s3)
data.append(s4)
data.append(s5)


def get_descriptor():
    dict = dictionary.dictionary

    data_source = {
        'credentials': 's3-catalog-credentials',
        'prefix': 'test/upsert_pricestock_method/J_PVP',
        'suffix': '.json'
    }

    data_set = [
        {
            'retailer': {
                'default': '557b478e53af331f002d3acc'
            },
            'iva': {
                'columns_file_descriptor': ['iva']
            },
            'location': {
                'columns_file_descriptor': ['location_id'],
                'rules': [
                    {
                        'lamb_func': lambda product: utils.return_format(dict['cencosud'][str(int(product['location']))])
                    }
                ]
            },
            'price': {
                'columns_file_descriptor': ['price'],
                'rules': [
                    {
                        'lamb_func': lambda product: utils.return_format(int(float(product['price'])))
                    }
                ]
            },
            'special_price': {
                'columns_file_descriptor': ['special_price'],
                'rules': [
                    {
                        'lamb_func': lambda product: utils.return_format(int(float(product['special_price'])))
                    }
                ]
            },
            'sku': {
                'columns_file_descriptor': ['retailer_sku'],
                'rules': [
                    {
                        'lamb_func': lambda product: utils.return_format(int(float(product['sku'])))
                    }
                ]
            },
            'stock': {
                'columns_file_descriptor': ['stock'],
                'rules': [
                    {
                        'lamb_func': lambda product: utils.return_format(int(float(product['stock'])))
                    },
                    {
                        'lamb_func': lambda product: [product['stock'], 'El producto se va a desactivar']
                        if product['stock'] == 0 else utils.return_format(product['stock'])
                    }
                ]
            },
            'discount_percentage': {
                'columns_file_descriptor': ['special_price'],
                'rules': [
                    {
                        'lamb_func': lambda product: utils.return_format(utils.get_discount_percentage(int(float(product['price'])),
                                                                                   int(float(product['special_price']))))
                    }
                ]
            }
        }
    ]

    data_destination = {
        'source': 'mysql',
        'credentials': 'catalog-database-credentials',
        'database': 'catalog',
        'table': 'location_stock',
        'destination_descriptor': master.data_destinations['location_stock']
    }

    descriptor = {
        'name': 'jumbo',
        'active': True,
        'kill': False,
        'seconds': 1,
        'data_source': data_source,
        'data_set': data_set,
        'data_destination': [data_destination]
    }

    return descriptor
