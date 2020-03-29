import DataFlow.Descriptor.utils as utils

master = {
        'product_id': {
            'columns_file_descriptor': ['sku', 'ean'],
            'columns_pattern': '{}-{}',
            'rules': [
                {
                    'lamb_func': lambda product: utils.return_format(utils.get_hashmd5(product['product_id'], 'utf-8'))
                }
            ]
        },
        'sku': {
            'columns_file_descriptor': ['sku']
        },
        'ean': {
            'columns_file_descriptor': ['ean']
        },
        'name': {
            'columns_file_descriptor': ['name']
        },
        'brand': {
            'columns_file_descriptor': ['brand']
        },
        'reference': {
            'columns_file_descriptor': ['reference']
        },
        'model': {
            'columns_file_descriptor': ['model']
        },
        'description': {
            'columns_file_descriptor': ['description']
        },
        'features': {
            'columns_file_descriptor': ['features']
        },
        'images': {
            'columns_file_descriptor': ['images']
        },
        'videos': {
            'columns_file_descriptor': ['videos']
        }
     }

master_process = {
        'product_id': {
            'columns_file_descriptor': ['sku', 'ean'],
            'columns_pattern': '{}-{}',
            'rules': [
                {
                    'lamb_func': lambda product: utils.return_format(utils.get_hashmd5(product['product_id'], 'utf-8'))
                }
            ]
        }
}

master_state = {
        'product_id': {
            'columns_file_descriptor': ['sku', 'ean'],
            'columns_pattern': '{}-{}',
            'rules': [
                {
                    'lamb_func': lambda product: utils.return_format(utils.get_hashmd5(product['product_id'], 'utf-8'))
                }
            ]
        },
        'id_thor': {
            'columns_file_descriptor': ['id_thor']
        },
        'file_origin': {
            'columns_file_descriptor': ['file_origin']
        },
        'state': {
            'default': 0
        },
        'message': {
            'default': 'thor_update'
        },
        'update_date': {
            'default': utils.now(),
            'rules': [
                {
                    'lamb_func': lambda product: utils.return_format(utils.now())
                }
            ]
        },
        'creation_date': {
            'default': utils.now(),
            'rules': [
                {
                    'lamb_func': lambda product: utils.return_format(utils.now())
                }
            ]
        }
}

data_destinations = {
    'master': master,
    'master_process': master_process,
    'master_state': master_state,
}
