import DataFlow.Descriptor.utils as utils
import DataFlow.Descriptor.dictionary as dictionary
import DataFlow.Descriptor.master as master

dict = dictionary.dictionary

data_source = {
    'credentials': 's3-catalog-credentials',
    'prefix': 'upsert_master/upsert_method/',
    'suffix': '.json'
}

data_set = [
     {
        'sku': {
            'columns_file_descriptor': ['sku']
        },
        'ean':{
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
            'columns_file_descriptor': ['features'],
            'rules': [
                {
                    'lamb_func': lambda product: utils.return_format(utils.get_objects(product['features'], "|", ":"))
                }
            ]
        },
        'images':{
            'columns_file_descriptor': ['images'],
            'rules': [
                {
                    'lamb_func': lambda product: utils.return_format(utils.get_list(product['images'], "|"))
                }
            ]
        },
        'videos': {
            'columns_file_descriptor': ['videos'],
            'rules': [
                {
                    'lamb_func': lambda product: utils.return_format(utils.get_list(product['videos'], "|"))
                }
            ]
        },
        'file_origin': {
            'columns_file_descriptor': ['file_origin']
        }
     }
]

data_destination_master = {
    'source': 'postgresql',
    'credentials': 'catalog-database-credentials',
    'database': 'catalog',
    'table': 'master',
    'destination_descriptor': master.data_destinations['master']
}

data_destination_master_process = {
    'source': 'postgresql',
    'credentials': 'catalog-database-credentials',
    'database': 'catalog',
    'table': 'master_process_1',
    'destination_descriptor': master.data_destinations['master_process']
}

data_destination_master_state = {
    'source': 'postgresql',
    'credentials': 'catalog-database-credentials',
    'database': 'catalog',
    'table': 'master_state',
    'destination_descriptor': master.data_destinations['master_state']
}

descriptor = {
    'name': 'generic',
    'active': True,
    'kill': False,
    'seconds': 1,
    'data_source': data_source,
    'data_set': data_set,
    'data_destination': [data_destination_master, data_destination_master_state, data_destination_master_process]
}