import DataFlow.DataDestination.DB.master as master
import Infrastructure.postgres as postgresql
import uuid
from Infrastructure import log
import json


class DB(master.MasterDB):

    def __init__(self):
        super().__init__()
        self.logger = log.get_logger('DB')

    def get_format(self, data, data_destination):
        upsert_data = super().get_basic_format(data, data_destination)
        duplicate = {}
        for d in data:
            if d['id'] not in duplicate:
                duplicate[d['id']] = uuid.uuid4().hex
            else:
                self.logger.info(f'Repeated data {json.dumps(d)} -- {duplicate[d["id"]]}')
                d['id'] = uuid.uuid4().hex

        upsert_data[self.constants.TABLE] = data_destination[self.constants.TABLE]
        upsert_data[self.constants.COLUMNS] = self.get_columns_format(data)
        upsert_data[self.constants.DATA] = json.dumps(data).replace(";", "")
        upsert_data[self.constants.PRIMARY_KEY] = data_destination[self.constants.PRIMARY_KEY]
        upsert_data[self.constants.ON_DUPLICATE_FORMAT] = self.get_on_duplicate_format(data)
        return upsert_data

    def execute_upsert(self, bd_config, upsert_data):
        postgres_instance = postgresql.Connector(bd_config)
        resp = postgres_instance.execute_multiple_queries_select_dict_response(self.procedures.upsert_generic_post_gre,
                                                      params=upsert_data)
        _, response = (resp[0])  # every record has to affectations insert and update
        response = response // 2 if response > 1 else response
        return response

    @staticmethod
    def get_on_duplicate_format(data):
        attr = "{attr} = excluded.{attr},"
        val = ""
        for c in data[0]:
            val += attr.format(attr=c)
        val = val[:-1]
        return val
