import DataFlow.DataDestination.DB.master as master
import Infrastructure.mysql as mysql


class DB(master.MasterDB):

    def __init__(self):
        super().__init__()

    def get_format(self, data, data_destination):
        upsert_data = super().get_basic_format(data, data_destination)
        upsert_data[self.constants.TABLE] = data_destination[self.constants.TABLE]
        upsert_data[self.constants.COLUMNS] = self.get_columns_format(data)
        upsert_data[self.constants.DATA] = self.convert_to_insert_format(data)
        upsert_data[self.constants.ON_DUPLICATE_FORMAT] = self.get_on_duplicate_format(data)
        return upsert_data

    def execute_upsert(self, bd_config, data_destination, upsert_data):
        mysql_instance = mysql.Connector(bd_config, data_destination[self.constants.DATABASE])
        resp = mysql_instance.execute_multiple_queries_select_dict_response(self.constants.upsert_generic,
                                                                            params=upsert_data)
        _, response = (resp[0])  # every record has to affectations insert and update
        response = response // 2
        return response

