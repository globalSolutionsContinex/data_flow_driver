import DataFlow.DataDestination.constants as constants
import DataFlow.DataDestination.StoreProcedures.procedures as procedures


class MasterDB:

    def __init__(self):
        self.constants = constants
        self.procedures = procedures

    def get_basic_format(self, data, data_destination):
        upsert_data = {}
        self.delete_metadata(data)
        upsert_data[constants.TABLE] = data_destination[constants.TABLE]
        return upsert_data

    @staticmethod
    def delete_metadata(data):
        # temporal, the idea is that this attribute could be inserted in db
        for d in data:
            del d['id_thor']

    @staticmethod
    def get_columns_format(data):
        val = ""
        for c in data[0]:
            val += "{},".format(c)
        val = "({})".format(val[:-1])
        return val

    @staticmethod
    def get_on_duplicate_format(data):
        attr = "{attr} = values({attr}),"
        val = ""
        for c in data[0]:
            val += attr.format(attr=c)
        val = val[:-1]
        return val

    @staticmethod
    def convert_to_insert_format(data):
        values = ""
        for d in data:
            val = ""
            for c in d:
                ty = type(d[c])
                if ty == int or ty == float:
                    val += "{},".format(d[c])
                else:
                    val += "'{}',".format(d[c])
            val = "({}),".format(val[:-1])
            values += val
        return values[:-1]