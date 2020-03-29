import DataFlow.DataDestination.DB.mysql as mysql
import DataFlow.DataDestination.DB.postgresql as postgresql
import Infrastructure.s3 as s3
import json
import DataFlow.DataDestination.constants as constants
import DataFlow.constants as dataflow_constants
import DataFlow.Descriptor.credentials as credentials
import Infrastructure.log as log
import Infrastructure.files as files


class DataDestination:

    def __init__(self, name, s3_config):
        self.logger = log.get_logger("{} DATA SOURCE".format(name))
        self.s3_client = s3.S3Client(s3_config)
        self.Mysql = mysql.DB()
        self.Postgresql = postgresql.DB()

    def upsert_data(self, data, data_destination, records_states):
        source = data_destination[constants.SOURCE]
        if source == constants.MYSQL:
            return self.upsert_data_db(data, data_destination, records_states, self.Mysql.get_format, self.Mysql.execute_upsert)
        elif source == constants.POSTGRESQL:
            return self.upsert_data_db(data, data_destination, records_states,
                                self.Postgresql.get_format, self.Postgresql.execute_upsert)
        else:
            self.logger.info("The source {} is not valid".format(source))
            return ['The source {} is not compatible'.format(source), False]

    def upsert_data_db(self, data, data_destination, records_states, db_format_func, db_execute_func):
        try:
            if len(data) > 0:
                bd_config = credentials.get_credential(data_destination[constants.CREDENTIALS])
                upsert_data = db_format_func(data, data_destination)
                response = db_execute_func(bd_config, upsert_data)
                return [{"upserted": response, "message": "success", "data_destination": data_destination[constants.TABLE]}, True]
            else:
                return [{"upserted": 0, "message": "Data does not have elements, possible validations filter it",
                        "data_destination": data_destination[constants.TABLE]},True]
        except Exception as ex:
            self.logger.info((str(ex)))
            for id in records_states:
                records_states[id].append({"state": dataflow_constants.ERROR__DB_UPSERT,
                                           "error": "An error inserting in DB {}: [{}]".format(data_destination[constants.TABLE], str(ex))})
            return [{"upserted": 0, "message": "Error upserting data in DB: {}".format(str(ex)),
                    "data_destination": data_destination[constants.TABLE]}, False]

    def upload_json(self, file_name, folder_name, data):
        folder_name = folder_name.replace("/", "")
        data = json.dumps(data).encode('latin-1')
        return self.s3_client.upload_json(folder_name, file_name, data)

    def set_as_processed_json(self, file_path):
        filename = files.get_file_name(file_path)
        file_path_new = str(file_path).replace("/" + filename, "_PROCESSED/{}".format(filename))
        self.s3_client.move_file(None, file_path, file_path_new)






