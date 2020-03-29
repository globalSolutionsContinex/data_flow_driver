import Infrastructure.s3 as s3
import DataFlow.DataSource.constants as constants


class DataSource:

    def __init__(self, data_source, s3_config):
        self.data_source = data_source
        self.s3_client = s3.S3Client(s3_config)

    def get_pending_files(self):
        pending_files = self.s3_client.get_pending_files(self.data_source[constants.PREFIX],
                                                         self.data_source[constants.SUFFIX])
        return pending_files

    def get_data_file(self, file_name):
        return self.s3_client.download_json(file_name)
