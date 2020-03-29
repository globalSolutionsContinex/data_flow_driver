import boto3
import boto3.session
import json

import logging
logging.getLogger('boto').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)


class S3Client:

    def __init__(self, s3_config):
        self.key = s3_config['accessKey']
        self.secret = s3_config['secretKey']
        self.bucket_name = s3_config['bucketName']
        session = boto3.session.Session()
        self.client = session.client(
            's3',
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret
        )
        self.resource = boto3.resource(
            's3',
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret)

        logging.getLogger('boto').setLevel(logging.CRITICAL)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)

    def get_url_in_s3(self, key_name):
        return "https://s3-us-west-2.amazonaws.com/" + self.bucket_name + "/" + key_name

    def get_pending_files(self, prefix, suffix):
        request = self.client.list_objects(Bucket=self.bucket_name, Prefix=prefix)
        pending_files = request['Contents'] if 'Contents' in request else []
        today_files = []
        for file in pending_files:
            if str(file['Key']).endswith(suffix):
                today_files.append(file['Key'])
        return today_files

    def move_file(self, client, file_path, destiny_path):
        self.resource.Object(self.bucket_name, destiny_path).copy_from(
            CopySource="{}/{}".format(self.bucket_name, file_path))
        self.resource.Object(self.bucket_name, file_path).delete()

    def upload_json(self, folder_name, file_name, data):
        key_name = folder_name + "/" + file_name
        self.client.put_object(Body=bytes(data), Bucket=self.bucket_name, Key=key_name, ContentType='application/json')
        url_in_s3 = self.get_url_in_s3(key_name)
        return url_in_s3

    def download_json(self, key_name):
        data = self.resource.Object(self.bucket_name, key_name).get()['Body'].read().decode('latin-1')
        json_data = json.loads(data)
        return json_data
