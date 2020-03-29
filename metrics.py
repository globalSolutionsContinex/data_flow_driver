from Infrastructure import config, s3
import datetime
import sys
import statistics


class Metrics:

    def __init__(self):
        self.config = None

    def get_config(self):
        config_path = sys.argv[1]
        self.config = config.get_config(config_path)

    def get_median_rate_records_per_second(self, iterations):

        configi = self.config

        s3_config = configi['s3']

        s3_client = s3.S3Client(s3_config)

        files = s3_client.get_pending_files("file_states", ".json")
        rates = []
        median = 0
        sum_size = 0
        for count, f in enumerate(files):
            data = s3_client.download_json(f)
            key = f.replace('file_states/', '')
            initial = data[key][0]
            final = data[key][2]
            if final['size'] > 0:
                size = final['size']
                format = "%Y-%m-%d %H:%M:%S.%f"
                date_i = datetime.datetime.strptime(initial['date'], format)
                date_f = datetime.datetime.strptime(final['date'], format)
                seconds_diff = (date_f - date_i).total_seconds()
                rate = size//seconds_diff
                rates.append(rate)
                sum_size += size
                median = statistics.median(rates)
                print("Median: {} Number of files: {}".format(median, len(rates)))
                print(sum_size)
            if count >= iterations:
                break

        return [median, len(rates), sum_size]
