import metrics
from Infrastructure import config


def test_rate_records_per_second():
    metrics_instance = metrics.Metrics()
    metrics_instance.config = config.get_config('config/production.toml')
    median, rates_size, sum_size = metrics_instance.get_median_rate_records_per_second(100)
    assert median >= 2810.0 and rates_size >= 94 and sum_size >= 272407


def run_all_test():
    test_rate_records_per_second()
