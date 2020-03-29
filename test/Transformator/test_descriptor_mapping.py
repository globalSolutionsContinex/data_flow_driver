from Infrastructure import config
import DataFlow.Transformator.descriptor_mapping as descriptor_mapping
import test.Descriptor.scenario_1 as scenario_1
import test.Descriptor.scenario_2 as scenario_2


def get_config():
    config_path = "config/staging.toml"
    return config.get_config(config_path)


def get_scenarios():
    return [scenario_1, scenario_2]


def set_up_descriptor_mapper(descriptor_test):
    descriptor_mapper = descriptor_mapping.CanonicalFormat(descriptor_test)
    return descriptor_mapper


def test_data_formatter():
    descriptor_test = get_scenarios()[0]
    descriptor = descriptor_test.get_descriptor()
    data_sets = descriptor['data_set']
    descriptor_mapper = set_up_descriptor_mapper(data_sets[0])
    print("test_data_formatter")
    records_states = {}
    canonical_data = descriptor_mapper.get_formatted_records(descriptor_test.data, records_states, "test")
    assert len(canonical_data) == 3
    assert canonical_data[0]['location'] == '571544d7e473209b066f3570'
    assert descriptor_test.data[1]['error'] is not None
    assert descriptor_test.data[2]['error'] is not None
    assert descriptor_test.data[3]['warning'] is not None
    assert canonical_data[2]['discount_percentage'] == 37.0
    del descriptor_mapper.ERROR
    canonical_data = descriptor_mapper.get_formatted_records(descriptor_test.data, records_states, "test")
    assert len(canonical_data) == 0
    assert str(descriptor_test.data[0]['error']).endswith("'CanonicalFormat' object has no attribute 'ERROR'")
    assert str(records_states[descriptor_test.data[0]['id_hulk']][1]['error']).endswith("'CanonicalFormat' object has no attribute 'ERROR'")


def test_data_formatter_fatal_errors():
    descriptor_test = get_scenarios()[1]
    descriptor = descriptor_test.get_descriptor()
    data_sets = descriptor['data_set']
    descriptor_mapper = set_up_descriptor_mapper(data_sets[0])
    print("test_data_formatter_fatal_errors")
    records_states = {}
    canonical_data = descriptor_mapper.get_formatted_records(descriptor_test.data, records_states, "test")
    assert len(canonical_data) == 0
    assert descriptor_test.data[0]['error'] == "discount_percentage #### 'default'"
    assert records_states[descriptor_test.data[0]['id_hulk']][0]['error'] == "discount_percentage #### 'default'"
    del descriptor_mapper.descriptor
    canonical_data = descriptor_mapper.get_formatted_records(descriptor_test.data, records_states, "test")
    assert len(canonical_data) == 0
    assert descriptor_test.data[0]['error'] == "'CanonicalFormat' object has no attribute 'descriptor'"
    assert records_states[descriptor_test.data[0]['id_hulk']][1]['error'] == "'CanonicalFormat' object has no attribute 'descriptor'"


def run_all_test():
    test_data_formatter()
    test_data_formatter_fatal_errors()


