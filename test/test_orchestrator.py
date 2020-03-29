import time

import DataFlow.orchestrator as orchestrator
import Infrastructure.s3 as s3
import test.Descriptor.scenario_1 as scenario_1
from Infrastructure import config


def get_config():
    config_path = "config/staging.toml"
    return config.get_config(config_path)


def get_scenario():
    s1 = scenario_1.get_descriptor()
    return s1


def set_up_hulk(descriptor_test=None):
    integrator = orchestrator.Flow(get_config())
    if descriptor_test:
        integrator.descriptor.descriptors = [descriptor_test]
    else:
        integrator.descriptor.descriptors = [integrator.descriptor.descriptors[0]]
    return integrator


def test_pipelines_kill():
    print("test_pipelines_kill")
    descriptor_test = get_scenario()
    integrator = set_up_hulk(descriptor_test)
    state = integrator.run()
    descriptor_test['active'] = False
    descriptor_test['kill'] = True
    assert state
    time.sleep(descriptor_test['seconds'] * 2)
    descriptor_test['kill'] = True
    assert descriptor_test['num_executions'] == 0
    assert descriptor_test['num_error_executions'] == 0

    print("test_pipelines_kill ok")


def test_pipelines_active():
    print("test_pipelines_active")
    descriptor_test = get_scenario()
    integrator = set_up_hulk(descriptor_test)
    descriptor_test['active'] = True
    descriptor_test['kill'] = False
    descriptor_test['is_running'] = True
    state = integrator.run()
    assert state
    time.sleep(descriptor_test['seconds'] * 3)
    descriptor_test['kill'] = True
    assert descriptor_test['num_executions'] >= 1
    assert descriptor_test['num_error_executions'] == 0
    print("test_pipelines_active ok")


def test_orchestrator_fatal_error():
    print("test_orchestrator_fatal_error")
    descriptor_test = get_scenario()
    integrator = set_up_hulk(descriptor_test)
    descriptor_test['active'] = True
    descriptor_test['kill'] = False
    descriptor_test['is_running'] = True
    descriptor_test['data_source'] = None
    state = integrator.run()
    assert state
    time.sleep(descriptor_test['seconds'] * 3)
    descriptor_test['kill'] = True
    assert descriptor_test['num_error_executions'] >= 1
    print("test_orchestrator_fatal_error ok")


def test_complete_flow():
    print("test_complete_flow")
    integrator = set_up_hulk()
    descriptor_test = integrator.descriptor.descriptors[0]
    descriptor_test['data_source']['prefix'] = 'test/upsert_pricestock_method/J_PVP'
    s3_client = s3.S3Client(get_config()['s3'])
    list_p = s3_client.get_pending_files("test/upsert_pricestock_method_PROCESSED/", ".json")
    for li in list_p:
        s3_client.move_file(None, li, li.replace('_PROCESSED/', '/'))
    descriptor_test['active'] = True
    descriptor_test['kill'] = False
    descriptor_test['is_running'] = True
    state = integrator.run()
    assert state
    time.sleep(descriptor_test['seconds'] * 3)
    assert descriptor_test['num_executions'] >= 1
    assert descriptor_test['num_error_executions'] == 0

    while descriptor_test['num_executions'] <= 1:
        pass
    descriptor_test['kill'] = True
    list_p = s3_client.get_pending_files("test/upsert_pricestock_method_PROCESSED/", ".json")
    assert len(list_p) > 0
    print("test_complete_flow ok")


def run_all_test():
    test_pipelines_kill()
    test_pipelines_active()
    test_orchestrator_fatal_error()
    test_complete_flow()
