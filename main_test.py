import test.test_orchestrator as orchestrator_test
import test.Transformator.test_descriptor_mapping as descriptor_mapping_test
import test.DataDestination.test_controller as destination_controller_test
import test.test_metrics as metrics_test

if __name__ == "__main__":
    orchestrator_test.run_all_test()
    descriptor_mapping_test.run_all_test()
    destination_controller_test.run_all_test()
    metrics_test.run_all_test()

