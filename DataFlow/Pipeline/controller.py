import datetime as datetime

import DataFlow.DataDestination.constants as destination_constants
import DataFlow.DataDestination.controller as destination_controller
import DataFlow.DataSource.controller as data_source_controller
import DataFlow.Descriptor.constants as descriptor_constants
import DataFlow.Transformator.descriptor_mapping as descriptor_mapping
import DataFlow.constants as dataflow_constants
from Infrastructure import log


class Pipeline:

    def __init__(self, pipe, config):
        self.pipe = pipe
        self.data_source = data_source_controller.DataSource(self.pipe[descriptor_constants.DATA_SOURCE], config['s3'])
        self.data_destination = destination_controller.DataDestination(self.pipe[descriptor_constants.NAME],
                                                                       config['s3'])
        self.logger = log.get_logger("{} DATA SOURCE".format(self.pipe[descriptor_constants.NAME]))
        self.MAX_FILES_PER_RUN = 5

    def execute_pipeline(self):
        # extract new data from data source
        self.logger.info("data source get pending files")
        pending_files = self.data_source.get_pending_files()
        count_files = 0
        for file_name in pending_files:
            try:
                self.logger.info("data source get data file {}".format(file_name))
                records = self.data_source.get_data_file(file_name)
                # transform data
                records_formatted = records
                # format in all data sets created
                self.logger.info("descriptor mapping formatting")
                records_states = {}
                file_state = {}
                file_state[file_name] = []
                for i, data_set_descriptors in enumerate(self.pipe[descriptor_constants.DATA_SET]):
                    descriptor_mapper = descriptor_mapping.CanonicalFormat(data_set_descriptors)
                    records_formatted = descriptor_mapper.get_formatted_records(records_formatted,
                                                                                records_states, file_name)
                    self.logger.info("data_set {}: size: {}".format(str(i), str(len(records_formatted))))
                    file_state[file_name].append(self.set_file_state_format("data_set", i, records_formatted))

                for destination in self.pipe[descriptor_constants.DATA_DESTINATION]:
                    self.logger.info(
                        "descriptor destination mapping {}".format(destination[destination_constants.TABLE]))
                    # finally formatted in data destination format
                    descriptor_mapper = descriptor_mapping.CanonicalFormat(
                        destination[destination_constants.DESTINATION_DESCRIPTOR])
                    records_destination = descriptor_mapper.get_formatted_records(records_formatted,
                                                                                records_states, file_name)
                    # load in data destinations
                    self.logger.info(
                        "Upserting data in destination {} size: {}".format(destination[destination_constants.TABLE],
                                                                           str(len(records_destination))))

                    file_state[file_name].append(self.set_file_state_format("destination",
                                                                            destination[destination_constants.TABLE],
                                                                            records_destination))
                    response, continue_destinations = self.data_destination.upsert_data(records_destination,
                                                                                        destination, records_states)
                    file_state[file_name].append(self.set_file_state_format("bd", response, records_destination))
                    self.logger.info(str(response))
                    count_files += 1
                    if not continue_destinations:
                        break
                self.logger.info("Uploading states")
                self.data_destination.upload_json(file_name,
                                                  dataflow_constants.RECORD_STATES_FOLDER, records_states)
                self.data_destination.upload_json(file_name,
                                                  dataflow_constants.FILES_STATES_FOLDER, file_state)
                self.data_destination.set_as_processed_json(file_name)
                self.logger.info("Pipeline Finish: success")
                if count_files >= self.MAX_FILES_PER_RUN:
                    self.logger.info("Max files, rebooting")
                    break

            except Exception as ex:
                self.logger.info("Pipeline Finish: error")
                self.logger.info("Error in filename {} : {}".format(file_name, str(ex)))
                self.logger.exception(ex)

    @staticmethod
    def set_file_state_format(destination_name, destination, records_formatted):
        update_date = str(datetime.datetime.now())
        return {destination_name: destination, "size": len(records_formatted), "date": update_date}
