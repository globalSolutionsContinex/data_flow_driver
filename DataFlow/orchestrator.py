import threading

import DataFlow.Descriptor.constants as descriptor_constants
import DataFlow.Descriptor.controller as descriptor_controller
import DataFlow.Descriptor.credentials as credentials
import DataFlow.Descriptor.descriptors as descriptors
import DataFlow.Descriptor.dictionary as dictionary
import DataFlow.Pipeline.controller as pipeline_controller
from Infrastructure import log

logger = log.get_logger("ORCHESTRATOR")


class Flow:

    def __init__(self, config):
        credentials.config = config
        descriptors.config = config
        dictionary.config = config
        self.config = config
        self.logger = logger
        self.descriptor = descriptor_controller.Descriptor()
        self.pipelines = {}

    def run(self):
        self.descriptor.load_pipelines(self.pipelines)
        self.logger.info("Running Orchestrator THOR Pipelines: {}".format(len(self.pipelines)))
        for pipe_name in self.pipelines:
            pipe = self.pipelines[pipe_name]
            self.set_interval(self.run_pipeline, pipe[descriptor_constants.SECONDS],
                              pipe[descriptor_constants.NAME], pipe)
        return True

    def run_pipeline(self, pipe):
        if pipe[descriptor_constants.ACTIVE]:
            pipe[descriptor_constants.NUM_EXECUTIONS] += 1
            if not pipe[descriptor_constants.IS_RUNNING]:
                try:
                    msg_info = "{} INIT PIPE - NUM EXECUTIONS {} NUM ERROR EXECUTIONS {}"
                    logger.info(msg_info.format(pipe[descriptor_constants.NAME],
                                                pipe[descriptor_constants.NUM_EXECUTIONS],
                                                pipe[descriptor_constants.NUM_ERROR_EXECUTIONS]))
                    pipe[descriptor_constants.IS_RUNNING] = True
                    pipeline = pipeline_controller.Pipeline(pipe, self.config)
                    pipeline.execute_pipeline()
                except Exception as ex:
                    logger.info("{} Fatal Error: {} - {}".format(pipe[descriptor_constants.NAME], str(ex),
                                                                 self.get_error_code_line(ex.__traceback__)))
                    pipe[descriptor_constants.NUM_ERROR_EXECUTIONS] += 1
                finally:
                    pipe[descriptor_constants.IS_RUNNING] = False

    def set_interval(self, func, sec, name, descriptor):
        # each interval run in a different thread
        def func_wrapper():
            func(descriptor)
            self.set_interval(func, sec, name, descriptor)

        t = threading.Timer(sec, func_wrapper)
        # if thread already exist
        if not descriptor[descriptor_constants.KILL]:
            self.pipelines[name]['interval'] = t
            t.start()

    def get_error_code_line(self, trace):
        if trace.tb_next:
            trace_msg = self.get_error_code_line(trace.tb_next)
            return "CodeLine: {} --> [{}]".format(trace.tb_lineno, trace_msg)
        return "CodeLine: {}".format(trace.tb_lineno)
