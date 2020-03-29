import DataFlow.Descriptor.constants as constants
import DataFlow.Descriptor.descriptors as descriptors
import DataFlow.Descriptor.dictionary as dictionary


class Descriptor:

    def __init__(self):
        self.descriptors = descriptors.get_descriptors()
        dictionary.get_dictionary('cencosud')
        dictionary.get_dictionary('alkosto')
        dictionary.get_dictionary('homecenter')

    def load_pipelines(self, pipelines):
        for descriptor in self.descriptors:
            self.add_basics(descriptor)
            pipelines[descriptor[constants.NAME]] = descriptor

    @staticmethod
    def add_basics(descriptor):
        descriptor[constants.INTERVAL] = None
        descriptor[constants.IS_RUNNING] = False
        descriptor[constants.NUM_EXECUTIONS] = 0
        descriptor[constants.NUM_ERROR_EXECUTIONS] = 0
