import logging
import sys
from Infrastructure import config, log
import DataFlow.orchestrator as orchestrator


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

# allow track all `logging` instances
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


logger = log.get_logger("THOR")


def get_config():
    config_path = sys.argv[1]
    return config.get_config(config_path)


def THOR_dataflow():
    config = get_config()
    data_flow = orchestrator.Flow(config)
    return data_flow.run()


if __name__ == "__main__":
    # load configuration
    logger.info("------------------------------------------------------------------")
    logger.info("starting THOR")
    logger.info("------------------------------------------------------------------")
    state = THOR_dataflow()
    logger.info("THOR started: {}".format(state))
