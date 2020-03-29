import DataBase.database as procedures
import Infrastructure.postgres as postgres

dictionary = {}
config = None


def get_descriptors():
    pg = postgres.Connector(config.get('thor_database'))
    descriptors = pg.execute_with_results(procedures.descriptors, as_dict=True)

    return descriptors
