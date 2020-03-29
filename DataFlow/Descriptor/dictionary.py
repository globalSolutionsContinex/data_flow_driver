import DataBase.database as procedures
import Infrastructure.postgres as postgres

dictionary = {}
config = None


def get_dictionary(name):
    if name not in dictionary:
        pg = postgres.Connector(config.get('thor_database'))
        params = {'mapper_name': name}
        dictionary[name] = dict(pg.execute_with_results(procedures.dictionary, params))

    return dictionary[name]
