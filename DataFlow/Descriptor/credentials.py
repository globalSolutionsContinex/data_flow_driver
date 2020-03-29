import DataBase.database as procedures
import Infrastructure.postgres as postgres

credentials = {}
config = None


def get_credential(name):
    if name not in credentials:
        pg = postgres.Connector(config.get('thor_database'))
        params = {'credential_name': name, 'key': config['credentials']['key']}
        credentials[name] = pg.execute_with_results(procedures.credential, params)[0][0]

    return credentials[name]
