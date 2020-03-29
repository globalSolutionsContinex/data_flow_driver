import psycopg2
import psycopg2.extras

from Infrastructure import log

logger = log.get_logger("Postgres")


class Connector:
    def __init__(self, config):
        self.host = config['hostname']
        self.database = config['database']
        self.user = config['username']
        self.password = config['password']
        self.connection = None

    def connect(self):
        i = 1
        while not self.connection:
            try:
                self.connection = psycopg2.connect(host=self.host,
                                                   database=self.database,
                                                   user=self.user,
                                                   password=self.password)
            except Exception as e:
                i += 1
                logger.info("Error postgres connection " + str(e))
                logger.info("Connect postgres " + str(i))

            if i > 10:
                break

    def execute_with_results(self, query, params={}, as_dict=False):
        query = query.format(**params)
        self.connect()
        if as_dict:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cursor = self.connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        self.connection.commit()
        cursor.close()
        self.close()
        if as_dict:
            data = list(map(lambda r: dict(r), data))

        return data

    def execute_with_results_generic(self, query):
        self.connect()
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query)
        rowcount = cursor.rowcount
        try:
            data = list(cursor.fetchall())
        except Exception as ex:
            data = []
        self.connection.commit()
        cursor.close()
        return [data, rowcount]

    def execute_multiple_queries_select_dict_response(self, store_procedure, params={}):
        procedure = open(store_procedure, 'r').read()
        sql_command = procedure.format(**params)
        sqllist = sql_command.split(";")[:-1]
        selects = []
        for sql_c in sqllist:
            selected = self.execute_with_results_generic(sql_c)
            selects.append(selected)
        return selects

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None
