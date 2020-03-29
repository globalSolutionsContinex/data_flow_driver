import pymysql
from Infrastructure import log
logger = log.get_logger("MySQL")


class Connector:
    def __init__(self, config, db):
        self.host = config['hostname']
        self.user = config['username']
        self.password = config['password']
        self.db = db
        self.connection = None
        self.pattern_mysql_pattern = "{:%Y-%m-%d %H:%M:%S}"

    def connect(self):
        i = 1
        while not self.connection:
            try:
                self.connection = pymysql.connect(host=self.host, user=self.user, passwd=self.password, db=self.db)
                cursor = self.connection.cursor()
                cursor.execute('SET autocommit = 0;')
            except Exception as e:
                i += 1
                logger.info("Error mysql connection " + str(e))
                logger.info("Connect Mysql " + str(i))

            if i > 10:
                break

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query_fetchall_dic(self, command):
        self.connect()
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(command)
        data = list(cursor.fetchall())
        rowcount = cursor.rowcount
        self.connection.commit()
        cursor.close()
        return [data, rowcount]

    def execute_multiple_queries_select_dict_response(self, store_procedure, params={}):
        procedure = open(store_procedure, 'r').read()
        sql_command = procedure.format(**params)
        sqllist = sql_command.split(";")[:-1]
        selects = []
        for sql_c in sqllist:
            selected = self.execute_query_fetchall_dic(sql_c)
            selects.append(selected)
        return selects




