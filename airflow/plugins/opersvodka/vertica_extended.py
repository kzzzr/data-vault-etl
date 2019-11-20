from airflow.plugins_manager import AirflowPlugin
# from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.hooks.dbapi_hook import DbApiHook
from vertica_python import connect
from contextlib import closing
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import re
import os


class VerticaHookExtended(DbApiHook):
    """
    Interact with Vertica.
    """

    conn_name_attr = 'vertica_conn_id'
    default_conn_name = 'vertica_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns verticaql connection object
        """
        conn = self.get_connection(self.vertica_conn_id)
        conn_config = {
            "user": conn.login,
            "password": conn.password or '',
            "database": conn.schema,
            "host": conn.host or 'localhost'
        }

        if not conn.port:
            conn_config["port"] = 5433
        else:
            conn_config["port"] = int(conn.port)

        conn = connect(**conn_config)
        return conn

    def run(self, sql, autocommit=True, parameters=None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if isinstance(sql, str):
            sql = [sql]

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, autocommit)

            with closing(conn.cursor()) as cur:
                for s in sql:
                    if parameters is not None:
                        self.log.info("{} with parameters {}".format(s, parameters))
                        cur.execute(s, parameters)
                    else:
                        self.log.info(s)
                        try:
                            cur.execute(s)
                            ft = cur.fetchone()
                            cur.execute("COMMIT")
                            if ft is not None:                                
                                return ft[0], ''
                            else:
                                return 0, ''
                        except Exception as e:
                            errmsg = str(re.sub("'", "", e.error_response.error_message()))
                            self.log.error(errmsg)
                            return -1, errmsg

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()            
 
class VerticaOperatorExtended(BaseOperator):
    """
    1. Execute SQL script
    2. Log metadata to database    
    """
    
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#b4e0ff'

    @apply_defaults
    def __init__(self, sql, log_meta, vertica_conn_id='vertica_staging', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.log_meta = log_meta

        self.sql_path = os.path.join('/usr/local/airflow/libs/opersvodka/', sql)
        with open(self.sql_path, 'r') as fd:
            sql_file_contents = fd.read()
        self.sql = sql_file_contents
        
        self.sql_log_to_database = """
                 INSERT INTO {log_table} (LOAD_TS, MODE, OBJECT, STEP, ROWCOUNT, STATUS, DESCRIPTION)
                 SELECT GETDATE(), '{stage}', '{obj}', '{step}', {rowcount}, '{status}', '{descr}'::VARCHAR(2048)
                 ;
                 """

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = VerticaHookExtended(vertica_conn_id=self.vertica_conn_id)

        # hook.run(sql=self.sql)
        self.log_meta['rowcount'], error_message = hook.run(sql=self.sql)
        
        if self.log_meta['rowcount'] >= 0:
            hook.run(sql=self.sql_log_to_database.format(**self.log_meta))
        else:
            self.log_meta['status'] = 'ERR'
            self.log_meta['description'] = error_message
            hook.run(sql=self.sql_log_to_database.format(**self.log_meta))
        
        # hook.run(sql=self.sql_log_to_database.format(**self.log_meta))

# Defining the plugin class
class VertricaExtended(AirflowPlugin):
    name = "vertica_extended"
    operators = [VerticaOperatorExtended]
    # sensors = [PluginSensorOperator]
    # hooks = [PluginHook]
  