import os
import sys
import toml
import logging
from os import environ
import re
import errno
import datetime
import pandas as pd
import vertica_python as vp


class Worker:
    """
    Class to perform actions with Database
    """

    _CONF_FILE_LOCATION = os.path.join(os.path.dirname(__file__), 'config.toml')

    def execute_scripts(self, mode):
        self.logger.info(f'{__file__} : START EXECUTING STEP : {mode}')

        rootdir = self.config['project_directory']
        metadir = self.config['metadir']

        print(f'rootdir = {rootdir}')

        self.logger.info(f'{__file__} : ROOTDIR : {rootdir}')

        for _, folder in enumerate(sorted(os.listdir(rootdir))):
            subdir = os.path.join(rootdir, folder, mode)
            folder_pattern = re.compile(metadir[mode]['RE'])
            if folder_pattern.match(folder):
                sf = os.path.join(subdir, 'sequence')
                with open(sf, 'r') as seq:
                    for filecounter, file in enumerate(seq):
                        file = file.strip()
                        filepath = os.path.join(subdir, file)
                        with open(filepath, 'r', encoding='utf-8') as fp:
                            self.logger.info(f'{__file__} : START : executing script {file}')

                            stage = metadir[mode]['MODE']
                            obj = metadir[mode]['OBJECT'].format(**locals())[2:]
                            step = metadir[mode]['STEP'].format(**locals())[:-4]
                            descr = metadir[mode]['DESCRIPTION'].format(**locals())

                            script = fp.read()
                            rowcount, msg = self._run_sql(script)
                            if rowcount >= 0:
                                self._log_to_db(stage, obj, step, rowcount, 'OK', descr)
                                continue
                            else:
                                self._log_to_db(stage, obj, step, rowcount, 'ERR', msg)
                                break
            else:
                continue

    def __init__(self):
        # Load config
        self.config = self._get_settings()

        # Configure logger
        self.logger = self._configure_logger()

        self.vconn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.vconn is not None:
            del self.vconn
            self.vconn = None

    def recreate_schema(self):
        self._run_sql("DROP SCHEMA IF EXISTS STG_OPERSVODKA CASCADE ;")
        self._run_sql("CREATE SCHEMA IF NOT EXISTS STG_OPERSVODKA ;")
        self._run_sql("DROP SCHEMA IF EXISTS ODS_OPERSVODKA CASCADE ;")
        self._run_sql("CREATE SCHEMA IF NOT EXISTS ODS_OPERSVODKA ;")

    def _get_settings(self, confFilepath=_CONF_FILE_LOCATION):
        """
        Get configurations from TOML file.
        Hardcoded to /config.toml
        """

        logging.info("worker - _get_settings() - Reading settings from config.toml")
        config = toml.load(confFilepath)

        # Get env variables values
        dbtag = config['dbtag']
        utype = config['server'][dbtag]['userType']
        ptype = config['server'][dbtag]['passwordType']

        if utype == 'env':
            uservar = config['server'][dbtag]['userEnvVar']
            config['server'][dbtag]['user'] = environ.get(uservar)

        if ptype == 'env':
            passwordvar = config['server'][dbtag]['passwordEnvVar']
            config['server'][dbtag]['password'] = environ.get(passwordvar)

        logging.info("getSettings(): Done")
        return config

    def _get_connection_parameters(self):
        """
        Возвращает словарь с параметрами соединения
        """
        self.logger.info(f'{__file__} : START EXECUTION : _get_connection_parameters')
        dbtag = self.config['dbtag']

        return {
            'host': self.config['server'][dbtag]['host'],
            'port': 5433,
            'user': self.config['server'][dbtag]['user'],
            'password': self.config['server'][dbtag]['password'],
            'database': self.config['server'][dbtag]['db'],
            'read_timeout': 600,
            # default throw error on invalid UTF-8 results
            'unicode_error': 'strict',
            # SSL is disabled by default
            'ssl': False,
            'connection_load_balance': True
        }

    def _get_connection(self):
        """
        Возвращает соединение с СУБД. Если оно удалось.
        """
        if self.vconn is None:
            try:
                pars = self._get_connection_parameters()
                self.logger.debug(f'{__file__} : START EXECUTION : _get_connection : {pars}')
                self.vconn = vp.connect(**pars)
                self.logger.info(f'{__file__} : START EXECUTION : _get_connection : CONNECTION OK')
            except Exception as e:
                self.logger.error("Error:", sys.exc_info()[1])
                # rethrow?
        return self.vconn

    def _get_copy_command_sql(self, columns: list, targetTable: str) -> str:
        res = """COPY {table}
                  ({header})
                  FROM LOCAL STDIN
                  DELIMITER '{delim}'
                  NULL '{nulv}'
                  TRAILING NULLCOLS
                  ABORT ON ERROR"""
        resf = res.format(
            table=targetTable,
            header=",".join(columns),
            delim='|',
            nulv='',
            rejfp='rejectedOnLoad.csv')
        self.logger.debug('_get_copy_command_sql: SQL\n {}'.format(resf))
        return resf

    def load_df_to_vertica(self, pdDf: pd.DataFrame, targetTable: str):
        """
        Основная функция загрузки, позволяет выбрать интерфейс вручную
        """
        cols = pdDf.columns

        self.logger.info('load_df_to_vertica : START : ColumnList={t}'.format(t=cols))

        copyCommand = "COPY {table} ({header}) FROM STDIN DELIMITER '{delim}'".format(
            table=targetTable,
            header=",".join(cols),
            delim='|'
        )
        self.logger.info('load_df_to_vertica : Prepare COPY command : text={t}'.format(t=copyCommand))
        self._load_df_to_vertica_with_copy(pdDf, copyCommand)

    def _load_df_to_vertica_with_vsql(self, pdDf: pd.DataFrame, vsqlCommand: str):
        from subprocess import Popen, PIPE, STDOUT

        self.logger.info("_load_df_to_vertica_with_vsql(): call")
        # self.truncateStage() # clears the stage table
        try:
            with Popen(
                    vsqlCommand,
                    stdin=PIPE,
                    stdout=PIPE,
                    stderr=STDOUT, encoding='utf-8') as p:
                ctr = 0
                for row in pdDf.itertuples():
                    # SLOPPY, should be revised
                    p.stdin.write(('|').join(list(map(str, row[1:]))) + '\n')
                    ctr += 1
                    if (ctr % 100000 == 0):
                        p.stdin.flush()
                p.stdin.flush()
                logging.info('Load done')
                stdout = p.communicate('\\.\n')
                p.kill()
        except BrokenPipeError as e:
            logging.error("Error:", repr(e))
        print(stdout)

    def _load_df_to_vertica_with_copy(self, pdDf: pd.DataFrame, copyCommand: str):
        from os import remove, path
        import csv

        self.logger.info("_load_df_to_vertica_with_copy(): call")
        tempfilename = f'{str(id(self))}'
        try:
            conn = self._get_connection()
            curs = conn.cursor()
            pdDf.to_csv(
                tempfilename,
                sep='|',
                quoting=csv.QUOTE_NONE,
                escapechar='\\',
                index=False,
                header=False,
                encoding='utf-8'  # encoding='cp1251'  # FIXME это костыль. на linux системе возможно будет нерабочим
            )
            with open(tempfilename, 'r') as f:
                self.logger.info("_load_df_to_vertica_with_copy(): COPY start")
                curs.copy(copyCommand, f, buffer_size=65536)
                self.logger.info("_load_df_to_vertica_with_copy(): COPY end")
            conn.commit()
        # except vp.errors.MessageError as m:
        #     print(m)
        #     raise Exception('STOP')
        except Exception as e:
            self.logger.error("ERROR : ", repr(e))
            raise
        finally:
            if path.isfile(tempfilename):
                remove(tempfilename)
            curs.close()
        self.logger.info("_load_df_to_vertica_with_copy(): exit")

    def _log_to_db(self, stage, obj, step, rowcount, status, descr, log_table='STG_OPERSVODKA.ETL_LOAD_LOG'):
        self.logger.info(f'OBJECT : {obj} - STEP : {step} - STATUS : {status} - ROWCOUNT : {rowcount} - DESCR : {descr}')
        stmt = f"""
                 INSERT INTO {log_table} (LOAD_TS, MODE, OBJECT, STEP, ROWCOUNT, STATUS, DESCRIPTION)
                 SELECT GETDATE(), '{stage}', '{obj}', '{step}', {rowcount}, '{status}', '{descr}'::VARCHAR(2048)
                 ;
                 """
        self._run_sql(stmt)

    def _run_sql(self, sqlCommand: str):
        self.logger.debug("_run_sql(): \n{}".format(sqlCommand))

        conn = self._get_connection()
        curs = conn.cursor()
        try:
            curs.execute(sqlCommand)
            ft = curs.fetchone()
            curs.execute("COMMIT")
            if ft is not None:
                rowcount = ft[0]
                return rowcount, ''
            else:
                return 0, ''
        except Exception as e:
            errmsg = str(re.sub("'", "", e.error_response.error_message()))
            self.logger.error(errmsg)
            return -1, errmsg
        finally:
            curs.close()

    def _configure_logger(self):
        """
        Declare and validate existence of log directory; create and configure logger object
        """
        # log_dir = os.path.join(os.path.dirname(__file__), self.config['log']['folder'])
        log_dir = os.path.join(os.path.expanduser("~"), self.config['log']['folder'])
        self._create_directory_if_not_exists(log_dir)
        self._configure_logging(log_dir)
        logger = logging.getLogger('importer_logger')
        return logger

    def _create_directory_if_not_exists(self, path):
        """
        Creates 'path' if it does not exist
        If creation fails, an exception will be thrown
        """
        try:
            os.makedirs(path)
        except OSError as ex:
            if ex.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                logging.error("Error:", repr(ex))
                raise

    def _configure_logging(self, path_to_log_directory):
        """
        Configure logger
        :param path_to_log_directory:  path to directory to write log file in
        """
        log_filename = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '.log'
        importer_logger = logging.getLogger('importer_logger')
        importer_logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')

        if not importer_logger.handlers:
            log_dir = os.path.join(path_to_log_directory, log_filename)
            print(log_dir)
            fh = logging.FileHandler(filename=log_dir)
            fh.setLevel(self.config['log']['log_level'])
            fh.setFormatter(formatter)
            importer_logger.addHandler(fh)

