import requests
import toml
import os
from os import environ
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import pandas as pd
import datetime
import re
from lxml import etree
from html import unescape
from io import StringIO

# GLOBAL CONFIG
_CONF_FILE_LOCATION = os.path.join(os.path.dirname(__file__), 'config.toml')


def load_excel(worker, file):
    worker.logger.info('load_excel_mock : START : BEGIN LOADING EXCEL FILES')
    file_config = toml.load(_CONF_FILE_LOCATION)['excel'][file]

    path = file_config['path']
    filename = file_config['filename']
    full_path = os.path.join(os.path.dirname(__file__), path, filename)
    sheet = file_config['sheet']
    target_schema = file_config['target_schema']
    target_table = file_config['target_table']
    target_full_name = f'{target_schema}.{target_table}'

    data = pd.read_excel(full_path, sheet, index_col=None)

    if not data.empty:
        worker._log_to_db(f'EXCEL', f'{target_table}', 'READ FILE', data.shape[0], 'OK',
                          f'LOAD SOURCE DATA - EXCEL - {full_path} - SHEET {sheet}')
    else:
        worker._log_to_db(f'EXCEL', f'{target_table}', 'READ FILE', data.shape[0], 'ERR',
                          f'DATASET EMPTY - LOAD SOURCE DATA - EXCEL - {full_path} - SHEET {sheet}')
        return

    worker.load_df_to_vertica(data, target_full_name)

    worker._log_to_db(f'EXCEL', f'{target_table}', 'LOAD FILE', data.shape[0], 'OK',
                      f'LOAD SOURCE DATA - EXCEL - {full_path} - SHEET {sheet}')


# def load_excel_mock(worker):
#     worker.logger.info('load_excel_mock : START : BEGIN LOADING EXCEL FILES')
#
#     config_excel = toml.load(_CONF_FILE_LOCATION)['excel']
#     for src, params in config_excel.items():
#         path = params['path']
#         filename = params['filename']
#         full_path = os.path.join(os.path.dirname(__file__), path, filename)
#         sheet = params['sheet']
#         target_schema = params['target_schema']
#         target_table = params['target_table']
#         target_full_name = f'{target_schema}.{target_table}'
#
#         data = pd.read_excel(full_path, sheet, index_col=None)
#
#         if not data.empty:
#             worker._log_to_db(f'SOURCE', f'{target_table}', 'EXCEL READ FILE', data.shape[0], 'OK',
#                               f'LOAD SOURCE DATA - EXCEL - {full_path} - SHEET {sheet}')
#         else:
#             worker._log_to_db(f'SOURCE', f'{target_table}', 'EXCEL READ FILE', data.shape[0], 'ERR',
#                               f'DATASET EMPTY - LOAD SOURCE DATA - EXCEL - {full_path} - SHEET {sheet}')
#             continue
#
#         worker.load_df_to_vertica(data, target_full_name)
#
#         worker._log_to_db(f'SOURCE', f'{target_table}', 'EXCEL LOAD FILE', data.shape[0], 'OK',
#                           f'LOAD SOURCE DATA - EXCEL - {full_path} - SHEET {sheet}')


def load_sap_mii(worker, report='STG_HQ_REPORT', shift='-1'):
    worker.logger.info('test_load_sap_mii : START : BEGIN LOADING SAP MII XML FILES')
    config_sapmii = toml.load(_CONF_FILE_LOCATION)['sapmii']['DataLake_report']

    request_body = config_sapmii['request_body']
    endpoint = config_sapmii['endpoint']
    target_schema = config_sapmii['target_schema']
    target_table = config_sapmii['target_table']
    target_full_name = f'{target_schema}.{target_table}'
    sap_usr_env = config_sapmii['sap_usr_env']
    sap_pwd_env = config_sapmii['sap_pwd_env']
    sap_usr = environ.get(sap_usr_env)
    sap_pwd = environ.get(sap_pwd_env)

    # FETCH
    content, status_code = _fetch_xml(request_body, endpoint, sap_usr, sap_pwd, report, shift)
    worker._log_to_db(f'SOURCE', f'{target_table}', 'FETCH SAP MII', 0, status_code,
                      f'FETCH SOURCE DATA - SAP MII - REPORT={report} - SHIFT={shift}')

    if status_code != 200:
        raise Exception(f'RESPONSE CODE = {status_code}')

    # PARSE
    data = _parse_xml(content)
    if not data.empty:
        worker._log_to_db(f'SOURCE', f'{target_table}', 'PARSE SAP MII', data.shape[0], 'OK',
                          f'PARSE SOURCE DATA - SAP MII - REPORT={report} - SHIFT={shift}')
    else:
        worker._log_to_db(f'SOURCE', f'{target_table}', 'PARSE SAP MII', 0, 'ERR',
                          f'DATASET EMPTY - LOAD SOURCE DATA - SAP MII - REPORT={report} - SHIFT={shift}')

    # LOAD
    worker.load_df_to_vertica(data, target_full_name)
    worker._log_to_db(f'SOURCE', f'{target_table}', 'LOAD SAP MII', data.shape[0], 'OK',
                      f'LOAD SOURCE DATA - SAP MII - REPORT={report} - SHIFT={shift}')


def load_sap_mii_mock(worker):
    worker.logger.info('test_load_sap_mii : START : BEGIN LOADING SAP MII XML FILES')

    config_sapmii = toml.load(_CONF_FILE_LOCATION)['sapmii']
    for src, params in config_sapmii.items():
        report = params['report']
        shift = params['shift']
        target_schema = params['target_schema']
        target_table = params['target_table']
        target_full_name = f'{target_schema}.{target_table}'

        # MOCK
        filepath = os.path.join(os.path.dirname(__file__), params['mock_file'])
        data = pd.read_csv(filepath, sep=',', header=0, index_col=None)

        # LOAD
        worker.load_df_to_vertica(data, target_full_name)
        worker._log_to_db(f'SOURCE', f'{target_table}', 'LOAD SAP MII', data.shape[0], 'OK',
                          f'LOAD SOURCE DATA - SAP MII - REPORT={report} - SHIFT={shift}')


def _fetch_xml(request_body, endpoint, sap_usr, sap_pwd, report, shift):
    request_body = request_body.format(**locals())
    request_body = request_body.encode('utf-8')

    # Preparing request
    session = requests.session()
    session.headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": "xmii:XacuteRequest"}
    session.headers.update({"Content-Length": str(len(request_body))})

    # Prepare retry in case of timeout
    retries = Retry(total=1, backoff_factor=0.8, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))

    # Sending request
    response = session.get(url=endpoint, data=request_body, verify=False, timeout=800)

    # Returning result code and contents
    return response.content, response.status_code

def _parse_xml(content):
    """
    Unescape characters in response

    :param content:
    :return:
    """
    unesc = _unescape_xml(content)

    # Save XML to file (for debugging)
    # self._save_xml(unesc)

    pattern = re.compile("^MENGE.*$")
    root = etree.parse(StringIO(unesc)).getroot()

    df_cols_fact = ["SCENARIO", "BUSINESS_DT", "DT", "PRODUCT_ID", "CAT", "VALUE"]
    df_fact = pd.DataFrame(columns=df_cols_fact)

    columnmap = {'PRODUCT_ID': 'PLANT_PRODUCT_ID',
                 'БП': 'BP',
                 'ПЛАН': 'PLAN',
                 'ППР': 'PPR',
                 'ПРОГНОЗ': 'FORECAST',
                 'ФАКТ': 'FACT'
                 }

    for a in root.iterfind("./{http://www.sap.com/abapxml}values/TAB/DTAB01/item"):
        scenario = root.xpath('.//ZTPP_PARAMS[PARAM_NAME="HTML_REPORT_INFO"]/SCENARIO')[0]
        biz_dt = root.find('.//BDATE')
        dt = a.find("SPTAG_START")
        cat = a.find("_--40SRC_ID")
        for b in a:
            if pattern.match(b.tag):
                tag = b.tag.replace("_--2D", "-")
                df_fact = df_fact.append(
                    pd.Series([scenario.text, biz_dt.text, dt.text, tag, cat.text, b.text], index=df_cols_fact),
                    ignore_index=True)

    df_fact = df_fact.pivot_table(index=["SCENARIO", "BUSINESS_DT", "DT", "PRODUCT_ID"], columns="CAT", values="VALUE",
                                  aggfunc='first')
    df_fact.reset_index(inplace=True)
    df_fact.rename(columns={k: v for k, v in columnmap.items() if k in df_fact}, inplace=True)
    df_fact.loc[:, 'PLANT_PRODUCT_ID'] = "[" + df_fact.loc[:, 'PLANT_PRODUCT_ID'] + "]"
    return df_fact


def _unescape_xml(content):
    """
    Parse response - get contents

    :param content:
    :return:
    """
    root = etree.fromstring(content)
    for a in root.iterfind("./{http://schemas.xmlsoap.org/soap/envelope/}Body/{http://www.sap.com/xMII}XacuteResponse/\
    {http://www.sap.com/xMII}Rowset/{http://www.sap.com/xMII}Row/"):
        unesc = unescape(a.text)
    return unesc


def _save_xml(content):
    """
    Save result to xml

    :param content:
    :return:
    """
    filename = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    with open(filename + ".xml", "w", encoding="utf-8") as text_file:
        print(content, file=text_file)
