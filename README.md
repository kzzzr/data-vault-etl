# ОПЕРСВОДКА 

# Схема процесса

Составляем доступные модули (блоки) в последовательность:
* Заполнение файла ФМД (вручную, excel)
* Генерация кода
* Загрузка источников в буферный слой
* Загрузка детального слоя хранилища
* Расчет Витрин
* Расчет сценариев Качества Данных
* Создание графов для Airflow (DAGs)

# Заполнение файла [ФМД](./meta/opersvodka/fmdOPERSVODKA.xlsx)

В файле 2 листа для генерации буферного слоя (stage & ods) и детального слоя

Пример: **[fmdOPERSVODKA.xlsx](./meta/opersvodka/fmdOPERSVODKA.xlsx)**

## Лист STAGE

Генерируем таблицы буферного слоя
![stage](/images/stage.png)

Stage:
* StageSchema - наименование схемы
* StageTable - наименование таблицы
* StageColumn - колонка
* StageColumnType - тип
* ColumnProperties - свойства колонки
* ColumnOrderBy - порядок сортировки (для проекции)
* DefaultValue - значение по умолчанию

ODS (опционально):
* ODSSchema - наименование схемы
* ODSTable - наименование таблицы

## Лист DDS

Задаем маппинг полей на структуры детального слоя (Data Vault)

![dds](/images/dds.png)

Target (применик):
* TargetType - тип сущности (HUB, LINK, SATELLITE)
* TargetSchema - схема
* TargetTable - таблица
* TargetColumnType - тип колонки (KEY, ATTRIBUTE)

Source (источник - буферный слой):
* SourceSchema - схема
* SourceTable - таблица
* SourceColumn - колонка 
* SourceColumnType - тип колонки

References - связи и ссылки (LINK - HUB, SATELLITE - HUB):
* ReferenceSchema - схема
* ReferenceTable - таблица

# [Генерация кода](./etlfw/generator)

Модуль: **[generator](./etlfw/generator)**

* Вход: Файл ФМД .xlsx, файл конфигурации config.toml
* Используются: Шаблоны скриптов и значения из ФМД
* Выход: Структура директорий с готовыми скриптами .sql

Входная точка - функция `launch(worker)`

## Конфигурация

Функция `_read_fmd()`

Пример файла **[/etlfw/generator/config.toml](./etlfw/generator/config.toml)**:

```toml
fmd_file_path = '../../META/opersvodka/fmdOPERSVODKA.xlsx' # path to fmd
scripts_path = '../../DWH' # path to generated scripts
project = 'OPERSVODKA/' # project name

sql_templates_file = './sql_templates.toml' # sql templates file
airflow_templates_file = './airflow_templates.toml' # airflow templates file

folder_marts = '/Data/002-opersvodka/6-MART/' # Data Marts scripts source
folder_dq = '/Data/002-opersvodka/6-MART//' # Data Quality scripts source
````

## Генерация скриптов метаданных
`_gen_meta(fmd_stage)`

 * Лог загрузки
 * Таблица со списком файлов, загруженных в буферный слой

## Генерация скриптов для буферного слоя
`_gen_stage(stage_table_name, stage_table_df)`

## Генерация скриптов для детального слоя
`_dds_gen(fmd_dds, stage_table_name)`

## Генерация скриптов для Витрин
`_gen_mart()`

Скрипты для витрин готовим заранее. Функция только скопирует готовые скрипты и поместит в общий репозиторий.

## Генерация скриптов для Качества Данных
`_gen_dq()`

Скрипты для КД готовим заранее. Функция только скопирует готовые скрипты и поместит в общий репозиторий.

## Генерация скриптов для Airflow
`_gen_airflow_dags()` с параметрами `'DDL', 'TRUNCATE', 'DML'`:
* DDL - граф для создания всех структур с учетом правильной последовательности
* TRUNCATE - очистка буферных таблиц
* DML - загрузка детального слоя хранилища

**Готовые `tasks` airflow необходимо собрать в последовательности вручную**

# Результирующий набор скриптов

Директория с сгенерированными скриптами: **[/DWH/OPERSVODKA](./DWH/OPERSVODKA)**:

![stage](/images/repo-tree.png)

Папки отсортированы по слоям в порядке для правильного исполнения
* 0-META - структуры метаданных и логирование
* 1-* - буферный слой (STAGE)
* 2-* - накопительный слой (ODS)
* 3-* - детальный слой HUBS
* 4-* - детальный слой LINKS
* 5-* - детальный слой SATELLITES
* 6-* - витрины (MARTS)
* 7-* - качество данных (DQ)

Внутри каждой директории есть поддиректории, соответствующие режимам запуска (Modes):
* DDL - создать объекты
* TRUNCATE - очистить буферный слой
* DML - заполнить детальный слой хранилища
* MART - расчет витрин
* DQ - расчет сценариев Качества Данных

# Загрузка данных из источников

Модули: **[etlfw/source](./etlfw/source)** и **[etlfw/vertica](./etlfw/vertica)**

* Вход: Конфигурация для источников, конфигурация для Vertica
* Используются: Функции / запросы под каждый источник, работа с приемником (Vertica)
* Выход: Заполненные таблицы буферного слоя

## Конфигурация для приемника (Vertica)

Модуль `etlfw/vertica/worker.py`, Функция `_get_settings(self, confFilepath=_CONF_FILE_LOCATION)`

Пример файла **[/etlfw/vertica/config.toml](./etlfw/vertica/config.toml)**:
```toml
# DEFAULT VERTICA DATABASE SERVER TAG
dbtag = "dev"

# Путь к скриптам проекта
project_directory = "/DWH/OPERSVODKA"

# VERTICA servers connection params
[server]
  [server.dev]
  host = "10.2.75.8"
  db = "devdb"
  userType = "env"
  user = ""
  userEnvVar = "PY_VE_USER"
  passwordType =  "env"
  password = ""
  passwordEnvVar = "PY_VE_PASSWORD"

# Logging params
[log]
folder = "airflow/etlfw/LOGS"
log_level = "INFO"
````

## SAP MII

Модуль `etlfw/source/load_source.py`, Функция `load_sap_mii(worker, report='STG_HQ_REPORT', shift='-1')`

Загрузка происходит в несколько этапов:
* Запрос к источнику - `_fetch_xml(request_body, endpoint, sap_usr, sap_pwd, report, shift)`
* Парсинг ответа - `_parse_xml(content)`
* Загрузка данных в Vertica - `worker.load_df_to_vertica(data, target_full_name)`

Пример файла конфигурации для SAP MII **[/etlfw/source/config.toml](./etlfw/source/config.toml)**:
```toml
[sapmii]
    [sapmii.DataLake_report]
        target_schema = "STG_OPERSVODKA"
        target_table = "STG_STG_HQ_REPORT"
        endpoint = "http://sapmii.sibur.local:52000/XMII/SOAPRunner/DATA_LAKE/DataLake_report"
        report = "STG_HQ_REPORT"
        shift = "-1"
        sap_usr_env = "SAPLOGIN"
        sap_pwd_env = "SAPPASSWORD"
        mock_file = "../../META/opersvodka/XML_load/2019-05-24.csv" # Указатель на файл-заглушку для тестирования
        request_body = """
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xmii="http://www.sap.com/xMII">
               <soapenv:Header/>
               <soapenv:Body>
                  <xmii:XacuteRequest>
                     <!--Optional:-->
                     <xmii:LoginName>{sap_usr}</xmii:LoginName>
                     <!--Optional:-->
                     <xmii:LoginPassword>{sap_pwd}</xmii:LoginPassword>
                     <!--Optional:-->
                     <xmii:InputParams>
                        <!--Optional:-->
                        <xmii:iv_bdate_shift>{shift}</xmii:iv_bdate_shift>
                        <!--Optional:-->
                        <xmii:iv_report>{report}</xmii:iv_report>
                     </xmii:InputParams>
                  </xmii:XacuteRequest>
               </soapenv:Body>
            </soapenv:Envelope>
            """
````

## Excel

Модуль `etlfw/source/load_source.py`, Функция `load_excel(worker, file)`

Загрузка происходит в цикле по всем excel-источникам:
* Читаем файл конифгурации `toml.load(_CONF_FILE_LOCATION)['excel']`
* Читаем excel-файл - `pd.read_excel(full_path, sheet, index_col=None)`
* Загрузка данных в Vertica - `worker.load_df_to_vertica(data, target_full_name)`


Пример файла конфигурации для EXCEL **[/etlfw/source/config.toml](./etlfw/source/config.toml)**:
```toml
[excel]

    [excel.calendar]
        path = "../../META/opersvodka/XLS_load"
        filename = "data.xlsx"
        sheet = "CALENDAR"
        target_schema = "STG_OPERSVODKA"
        target_table = "STG_DUMMY_CALENDAR"
````

# Загрузка детального слоя хранилища

Модуль: **[etlfw/vertica](./etlfw/vertica)**

* Вход: Конфигурация для Vertica, Словарь метаданных для логирования
* Используются: Подключение к Vertica, Исполнение скриптов в цикле
* Выход: Заполненные таблицы детального слоя

## 

## Последовательное исполнение скриптов

Модуль `etlfw/vertica/worker.py`, Функция `execute_scripts(self, mode)`
Режимы выполнения скриптов (Modes):
* DDL - создать объекты
* TRUNCATE - очистить буферный слой
* DML - заполнить детальный слой хранилища

## Словарь метаданных для логирования

На каждое выполнение скрипта (итерацию цикла) в таблицу лога будет добавлена запись:

* LOAD_TS - временная метка
* MODE - режим исполнения скриптов (mode)
* OBJECT - объект (например, таблица)
* STEP - скрипт
* ROWCOUNT - количество строк
* STATUS - статус OK / ERROR
* DESCRIPTION - комментарий (сообщение об ошибке)

Пример **[/etlfw/vertica/config.toml](./etlfw/vertica/config.toml)**:
```toml
# Metadata context to log every action
[metadir]
    [metadir.DDL]
    MODE = 'DDL'
    OBJECT = '{folder}'
    STEP = '{file}'
    DESCRIPTION = 'CREATING OBJECT : {file}'
    RE = "[0-7]-.*"

    [metadir.TRUNCATE]
    MODE = 'TRUNCATE'
    OBJECT = '{folder}'
    STEP = '{file}'
    DESCRIPTION = 'TRUNCATE STAGE TABLE : {file}'
    RE = "1-.*"

    [metadir.DML]
    MODE = 'DML'
    OBJECT = '{folder}'
    STEP = '{file}'
    DESCRIPTION = 'LOAD DATA WAREHOUSE : {file}'
    RE = "[1-5]-.*"

    [metadir.MART]
    MODE = 'MART'
    OBJECT = '{folder}'
    STEP = '{file}'
    DESCRIPTION = 'LOAD DATA MART : {file}'
    RE = "6-.*"

    [metadir.DQ]
    MODE = 'DQ'
    OBJECT = '{folder}'
    STEP = '{file}'
    DESCRIPTION = 'DATA QUALITY : {file}'
    RE = "7-.*"
````

# Расчет Витрин

Модуль: **[etlfw/vertica](./etlfw/vertica)**

* Вход: Конфигурация для Vertica, Словарь метаданных для логирования
* Используются: Подключение к Vertica, Исполнение скриптов в цикле
* Выход: Заполненные таблицы витрин

## Последовательное исполнение скриптов

Модуль `etlfw/vertica/worker.py`, Функция `execute_scripts(self, mode)`
Режимы выполнения скриптов (Modes):
* MART - расчет витрин

# Расчет сценариев Качества Данных

Модуль: **[etlfw/vertica](./etlfw/vertica)**

* Вход: Конфигурация для Vertica, Словарь метаданных для логирования
* Используются: Подключение к Vertica, Исполнение скриптов в цикле
* Выход: Заполненные таблицы витрин

## Последовательное исполнение скриптов

Модуль `etlfw/vertica/worker.py`, Функция `execute_scripts(self, mode)`
Режимы выполнения скриптов (Modes):
* DQ - расчет качества данных

# Создание графов для Airflow (DAGs)

Генератор создает набор тасков с контекстом (для записи в лог):

**[DWH/OPERSVODKA/8-airflow](./DWH/OPERSVODKA/8-airflow)**

**ToDo**: Контекст вычислять динамически, а не прописывать хардкодом:

```python
OPERSVOD_STG_ETL_LOAD_LOG = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_ETL_LOAD_LOG',
    sql='DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.ETL_LOAD_LOG.sql',
    # Контекст оператора (то, что пишем в лог) - хардкод
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'META',
        'step' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.ETL_LOAD_LOG.sql'
    }
)
```

После этого их необходимо вручную расставить в последовательность исполнения.

## Готовые DAGs

* **[OPERSVODKA_CREATE_OBJECTS.py](./airflow/dags/opersvodka/OPERSVODKA_CREATE_OBJECTS.py)** - создание всех объектов
* **[OPERSVODKA_LOAD_SOURCE_EXCEL.py](./airflow/dags/opersvodka/OPERSVODKA_LOAD_SOURCE_EXCEL.py)** - загрузка справочников их Excel (единоразово)
* **[OPERSVODKA_LOAD_SOURCE_DAILY.py](./airflow/dags/opersvodka/OPERSVODKA_LOAD_SOURCE_DAILY.py)** - регулярная загрузка всех обновляемых источников (SAP MII, Excel с комментариями с завода)
* **[OPERSVODKA_LOAD_SOURCE_SAP_DAILY.py](./airflow/dags/opersvodka/OPERSVODKA_LOAD_SOURCE_SAP_DAILY.py)** - загрузка SAP MII (в случае, если нам нужно загрузить только этот источник, например, вручную)
* **[OPERSVODKA_DWH.py](./airflow/dags/opersvodka/OPERSVODKA_DWH.py)** - загрузка детального слоя
* **[OPERSVODKA_LOAD_SOURCE_ALL_MOCK.py](./airflow/dags/opersvodka/OPERSVODKA_LOAD_SOURCE_ALL_MOCK.py)** - загрузка файлов-заглушек (для тестирования)

![stage](/images/dag-list.png)

![stage](/images/dag-sequence.png)

## Airflow plugin - Vertica Extended

Расширение Vertica Operator для того, чтобы каждое действие в базе было залогировано.

```sql
self.sql_log_to_database = """
                 INSERT INTO {log_table} (LOAD_TS, MODE, OBJECT, STEP, ROWCOUNT, STATUS, DESCRIPTION)
                 SELECT GETDATE(), '{stage}', '{obj}', '{step}', {rowcount}, '{status}', '{descr}'::VARCHAR(2048)
                 ;
                 """
```                 


# FAQ - вопросы и ответы

## Куда пишутся логи?

Файл **[/etlfw/vertica/config.toml](./etlfw/vertica/config.toml)**:
```toml
# Logging params
[log]
folder = "airflow/etlfw/LOGS"
log_level = "INFO"
````

## Как отлаживаться (debugging)?
Собрать конструкцию (последовательность действий) в **[/etlfw/\_\_main\_\_.py](./etlfw/__main__.py)**
:
```python
def main():
    with Worker() as worker:
        generate_scripts(worker) # Сгенерировать скрипты

        recreate_schema(worker) # Удалить и создать схему
        execute_ddl(worker) # Создать структуры данных

        # Загрузить файлы-заглушки
        load_excel(worker, 'calendar')
        load_excel(worker, 'reference')
        load_excel(worker, 'comments_mock')
        load_sap_mii_mock(worker)

        load_dds(worker) # Загрузить детальный слой
        load_marts(worker) # Заполнить витрины
        load_dq(worker) # Заполнить КД
```

Запускать из командной строки по имени модуля:
```bash
python etlfw
```

Логи смотреть в файле логов или в самой базе Vertica:
```sql
SELECT * FROM STG_OPERSVODKA.ETL_LOAD_LOG ORDER BY LOAD_TS DESC ;
SELECT * FROM STG_OPERSVODKA.ETL_FILE_LOAD ORDER BY LOAD_TS DESC ;
```

## Как тестировать Airflow DAGs?

* Использовать docker-compose для Airflow + PostgreSQL
* Подмонтировать необходимые директории со скриптами внутрь контейнера
* Добавить подключение к Vertica (CLOUD, DEV, TEST)
* Запускать DAGs вручную из интерфейса или CLI

Docker-compose.yml:
```yml
version: '2.1'
services:
    postgres:
        image: postgres:9.6
        environment:
            - POSTGRES_USER=airflow
            - POSTGRES_PASSWORD=airflow
            - POSTGRES_DB=airflow

    webserver:
        image: puckel/docker-airflow:latest
        restart: always
        depends_on:
            - postgres
        environment:
            - LOAD_EX=n
            - EXECUTOR=Local            
            - PYTHONPATH=/usr/local/airflow/libs
            # - AIRFLOW_CONN_VERTICA_STAGING=vertica://login:pass@host:5433/dbname
        volumes:
            - /airflow/:/usr/local/airflow/
            - /airflow/dags:/usr/local/airflow/dags
            - /airflow/plugins:/usr/local/airflow/plugins
            - /airflow/requirements.txt:/requirements.txt
            - /DWH:/usr/local/airflow/libs/opersvodka/DWH
            - /etlfw:/usr/local/airflow/libs/opersvodka/etlfw
            - /META:/usr/local/airflow/libs/opersvodka/META
        ports:
            - "8080:8080"
        command: webserver
        healthcheck:
            test: ["CMD-SHELL", "[ -f /usr/local/airflow/airflow-webserver.pid ]"]
            interval: 30s
            timeout: 30s
            retries: 3
```

Запуск контейнеров:

```bash
docker-compose -f docker-compose-LocalExecutor.yml up -d
```

Airflow доступен по адресу `http://localhost:8080/admin/`