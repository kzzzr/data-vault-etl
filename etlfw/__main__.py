from vertica.worker import Worker
from generator.generate_scripts import launch as generate_scripts_launch
from source.load_source import load_excel, load_sap_mii, load_sap_mii_mock


def main():
    """
    Execute functions in sequence:
    1. First generate scripts
    2. Then load source data into staging
    3. Then execute scripts on database
    """

    with Worker() as worker:
        # generate_scripts(worker)

        # recreate_schema(worker)
        # execute_ddl(worker)

        # load_excel(worker, 'calendar')
        # load_excel(worker, 'reference')
        # load_excel(worker, 'comments_mock')

        # load_sap_mii(worker, 'STG_HQ_REPORT', '-1')
        # load_sap_mii_mock(worker)

        # load_dds(worker)
        # load_marts(worker)
        load_dq(worker)


def generate_scripts(worker):
    generate_scripts_launch(worker)


def recreate_schema(worker):
    worker.recreate_schema()


def execute_ddl(worker):
    worker.execute_scripts('DDL')


def truncate_stage(worker):
    worker.execute_scripts('TRUNCATE')


def load_dds(worker):
    worker.execute_scripts('DML')


def load_marts(worker):
    worker.execute_scripts('MART')


def load_dq(worker):
    worker.execute_scripts('DQ')


def load_excel_file(file):
    with Worker() as worker:
        load_excel(worker, file)


def load_sap_mii_file(worker, report, shift):
    load_sap_mii(worker, report, shift)


def load_sap_mii_file_mock(worker):
    load_sap_mii_mock(worker)


if __name__ == "__main__":
    main()
