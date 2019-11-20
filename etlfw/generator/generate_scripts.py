import numpy as np
import pandas as pd
import os
import shutil
import toml
import re

# GLOBAL CONFIG TO REMEMBER ALL DEFINITIONS
_CONF = {}
_CONF_FILE_LOCATION = os.path.join(os.path.dirname(__file__), 'config.toml')


def launch(worker):
    # READ CONFIG
    worker.logger.info(f'{__file__} : START EXECUTION : READ CONFIG')
    global _CONF
    _CONF = toml.load(_CONF_FILE_LOCATION)
    _CONF['metadir'] = worker.config['metadir']

    # READ FMD FILE
    worker.logger.info(f'{__file__} : START EXECUTION : READ FMD FILE')
    fmd_stage, fmd_dds, project_directory = _read_fmd()
    worker.config['project_directory'] = project_directory

    # CLEAN UP SCRIPTS DIRECTORY
    worker.logger.info(f'{__file__} : START EXECUTION : CLEAN UP SCRIPTS DIRECTORY')
    _clean_up()

    # Generate STAGE, ODS, DDS scripts (DDL + DML)
    worker.logger.info(f'{__file__} : START EXECUTION : Generate STAGE, ODS, DDS scripts (DDL + DML)')
    _gen_scripts(fmd_stage, fmd_dds)


def _read_fmd():
    _CONF['fmd_file_path'] = os.path.join(os.path.dirname(__file__), _CONF['fmd_file_path'])
    _CONF['project_directory'] = os.path.join(os.path.dirname(__file__), _CONF['scripts_path'],
                                              _CONF['project'])
    _CONF['sql_templates'] = toml.load(
        os.path.join(os.path.dirname(__file__), _CONF['sql_templates_file']))
    _CONF['airflow_templates'] = toml.load(
        os.path.join(os.path.dirname(__file__), _CONF['airflow_templates_file']))

    # Read fmd STAGE Sheet to Pandas and Fill Excel Merged cells
    fmd_stage = pd.read_excel(_CONF['fmd_file_path'], 'STAGE', index_col=None, skiprows=[0])
    fmd_stage[['StageSchema', 'StageTable']] = fmd_stage[['StageSchema', 'StageTable']].fillna(
        method='ffill')

    # Read fmd DDS sheet to Pandas and Fill Excel Merged cells
    fmd_dds = pd.read_excel(_CONF['fmd_file_path'], 'DDS', index_col=None, skiprows=[0])
    fmd_dds[['TargetType', 'TargetSchema', 'TargetTable', 'SourceSchema', 'SourceTable']] = fmd_dds[
        ['TargetType', 'TargetSchema', 'TargetTable', 'SourceSchema', 'SourceTable']].fillna(
        method='ffill')

    return fmd_stage, fmd_dds, _CONF['project_directory']


def _clean_up():
    directory = _CONF['project_directory']
    pattern = re.compile("[0-5]-.*")
    _ensure_dir(directory)
    for _, folder in enumerate(sorted(os.listdir(directory))):
        if os.path.exists(directory) and pattern.match(folder):
            shutil.rmtree(directory)


def _gen_scripts(fmd_stage, fmd_dds):
    # Generate META tables
    _gen_meta(fmd_stage)

    # GROUP fmd BY SOURCE FILES and gen scripts for each
    stage_tables = fmd_stage.groupby('StageTable')

    for stage_table_name, stage_table_df in stage_tables:
        _gen_stage(stage_table_name, stage_table_df)
        _dds_gen(fmd_dds, stage_table_name)
        _gen_mart()
        _gen_dq()

    _gen_airflow_dags('DDL')
    _gen_airflow_dags('TRUNCATE')
    _gen_airflow_dags('DML')


def _gen_meta(fmd_stage):
    # Get values from config
    meta_schema = fmd_stage.StageSchema.values[0]
    _CONF['META'] = locals()

    sql_templates = _CONF['sql_templates']['META']
    for template, script in sql_templates.items():
        _write_file('{project_directory}/0-META/DDL'.format(**_CONF),
                    f'{template}'.format(**_CONF['META']),
                    script.format(**_CONF['META']))


def _gen_stage(stage_table_name, stage_table_df):
    stage_table = stage_table_name
    stage_schema = stage_table_df.StageSchema.values[0]
    ods_schema = stage_table_df.ODSSchema.values[0]
    ods_table = stage_table_df.ODSTable.values[0]
    skip_ods = pd.isnull(ods_table)

    # Columns Definition List
    stage_table_df['NotNull'] = stage_table_df['ColumnProperties'].str.contains('NotNull', na=False)
    stage_table_df['NotNull'] = np.where((stage_table_df.NotNull == True), ' NOT NULL', '')
    stage_table_df['DefaultValue'] = np.where(stage_table_df.DefaultValue.isnull(), '',
                                              ' DEFAULT ' + stage_table_df.DefaultValue)
    columns_definition = stage_table_df.StageColumn + ' ' + stage_table_df.StageColumnType\
                         + stage_table_df.NotNull + stage_table_df.DefaultValue
    columns_definition = ',\n\t'.join(columns_definition.tolist())
    # Columns List
    columns_list_qualified = ',\n\t'.join('src.' + stage_table_df.StageColumn)
    columns_list = ',\n\t'.join(stage_table_df.StageColumn)
    # Columns PK
    stage_table_df['PK'] = stage_table_df['ColumnProperties'].str.contains('PrimaryKey', na=False)
    # columns_pk = ', '.join(stage_table_df['StageColumn'][stage_table_df.PK == True].sort_values())
    columns_pk = ', '.join(stage_table_df['StageColumn'][stage_table_df.PK == True])
    # Columns ORDER BY
    columns_ordered_by = ',\n\t'.join(
        stage_table_df[stage_table_df['ColumnOrderBy'].notnull()].sort_values(
            by=['ColumnOrderBy']).StageColumn)
    # Columns SEGMENTED BY
    stage_table_df['SEGM'] = stage_table_df['ColumnProperties'].str.contains('SegmentedBy',
                                                                             na=False)
    if stage_table_df['SEGM'].any():
        segmentation_clause = 'SEGMENTED BY HASH(MD5(' + ' || '\
            .join(stage_table_df['StageColumn'][stage_table_df.SEGM == True].sort_values() + '::VARCHAR') + '))'
    else:
        segmentation_clause = 'UNSEGMENTED'
    # Column PARTITION BY
    stage_table_df['PARTBY'] = stage_table_df['ColumnProperties'].str.contains('PartitionBy',
                                                                               na=False)
    column_partition_by = ', '.join(
        stage_table_df['StageColumn'][stage_table_df.PARTBY == True].sort_values())
    # Column Source System
    stage_table_df['SRC'] = stage_table_df['ColumnProperties'].str.contains('SourceSystem',
                                                                            na=False)
    column_source_system = ', '.join(
        'src.' + stage_table_df['StageColumn'][stage_table_df.SRC == True].sort_values())
    # Column FILE NAME
    stage_table_df['FN'] = stage_table_df['ColumnProperties'].str.contains('FileName', na=False)
    column_file_name = ', '.join(
        'src.' + stage_table_df['StageColumn'][stage_table_df.FN == True].sort_values())
    # Column Business Date
    stage_table_df['BDATE'] = stage_table_df['ColumnProperties'].str.contains('BusinessDate',
                                                                              na=False)
    column_business_date = ', '.join(
        'src.' + stage_table_df['StageColumn'][stage_table_df.BDATE == True].sort_values())
    # ODS

    # FILL IN A DICT WITH LOCAL VARIABLES
    _CONF[stage_table] = locals()

    directory = _CONF['project_directory']
    sql_templates = _CONF['sql_templates']

    # Gen STAGE
    for type, scripts in sql_templates['STAGE'].items():
        for template, script in scripts.items():
            _write_file(f'{directory}/1-{stage_table}/{type}',
                        f'{stage_schema}.{template}.sql'.format(**_CONF[stage_table]),
                        script.format(**_CONF[stage_table]))

    # Gen ODS
    if not skip_ods:
        for type, scripts in sql_templates['ODS'].items():
            for template, script in scripts.items():
                _write_file(f'{directory}/2-{ods_table}/{type}',
                            f'{ods_schema}.{template}.sql'.format(**_CONF[stage_table]),
                            script.format(**_CONF[stage_table]))


def _dds_gen(fmd_dds, stage_table_name):
    df = fmd_dds[fmd_dds.SourceTable == stage_table_name]
    if not df.empty:
        groups = df.groupby('TargetType')
        if 'HUB' in groups.indices:
            hubs = groups.get_group('HUB')
            _gen_hubs(hubs)
        if 'LINK' in groups.indices:
            links = groups.get_group('LINK')
            _gen_links(links)
        if 'SATELLITE' in groups.indices:
            satellites = groups.get_group('SATELLITE')
            _gen_satellites(satellites)


def _gen_hubs(hubs):
    hub_list = hubs.TargetTable.unique().tolist()
    for hub_name in hub_list:
        hub_df = hubs[hubs.TargetTable == hub_name]

        target_schema = hub_df.TargetSchema.values[0]
        source_schema = hub_df.SourceSchema.values[0]
        target_table = hub_df.TargetTable.values[0]
        source_table = hub_df.SourceTable.values[0]
        column_source_system = _CONF[source_table]['column_source_system']
        column_file_name = _CONF[source_table]['column_file_name']
        column_business_date = _CONF[source_table]['column_business_date']

        # BUSINESS KEY PROPERTIES
        business_key = hub_df.SourceColumn
        business_key = ',\n\t'.join(business_key.tolist())
        business_key_qualified = 'src.' + hub_df.SourceColumn
        business_key_qualified = ',\n\t'.join(business_key_qualified.tolist())
        hash_key = 'TRIM(src.' + hub_df.SourceColumn + '::VARCHAR)'
        hash_key = 'MD5(' + ' || '.join(hash_key.tolist()) + ')'

        if target_table in _CONF.keys():
            if source_table in _CONF[target_table].keys():
                master_key_def = _CONF[target_table]['master_key_def']
                master_key = _CONF[target_table]['master_key']
                _CONF[target_table]['skip_ddl'] = True
                _CONF[target_table][source_table].update(locals())
            else:
                _CONF[target_table][source_table] = {}
                master_key_def = _CONF[target_table]['master_key_def']
                master_key = _CONF[target_table]['master_key']
                _CONF[target_table]['skip_ddl'] = True
                _CONF[target_table][source_table].update(locals())
        else:
            # BUSINESS KEY PROPERTIES
            business_key = hub_df.SourceColumn
            business_key = ',\n\t'.join(business_key.tolist())
            master_key_def = hub_df.SourceColumn + ' ' + hub_df.SourceColumnType + ' NOT NULL'
            master_key_def = ',\n\t'.join(master_key_def.tolist())
            master_key = business_key
            business_key_qualified = 'src.' + hub_df.SourceColumn
            business_key_qualified = ',\n\t'.join(business_key_qualified.tolist())
            hash_key = 'TRIM(src.' + hub_df.SourceColumn + '::VARCHAR)'
            hash_key = 'MD5(' + ' || '.join(hash_key.tolist()) + ')'

            # ADD HK definition to global dictionary
            _CONF[target_table] = {}
            _CONF[target_table].update(locals())
            _CONF[target_table][source_table] = {}
            _CONF[target_table][source_table].update(locals())
            _CONF[target_table]['skip_ddl'] = False

        # Get values from config
        directory = _CONF['project_directory']
        sql_templates = _CONF['sql_templates']['DDS']['HUB']

        # Gen HUBS
        for type, scripts in sql_templates.items():
            for template, script in scripts.items():
                if template == '{target_table}' and _CONF[target_table]['skip_ddl'] == True:
                    continue
                _write_file(f'{directory}/3-{target_table}/{type}',
                            f'{target_schema}.{template}.sql'.format(
                                **_CONF[target_table][source_table]),
                            script.format(**_CONF[target_table][source_table]))


def _gen_links(links):
    link_list = links.TargetTable.unique().tolist()
    for link_name in link_list:
        link_df = links[links.TargetTable == link_name]

        target_schema = link_df.TargetSchema.values[0]
        target_table = link_df.TargetTable.values[0]
        source_schema = link_df.SourceSchema.values[0]
        source_table = link_df.SourceTable.values[0]
        reference_schema = link_df.ReferenceSchema.values[0]
        column_source_system = _CONF[source_table]['column_source_system']
        column_file_name = _CONF[source_table]['column_file_name']
        column_business_date = _CONF[source_table]['column_business_date']

        reference_key_def = [
            f'HK_{ref_table} VARCHAR(32) NOT NULL CONSTRAINT FK_{ref_table} REFERENCES {reference_schema}.{ref_table}' 
            f'(HK_{ref_table})' for ref_table in link_df.ReferenceTable.values]
        reference_key_def = ',\n\t'.join(reference_key_def)
        reference_key = 'HK_' + link_df.ReferenceTable
        reference_key = ',\n\t'.join(reference_key.tolist())
        reference_key_alias = [_CONF[ref_table][source_table]['hash_key'] + f' AS HK_{ref_table}'
                               for ref_table in
                               link_df.ReferenceTable.values]
        reference_key_alias = ',\n\t'.join(reference_key_alias)
        hash_key = [_CONF[ref_table][source_table]['hash_key'] for ref_table in
                    link_df.ReferenceTable.values]
        hash_key = 'MD5(' + ' || '.join(hash_key) + ')'

        # ADD HK definition to global dictionary
        _CONF[target_table] = {}
        _CONF[target_table].update(locals())

        # Get values from config
        directory = _CONF['project_directory']
        sql_templates = _CONF['sql_templates']['DDS']['LINK']

        # Gen STAGE
        for type, scripts in sql_templates.items():
            for tmpl, script in scripts.items():
                _write_file(f'{directory}/4-{target_table}/{type}',
                            f'{target_schema}.{tmpl}.sql'.format(**_CONF[target_table]),
                            script.format(**_CONF[target_table]))


def _gen_satellites(satellites):
    sat_list = satellites.TargetTable.unique().tolist()
    for sat_name in sat_list:
        sat_df = satellites[satellites.TargetTable == sat_name]

        target_schema = sat_df.TargetSchema.values[0]
        source_schema = sat_df.SourceSchema.values[0]
        reference_schema = sat_df.ReferenceSchema.values[0]
        target_table = sat_df.TargetTable.values[0]
        source_table = sat_df.SourceTable.values[0]
        reference_table = sat_df.ReferenceTable.unique().tolist()[0]
        column_source_system = _CONF[source_table]['column_source_system']
        column_file_name = _CONF[source_table]['column_file_name']
        column_business_date = _CONF[source_table]['column_business_date']

        attribute_columns_def = sat_df[sat_df.TargetColumnType == 'ATTRIBUTE'].SourceColumn + ' ' + \
                                sat_df[
                                    sat_df.TargetColumnType == 'ATTRIBUTE'].SourceColumnType
        attribute_columns_def = ',\n\t'.join(attribute_columns_def.tolist())
        attribute_columns = sat_df[sat_df.TargetColumnType == 'ATTRIBUTE'].SourceColumn
        attribute_columns = ',\n\t'.join(attribute_columns.tolist())
        attribute_columns_qualified = 'src.' + sat_df[
            sat_df.TargetColumnType == 'ATTRIBUTE'].SourceColumn
        attribute_columns_qualified = ',\n\t'.join(attribute_columns_qualified.tolist())
        # Satellite HASH KEY
        hash_key = [_CONF[ref_table]['hash_key'] for ref_table in
                    sat_df.ReferenceTable[~pd.isnull(sat_df.ReferenceTable)].unique()][0]
        # Satellite Hash Diff
        hash_diff = 'isnull(src.' + sat_df[
            sat_df.TargetColumnType == 'ATTRIBUTE'].SourceColumn + '::VARCHAR, \'NULL\')'
        hash_diff = ' || '.join(hash_diff.tolist())

        # ADD HK definition to global dictionary
        _CONF[target_table] = {}
        _CONF[target_table].update(locals())

        # Get values from config
        directory = _CONF['project_directory']
        sql_templates = _CONF['sql_templates']['DDS']['SATELLITE']

        # Gen STAGE
        for type, scripts in sql_templates.items():
            for tmpl, script in scripts.items():
                _write_file(f'{directory}/5-{target_table}/{type}',
                            f'{target_schema}.{tmpl}.sql'.format(**_CONF[target_table]),
                            script.format(**_CONF[target_table]))


def _gen_mart():
    from distutils.dir_util import copy_tree
    source = _CONF['folder_marts']
    destination = _CONF['project_directory']
    copy_tree(source, destination)


def _gen_dq():
    from distutils.dir_util import copy_tree
    source = _CONF['folder_dq']
    destination = _CONF['project_directory']
    copy_tree(source, destination)


def _gen_airflow_dags(mode):
    metadir = _CONF['metadir']
    rootdir = _CONF['project_directory']
    airflow_templates = _CONF['airflow_templates']['OPERATOR']

    for _, folder in enumerate(sorted(os.listdir(rootdir))):
        subdir = os.path.join(rootdir, folder, mode)
        folder_pattern = re.compile(metadir[mode]['RE'])
        if folder_pattern.match(folder):
            sf = os.path.join(subdir, 'sequence')
            with open(sf, 'r') as seq:
                for filecounter, file in enumerate(seq):
                    file = file.strip()
                    filepath = os.path.join(subdir, file)
                    stage = metadir[mode]['MODE']
                    obj = metadir[mode]['OBJECT'].format(**locals())[2:]
                    step = metadir[mode]['STEP'].format(**locals())[:-4]
                    descr = metadir[mode]['DESCRIPTION'].format(**locals())
                    task_name = re.sub('\.', '_', step)

                    # Generate Airflow scripts
                    for tmpl, script in airflow_templates.items():
                        _write_airflow_scripts(f'{rootdir}/8-airflow/{stage}',
                                               f'{stage}.py'.format(**locals()),
                                               script.format(**locals()),
                                               task_name)
        else:
            continue


def _write_airflow_scripts(dir, filename, contents, task_name):
    fullpath = os.path.join(dir, filename)
    seqfile = os.path.join(dir, 'sequence')
    _ensure_dir(fullpath)

    with open(fullpath, 'a+') as f:
        f.write(contents)

    with open(seqfile, 'a+') as f:
        f.write(f"{task_name}\n")


def _write_file(dir, filename, contents):
    fullpath = os.path.join(dir, filename)
    seqfile = os.path.join(dir, 'sequence')
    _ensure_dir(fullpath)

    with open(fullpath, 'w+') as f:
        f.write(contents)

    with open(seqfile, 'a+') as f:
        f.write(f"{filename}\n")


def _ensure_dir(directory):
    directory = os.path.dirname(directory)
    if not os.path.exists(directory):
        os.makedirs(directory)
