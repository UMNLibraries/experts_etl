import dotenv_switch.auto

import importlib
import multiprocessing as mp
import os
import time

from experts_dw import db, pure_json
from experts_etl import loggers, sync_file_rotator
from experts_etl.pure_to_edw import changes, collection

experts_etl_logger = loggers.experts_etl_logger()
default_interval = 14400 # 4 hours, in seconds

extractor_loaders = list(map(
    lambda x: 'experts_etl.extractor_loaders.' + x,
    [
        'pure_api_changes',
        'pure_api_external_organisations',
        'pure_api_organisational_units',
        'pure_api_external_persons',
        'pure_api_persons',
        'pure_api_research_outputs',
    ]
))

transformer_loaders = list(map(
    lambda x: 'experts_etl.transformer_loaders.' + x,
    [
        'pure_api_external_org',
        'pure_api_internal_org',
        'pure_api_external_person',
        'pure_api_internal_person',
        'pure_api_pub',
    ]
))

syncers = list(map(
    lambda x: 'experts_etl.' + x,
    [
        'oit_to_edw.person',
        'edw_to_pure.person',
        'edw_to_pure.user',
        'umn_data_error',
        'sync_file_rotator',
    ]
))

def subprocess(module_name):
    module = importlib.import_module(module_name)

    experts_etl_logger.info(
        'starting: ' + module_name,
        extra={
            'pid': str(os.getpid()),
            'ppid': str(os.getppid()),
        }
    )

    try:
        module.run(
            experts_etl_logger=experts_etl_logger
        )
    except Exception as e:
        formatted_exception = loggers.format_exception(e)
        experts_etl_logger.error(
            f'attempt to execute {module_name}.run() failed: {formatted_exception}',
            extra={
                'pid': str(os.getpid()),
                'ppid': str(os.getppid()),
            }
        )

    experts_etl_logger.info(
        'ending: ' + module_name,
        extra={
            'pid': str(os.getpid()),
            'ppid': str(os.getppid()),
        }
    )

def extract_load_changes(params):
    changes.run(**params)

def extract_load_collection(collection_api_name, params):
    collection.run(collection_api_name, **params)

def run():
    experts_etl_logger.info(
        'starting: experts etl',
        extra={
            'pid': str(os.getpid()),
            'ppid': str(os.getppid()),
        }
    )

    for module_name in extractor_loaders + transformer_loaders + syncers:
        try:
            # Must include a comma after a single arg, or multiprocessing seems to
            # get confused and think we're passing multiple arguments:
            p = mp.Process(target=subprocess, args=(module_name,))
            p.start()
            p.join()
        except Exception as e:
            formatted_exception = loggers.format_exception(e)
            experts_etl_logger.error(
                f'attempt to execute {module_name} in a new process failed: {formatted_exception}',
                extra={
                    'pid': str(os.getpid()),
                    'ppid': str(os.getppid()),
                }
            )

    api_version_collections_map = {}
    with db.cx_oracle_connection() as connection:
        cursor = connection.cursor()
        #for api_version in pure_json.api_versions(cursor): # Later!
        for api_version in ['516']: # For now, until we create tables for other versions.
            collections = pure_json.collection_api_names_for_api_version(cursor, api_version=api_version)
            api_version_collections_map[api_version] = collections

    # Just 1 process for now, until we add other api_versions. But do we even need param? Defaults to os.cpu_count.
    with mp.Pool(processes=1) as pool:
        results = []
        for api_version in api_version_collections_map:
            try:
                results.append(
                    pool.apply_async(extract_load_changes, ({'api_version': api_version},))
                )
            except Exception as e:
                formatted_exception = loggers.format_exception(e)
                experts_etl_logger.error(
                    f'attempt to extract/load changes raw json in a new process failed: {formatted_exception}',
                    extra={
                        'pid': str(os.getpid()),
                        'ppid': str(os.getppid()),
                    }
                )
        for result in results:
            result.get()

    with mp.Pool() as pool:
        results = []
        for api_version, collection_api_names in api_version_collections_map.items():
            for collection_api_name in collection_api_names:
                try:
                    results.append(
                        pool.apply_async(extract_load_collection, (collection_api_name, {'api_version': api_version},))
                    )
                except Exception as e:
                    formatted_exception = loggers.format_exception(e)
                    experts_etl_logger.error(
                        f'attempt to extract/load {collection_api_name} raw json in a new process failed: {formatted_exception}',
                        extra={
                            'pid': str(os.getpid()),
                            'ppid': str(os.getppid()),
                        }
                    )
        for result in results:
            result.get()

    experts_etl_logger.info(
        'ending: experts etl',
        extra={
            'pid': str(os.getpid()),
            'ppid': str(os.getppid()),
        }
    )
    loggers.rollover(experts_etl_logger)

def daemon(interval=default_interval):
    while True:
        run()
        time.sleep(interval)

if __name__ == '__main__':
    daemon()
