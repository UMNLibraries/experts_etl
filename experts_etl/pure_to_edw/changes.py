import json
from datetime import datetime, timedelta
from sys import getsizeof

from experts_dw import db, pure_json
from experts_etl import loggers
from pureapi import client

# defaults:

insert_record_limit = 1000
cx_oracle_parameter_size_limit_gb = 1 # It's actually 2, but...
commit_size_threshold_gb = 20 # Actually may be as high as 30 for the OIT Oracle Hotel...
one_gb = 1024**3
# Named for the Pure API endpoint:
pure_api_record_type = 'changes'

# functions:

def ensure_valid_startdate(cursor, api_version, startdate=None):
    if startdate is None:
        change_history_date = pure_json.max_change_history_inserted_date(cursor, api_version=api_version)
        change_date = pure_json.max_change_inserted_date(cursor, api_version=api_version)
        if change_history_date is None and change_date is not None:
            startdate = change_date
        if change_date is None and change_history_date is not None:
            startdate = change_history_date
        if change_date is not None and change_history_date is not None:
            startdate = max(change_date, change_history_date)
    if startdate is None:
        # Default to 1 day ago (yesterday):
        startdate = datetime.now() - timedelta(days=1)
    return startdate

def required_fields_exist(api_document):
    for field in ['uuid','familySystemName','version','changeType']:
        if field not in api_document:
            return False
    return True

# entry point/public api:

def run(
    startdate=None,
    startdate_str=None, # yyyy-MM-dd or yyyy-MM-dd_HH-mm-ss format
    insert_record_limit=insert_record_limit,
    commit_size_threshold_gb=commit_size_threshold_gb,
    experts_etl_logger=None,
    api_version=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: extracting/loading raw json', extra={'pure_api_record_type': pure_api_record_type})

    client_config = client.Config() if api_version is None else client.Config(version=api_version)
    api_version = client_config.version

    with db.cx_oracle_connection() as connection:
        try:
            cursor = connection.cursor()
            family_system_names = pure_json.collection_family_system_names_for_api_version(
                cursor,
                api_version=api_version
            )

            if startdate_str is None:
                startdate_str = ensure_valid_startdate(cursor, api_version, startdate).isoformat()

            documents_to_insert = {}
            uncommitted_json_documents_size_bytes = 0
            for api_document in client.get_all_changes_transformed(startdate_str, config=client_config):
                if not required_fields_exist(api_document):
                    continue
                if api_document.familySystemName not in family_system_names:
                    continue

                json_document = json.dumps(api_document)

                # We make this a dict with unique keys to avoid attempts to insert duplicates:
                documents_to_insert[f'{api_document.uuid}:{api_document.version}'] = {
                    'uuid': api_document.uuid,
                    'pure_version': api_document.version,
                    'change_type': api_document.changeType,
                    'family_system_name': api_document.familySystemName,
                    'inserted': datetime.now(),
                    'json_document': json_document,
                }
                uncommitted_json_documents_size_bytes += getsizeof(json_document)

                if len(documents_to_insert) % insert_record_limit == 0:
                    pure_json.insert_change_documents(
                        cursor,
                        documents=list(documents_to_insert.values()),
                        api_version=api_version
                    )
                    documents_to_insert = {}

                    if (uncommitted_json_documents_size_bytes/one_gb) > commit_size_threshold_gb:
                        connection.commit()
                        uncommitted_json_documents_size_bytes = 0

            if documents_to_insert:
                pure_json.insert_change_documents(
                    cursor,
                    documents=list(documents_to_insert.values()),
                    api_version=api_version
                )
            connection.commit()

        except Exception as e:
            connection.rollback()
            formatted_exception = loggers.format_exception(e)
            experts_etl_logger.error(
                f'exception encountered during extractin/loading raw json: {formatted_exception}',
                extra={'pure_api_record_type': pure_api_record_type}
            )

    experts_etl_logger.info('ending: extracting/loading raw json', extra={'pure_api_record_type': pure_api_record_type})
