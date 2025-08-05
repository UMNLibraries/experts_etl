import json
from datetime import datetime, timedelta

from experts_dw import db, pure_json
from experts_dw.pure_json_collection_meta import \
    get_change_meta, \
    collection_family_system_names_for_api_version
from experts_etl import loggers
from pureapi import client

# defaults:

transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'changes'

# functions:

def ensure_valid_startdate(cursor, meta, startdate=None):
    if startdate is None:
        change_history_date = pure_json.max_change_history_inserted_date(cursor, meta=meta)
        change_date = pure_json.max_change_inserted_date(cursor, meta=meta)
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
    transaction_record_limit=transaction_record_limit,
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
            meta = get_change_meta(
                cursor=cursor,
                api_version=api_version,
            )
            family_system_names = collection_family_system_names_for_api_version(
                cursor,
                api_version=api_version
            )

            if startdate_str is None:
                #startdate_str = ensure_valid_startdate(cursor, api_version, startdate).isoformat()
                startdate_str = ensure_valid_startdate(cursor, meta, startdate).strftime('%Y-%m-%d')

            documents_to_insert = {}
            for api_document in client.get_all_changes_transformed(startdate_str, config=client_config):
                if not required_fields_exist(api_document):
                    continue
                if api_document.familySystemName not in family_system_names:
                    continue

                # We make this a dict with unique keys to avoid attempts to insert duplicates:
                documents_to_insert[f'{api_document.uuid}:{api_document.version}'] = {
                    'uuid': api_document.uuid,
                    'pure_version': api_document.version,
                    'change_type': api_document.changeType,
                    'family_system_name': api_document.familySystemName,
                    'inserted': datetime.now(),
                    'json_document': json.dumps(api_document),
                }

                if len(documents_to_insert) % transaction_record_limit == 0:
                    pure_json.insert_change_documents(
                        cursor,
                        documents=list(documents_to_insert.values()),
                        meta=meta,
                    )
                    documents_to_insert = {}
                    connection.commit()

            if documents_to_insert:
                pure_json.insert_change_documents(
                    cursor,
                    documents=list(documents_to_insert.values()),
                    meta=meta,
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
