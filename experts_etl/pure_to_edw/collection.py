from collections import Counter
from datetime import datetime
import json

from experts_dw import db, pure_json
from experts_dw.pure_json_collection_meta import \
    get_collection_meta_by_api_name
from experts_etl import loggers
from pureapi import client

# defaults:

transaction_record_limit = 100
iso_8601_format = '%Y-%m-%dT%H:%M:%S.%f%z'

# entry point/public api:

def run(
    collection_api_name,
    startdate=None,
    startdate_str=None, # yyyy-MM-dd or yyyy-MM-dd_HH-mm-ss format
    transaction_record_limit=transaction_record_limit,
    experts_etl_logger=None,
    api_version=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: extracting/loading raw json', extra={'pure_api_record_type': collection_api_name})

    client_config = client.Config() if api_version is None else client.Config(version=api_version)
    api_version = client_config.version

    with db.cx_oracle_connection() as connection:
        try:
            cursor = connection.cursor()
            meta = get_collection_meta_by_api_name(
                cursor,
                api_version=api_version,
                api_name=collection_api_name,
            )

            pure_json.process_changes_matching_previous_uuids(
                cursor,
                meta=meta,
            )

            uuids = pure_json.distinct_change_uuids_for_collection(
                cursor,
                meta=meta,
            )
            uuids_in_pure = []
            documents_to_insert = {}
            for api_document in client.filter_all_by_uuid_transformed(collection_api_name, uuids=uuids, config=client_config):
                uuids_in_pure.append(api_document.uuid)

                documents_to_insert[f'{api_document.uuid}:{api_document.version}'] = {
                    'uuid': api_document.uuid,
                    'pure_created': datetime.strptime(api_document.info.createdDate, iso_8601_format),
                    'pure_modified': datetime.strptime(api_document.info.modifiedDate, iso_8601_format),
                    'inserted': datetime.now(),
                    'updated': datetime.now(),
                    'json_document': json.dumps(api_document),
                }

                if len(documents_to_insert) % transaction_record_limit == 0:
                    pure_json.insert_documents(
                        cursor,
                        documents=list(documents_to_insert.values()),
                        meta=meta,
                        staging=True,
                    )
                    connection.commit()
                    documents_to_insert = {}

            if documents_to_insert:
                pure_json.insert_documents(
                    cursor,
                    documents=list(documents_to_insert.values()),
                    meta=meta,
                    staging=True,
                )
            connection.commit()

            pure_json.process_changes_matching_staging(
                cursor,
                meta=meta,
            )

            missing_uuids = list((Counter(uuids) - Counter(uuids_in_pure)).elements())
            for missing_uuids_sublist in [missing_uuids[i:i+100] for i in range(0, len(missing_uuids), 100)]:
                pure_json.delete_documents_and_changes_matching_uuids(
                    cursor,
                    uuids=missing_uuids_sublist,
                    meta=meta,
                )

            pure_json.load_documents_from_staging(
                cursor,
                meta=meta,
            )

            pure_json.process_changes_matching_previous_uuids(
                cursor,
                meta=meta,
            )

        except Exception as e:
            connection.rollback()
            formatted_exception = loggers.format_exception(e)
            experts_etl_logger.error(
                f'exception encountered during extractin/loading raw json: {formatted_exception}',
                extra={'pure_api_record_type': collection_api_name}
            )

    experts_etl_logger.info('ending: extracting/loading raw json', extra={'pure_api_record_type': collection_api_name})
