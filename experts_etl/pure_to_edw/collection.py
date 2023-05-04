from collections import Counter
from datetime import datetime
import json
from sys import getsizeof

from experts_dw import db, pure_json
from experts_etl import loggers
from pureapi import client

# defaults:

insert_record_limit = 1000
# Based on: ORA-01795: maximum number of expressions in a list is 1000
oracle_expression_list_limit = 1000
cx_oracle_parameter_size_limit_gb = 1 # It's actually 2, but...
commit_size_threshold_gb = 20 # Actually may be as high as 30 for the OIT Oracle Hotel...
one_gb = 1024**3
iso_8601_format = '%Y-%m-%dT%H:%M:%S.%f%z'

# entry point/public api:

def run(
    collection_api_name,
    startdate=None,
    startdate_str=None, # yyyy-MM-dd or yyyy-MM-dd_HH-mm-ss format
    insert_record_limit=insert_record_limit,
    commit_size_threshold_gb=commit_size_threshold_gb,
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

            pure_json.process_changes_matching_previous_uuids(
                cursor,
                collection_api_name=collection_api_name,
                api_version=api_version
            )

            uuids = pure_json.distinct_change_uuids_for_collection(
                cursor,
                collection_api_name=collection_api_name,
                api_version=api_version
            )
            uuids_in_pure = []
            documents_to_insert = {}
            uncommitted_json_documents_size_bytes = 0
            for api_document in client.filter_all_by_uuid_transformed(collection_api_name, uuids=uuids, config=client_config):
                uuids_in_pure.append(api_document.uuid)
                json_document = json.dumps(api_document)

                documents_to_insert[f'{api_document.uuid}:{api_document.version}'] = {
                    'uuid': api_document.uuid,
                    'pure_created': datetime.strptime(api_document.info.createdDate, iso_8601_format),
                    'pure_modified': datetime.strptime(api_document.info.modifiedDate, iso_8601_format),
                    'inserted': datetime.now(),
                    'updated': datetime.now(),
                    'json_document': json_document,
                }
                uncommitted_json_documents_size_bytes += getsizeof(json_document)

                if len(documents_to_insert) % insert_record_limit == 0:
                    pure_json.insert_documents(
                        cursor,
                        documents=list(documents_to_insert.values()),
                        collection_api_name=collection_api_name,
                        staging=True,
                        api_version=api_version
                    )
                    documents_to_insert = {}

                    if (uncommitted_json_documents_size_bytes/one_gb) > commit_size_threshold_gb:
                        connection.commit()
                        uncommitted_json_documents_size_bytes = 0

            if documents_to_insert:
                pure_json.insert_documents(
                    cursor,
                    documents=list(documents_to_insert.values()),
                    collection_api_name=collection_api_name,
                    staging=True,
                    api_version=api_version
                )
            connection.commit()

            pure_json.process_changes_matching_staging(
                cursor,
                collection_api_name=collection_api_name,
                api_version=api_version
            )

            missing_uuids = list((Counter(uuids) - Counter(uuids_in_pure)).elements())
            for missing_uuids_sublist in [missing_uuids[i:i+oracle_expression_list_limit] for i in range(0, len(missing_uuids), oracle_expression_list_limit)]:
                pure_json.delete_documents_and_changes_matching_uuids(
                    cursor,
                    uuids=missing_uuids_sublist,
                    collection_api_name=collection_api_name,
                    api_version=api_version
                )
            # UPDATE: The below was not true! Got this error:
            # ORA-01795: maximum number of expressions in a list is 1000
            # Should be a small list, so we'll just delete all of them at once:
#            if missing_uuids:
#                pure_json.delete_documents_and_changes_matching_uuids(
#                    cursor,
#                    uuids=missing_uuids,
#                    collection_api_name=collection_api_name,
#                    api_version=api_version
#                )

            pure_json.load_documents_from_staging(
                cursor,
                collection_api_name=collection_api_name,
                api_version=api_version
            )

            pure_json.process_changes_matching_previous_uuids(
                cursor,
                collection_api_name=collection_api_name,
                api_version=api_version
            )

        except Exception as e:
            connection.rollback()
            formatted_exception = loggers.format_exception(e)
            experts_etl_logger.error(
                f'exception encountered during extractin/loading raw json: {formatted_exception}',
                extra={'pure_api_record_type': collection_api_name}
            )

    experts_etl_logger.info('ending: extracting/loading raw json', extra={'pure_api_record_type': collection_api_name})
