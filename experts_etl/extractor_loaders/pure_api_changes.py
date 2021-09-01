import json
import datetime
import re
from sqlalchemy import and_, func
from experts_dw import db
from experts_dw.models import PureApiChange, PureApiChangeHst, Pub, Person, PureOrg
from pureapi import client
from pureapi.client import Config
from experts_etl import loggers

# defaults:

db_name = 'hotel'
transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'changes'

family_system_name_db_class_map = {
    'Person': Person,
    'ExternalPerson': Person,
    'Organisation': PureOrg,
    'ExternalOrganisation': PureOrg,
    'ResearchOutput': Pub,
}

# functions:

def ensure_valid_startdate(session, startdate=None):
    if startdate is None:
        history_startdate = session.query(func.max(PureApiChangeHst.downloaded)).scalar()
        buffer_startdate = session.query(func.max(PureApiChange.downloaded)).scalar()
        if history_startdate is None and buffer_startdate is not None:
            startdate = buffer_startdate
        if buffer_startdate is None and history_startdate is not None:
            startdate = history_startdate
        if buffer_startdate is not None and history_startdate is not None:
            startdate = max(buffer_startdate, history_startdate)
    if startdate is None:
        # Default to 1 day ago (yesterday):
        startdate = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    return startdate

def required_fields_exist(api_change):
    for field in ['uuid','familySystemName','version','changeType']:
        if field not in api_change:
            return False
    return True

def matching_db_record_exists(session, api_change):
    db_class = family_system_name_db_class_map[api_change.familySystemName]
    db_record = (
        session.query(db_class)
        .filter(db_class.pure_uuid == api_change.uuid)
        .one_or_none()
    )
    if db_record:
        return True
    else:
        return False

def previously_processed_same_or_newer_change(session, api_change):
    db_change_hst_version = (session
        .query(func.max(PureApiChangeHst.version))
        .filter(PureApiChangeHst.uuid==api_change.uuid)
        .scalar()
    )
    if db_change_hst_version is not None and db_change_hst_version >= api_change.version:
        return True
    return False

def already_loaded_same_or_newer_change(session, api_change):
    db_change_version = (session
        .query(func.max(PureApiChange.version))
        .filter(PureApiChange.uuid==api_change.uuid)
        .scalar()
    )
    if db_change_version is not None and db_change_version >= api_change.version:
        return True
    return False

def load_api_change(session, api_change):
    db_change = PureApiChange(
        uuid=api_change.uuid,
        family_system_name=api_change.familySystemName,
        change_type=api_change.changeType,
        json=json.dumps(api_change),
        version=api_change.version
    )
    session.add(db_change)

# entry point/public api:

def run(
    startdate=None,
    startdate_str=None, # yyyy-MM-dd or yyyy-MM-dd_HH-mm-ss format
    db_name=db_name,
    transaction_record_limit=transaction_record_limit,
    experts_etl_logger=None,
    pure_api_config=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: extracting/loading', extra={'pure_api_record_type': pure_api_record_type})

    if pure_api_config is None:
        pure_api_config = Config()

    with db.session(db_name) as session:
        record_count = 0
        if startdate_str is None:
            startdate_str = ensure_valid_startdate(session, startdate).isoformat()
        for api_change in client.get_all_changes_transformed(startdate_str, config=pure_api_config):
            if not required_fields_exist(api_change):
                continue
            if api_change.familySystemName not in family_system_name_db_class_map:
                continue
            if matching_db_record_exists(session, api_change) \
                and previously_processed_same_or_newer_change(session, api_change):
                continue
            if already_loaded_same_or_newer_change(session, api_change):
                continue

            load_api_change(session, api_change)

            record_count += 1
            if (record_count % transaction_record_limit) == 0:
                session.commit()

    experts_etl_logger.info('ending: extracting/loading', extra={'pure_api_record_type': pure_api_record_type})
