from sqlalchemy import and_, func
from experts_dw import db
from experts_dw.models import PureApiInternalOrg, PureApiInternalOrgHst, PureApiChange, PureApiChangeHst, PureOrg, PureInternalOrg, PersonPureOrg, PubPersonPureOrg, Pub, UmnPersonPureOrg, UmnDeptPureOrg, PureInternalOrg
from experts_etl import transformers
from pureapi import client, response
from pureapi.client import Config, PureAPIRequestException, PureAPIHTTPError
from experts_etl import loggers
from experts_etl.changes_buffer_managers import changes_for_family_ordered_by_uuid_version, record_changes_as_processed

# defaults:

db_name = 'hotel'
transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'organisational-units'

# functions:

def api_internal_org_exists_in_db(session, api_internal_org):
    api_internal_org_modified = transformers.iso_8601_string_to_datetime(api_internal_org.info.modifiedDate)

    db_api_internal_org_hst = (
        session.query(PureApiInternalOrgHst)
        .filter(and_(
            PureApiInternalOrgHst.uuid == api_internal_org.uuid,
            PureApiInternalOrgHst.modified == api_internal_org_modified,
        ))
        .one_or_none()
    )
    if db_api_internal_org_hst:
        return True

    db_api_internal_org = (
        session.query(PureApiInternalOrg)
        .filter(and_(
            PureApiInternalOrg.uuid == api_internal_org.uuid,
            PureApiInternalOrg.modified == api_internal_org_modified,
        ))
        .one_or_none()
    )
    if db_api_internal_org:
        return True

    return False

def get_db_org(session, uuid):
    return (
        session.query(PureOrg)
        .filter(PureOrg.pure_uuid == uuid)
        .one_or_none()
    )

def delete_db_org(session, db_org):
    # We may be able to do all this with less code by using
    # the sqlalchemy delete cascade somehow:

    # If this org owns research outputs, change the owner_pure_org_uuid to one
    # that we know should be stable, just as a placeholder. That field of the
    # research output will likely be automatically updated later.
    root_db_mptt_org = session.query(PureInternalOrg).filter(PureInternalOrg.left == 1).one()
    for pub in session.query(func.count(Pub.uuid)).filter(
        Pub.owner_pure_org_uuid == db_org.pure_uuid
    ).all():
        pub.owner_pure_org_uuid = root_db_mptt_org.pure_uuid
        session.add(pub)

    session.query(PubPersonPureOrg).filter(
        PubPersonPureOrg.pure_org_uuid == db_org.pure_uuid
    ).delete(synchronize_session=False)

    session.query(PersonPureOrg).filter(
        PersonPureOrg.pure_org_uuid == db_org.pure_uuid
    ).delete(synchronize_session=False)

    session.query(UmnPersonPureOrg).filter(
        UmnPersonPureOrg.pure_org_uuid == db_org.pure_uuid
    ).delete(synchronize_session=False)

    session.query(UmnDeptPureOrg).filter(
        UmnDeptPureOrg.pure_org_uuid == db_org.pure_uuid
    ).delete(synchronize_session=False)

    session.delete(db_org)

def delete_merged_records(session, api_org):
    for uuid in api_org.info.previousUuids:
        db_org = get_db_org(session, uuid)
        if db_org:
            delete_db_org(session, db_org)

def db_org_newer_than_api_org(session, api_org):
    api_org_modified = transformers.iso_8601_string_to_datetime(api_org.info.modifiedDate)
    db_org = get_db_org(session, api_org.uuid)
    # We need the replace(tzinfo=None) here, or we get errors like:
    # TypeError: can't compare offset-naive and offset-aware datetimes
    if db_org and db_org.pure_modified and db_org.pure_modified >= api_org_modified.replace(tzinfo=None):
        return True
    return False

def load_api_internal_org(session, api_internal_org, raw_json):
    db_api_internal_org = PureApiInternalOrg(
        uuid=api_internal_org.uuid,
        json=raw_json,
        modified=transformers.iso_8601_string_to_datetime(api_internal_org.info.modifiedDate)
    )
    session.add(db_api_internal_org)

# entry point/public api:

def run(
    # Do we need other default functions here?
    #extract_api_changes=extract_api_changes,
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

    # Capture the current record for each iteration, so we can log it in case of an exception:
    latest_change = None

    try:
        with db.session(db_name) as session:
            processed_changes = []
            for changes in changes_for_family_ordered_by_uuid_version(session, 'Organisation'):
                latest_change = changes[0]
                db_org = get_db_org(session, latest_change.uuid)

                # We delete here and continue, because there will be no record
                # to download from the Pure API when it has been deleted.
                if latest_change.change_type == 'DELETE':
                    if db_org:
                        delete_db_org(session, db_org)
                    processed_changes.extend(changes)
                    if len(processed_changes) >= transaction_record_limit:
                        record_changes_as_processed(session, processed_changes)
                        processed_changes = []
                        session.commit()
                    continue

                r = None
                try:
                    r = client.get(pure_api_record_type + '/' + latest_change.uuid, config=pure_api_config)
                except PureAPIHTTPError as e:
                    if e.response.status_code == 404:
                        if db_org:
                            # This record has been deleted from Pure but still exists in our local db:
                            delete_db_org(session, db_org)
                        processed_changes.extend(changes)
                        if len(processed_changes) >= transaction_record_limit:
                            record_changes_as_processed(session, processed_changes)
                            processed_changes = []
                            session.commit()
                    else:
                        experts_etl_logger.error(
                            f'HTTP error {e.response.status_code} returned during record extraction',
                            extra={'pure_uuid': latest_change.uuid, 'pure_api_record_type': pure_api_record_type}
                        )
                    continue
                except PureAPIRequestException as e:
                    formatted_exception = loggers.format_exception(e)
                    experts_etl_logger.error(
                        f'mysterious client request exception encountered during record extraction: {formatted_exception}',
                        extra={'pure_uuid': latest_change.uuid, 'pure_api_record_type': pure_api_record_type}
                    )
                    continue
                except Exception:
                    raise


                api_internal_org = response.transform(
                    pure_api_record_type,
                    r.json(),
                    version=pure_api_config.version
                )

                delete_merged_records(session, api_internal_org)

                load = True
                if db_org_newer_than_api_org(session, api_internal_org):
                    load = False
                if api_internal_org_exists_in_db(session, api_internal_org):
                    load = False
                if load:
                    load_api_internal_org(session, api_internal_org, r.text)

                processed_changes.extend(changes)
                if len(processed_changes) >= transaction_record_limit:
                    record_changes_as_processed(session, processed_changes)
                    processed_changes = []
                    session.commit()

            record_changes_as_processed(session, processed_changes)
            session.commit()

    except Exception as e:
        formatted_exception = loggers.format_exception(e)
        experts_etl_logger.error(
            f'exception encountered during record extraction: {formatted_exception}',
            extra={'pure_uuid': latest_change.uuid, 'pure_api_record_type': pure_api_record_type}
        )

    experts_etl_logger.info('ending: extracting/loading', extra={'pure_api_record_type': pure_api_record_type})
