from dateutil.parser import isoparse

from sqlalchemy import and_, func

from experts_dw import db
from experts_dw.models import PureApiInternalPerson, PureApiInternalPersonHst, PureApiChange, PureApiChangeHst, Person, PubPerson, PubPersonPureOrg, PersonPureOrg, PersonScopusId, UmnPersonPureOrg
from pureapi import client, response
from pureapi.client import Config, PureAPIRequestException, PureAPIHTTPError
from experts_etl import loggers
from experts_etl.changes_buffer_managers import changes_for_family_ordered_by_uuid_version, record_changes_as_processed

# defaults:

db_name = 'hotel'
transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'persons'

# functions:

def already_loaded_same_api_internal_person(session, api_internal_person):
    api_internal_person_modified = isoparse(
        api_internal_person.info.modifiedDate
    )
    db_api_internal_person = (
        session.query(PureApiInternalPerson)
        .filter(and_(
            PureApiInternalPerson.uuid == api_internal_person.uuid,
            PureApiInternalPerson.modified == api_internal_person_modified,
        ))
        .one_or_none()
    )
    if db_api_internal_person:
        return True
    return False

def get_db_person(session, uuid):
    return (
        session.query(Person)
        .filter(Person.pure_uuid == uuid)
        .one_or_none()
    )

def delete_db_person(session, db_person):
    # We may be able to do this with less code by using
    # the sqlalchemy delete cascade somehow:
    session.query(PubPerson).filter(
        PubPerson.person_uuid == db_person.uuid
    ).delete(synchronize_session=False)

    session.query(PubPersonPureOrg).filter(
        PubPersonPureOrg.person_uuid == db_person.uuid
    ).delete(synchronize_session=False)

    session.query(PersonPureOrg).filter(
        PersonPureOrg.person_uuid == db_person.uuid
    ).delete(synchronize_session=False)

    session.query(UmnPersonPureOrg).filter(
        UmnPersonPureOrg.person_uuid == db_person.uuid
    ).delete(synchronize_session=False)

    session.query(PersonScopusId).filter(
        PersonScopusId.person_uuid == db_person.uuid
    ).delete(synchronize_session=False)

    session.delete(db_person)

def delete_merged_records(session, api_person):
    for uuid in api_person.info.previousUuids:
        db_person = get_db_person(session, uuid)
        if db_person:
            delete_db_person(session, db_person)

def db_person_same_or_newer_than_api_internal_person(session, db_person, api_internal_person):
    # We need the replace(tzinfo=None) here, or we get errors like:
    # TypeError: can't compare offset-naive and offset-aware datetimes
    api_internal_person_modified = isoparse(
        api_internal_person.info.modifiedDate
    ).replace(tzinfo=None)
    if db_person.pure_modified and db_person.pure_modified >= api_internal_person_modified:
        return True
    return False

def load_api_internal_person(session, api_internal_person, raw_json):
    db_api_internal_person = PureApiInternalPerson(
        uuid=api_internal_person.uuid,
        json=raw_json,
        modified=isoparse(api_internal_person.info.modifiedDate)
    )
    session.add(db_api_internal_person)

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
            for changes in changes_for_family_ordered_by_uuid_version(session, 'Person'):
                latest_change = changes[0]
                db_person = get_db_person(session, latest_change.uuid)

                # We delete here and continue, because there will be no record
                # to download from the Pure API when it has been deleted.
                if latest_change.change_type == 'DELETE':
                    if db_person:
                        delete_db_person(session, db_person)
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
                        if db_person:
                            # This record has been deleted from Pure but still exists in our local db:
                            delete_db_person(session, db_person)
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

                api_internal_person = response.transform(
                    pure_api_record_type,
                    r.json(),
                    version=pure_api_config.version
                )

                delete_merged_records(session, api_internal_person)

                if db_person and db_person_same_or_newer_than_api_internal_person(session, db_person, api_internal_person):
                    continue
                if already_loaded_same_api_internal_person(session, api_internal_person):
                    continue
                load_api_internal_person(session, api_internal_person, r.text)

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
