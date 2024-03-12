import json

from sqlalchemy import and_, func

from experts_dw import db
from experts_dw.models import PureApiExternalPerson, PureApiExternalPersonHst, PureApiChange, PureApiChangeHst, Person, PubPerson, PubPersonPureOrg, PersonPureOrg, PersonScopusId
from pureapi import client, response
from pureapi.client import Config, PureAPIRequestException, PureAPIHTTPError
from experts_etl import loggers
from experts_etl.changes_buffer_managers import changes_for_family_ordered_by_uuid_version, record_changes_as_processed
from experts_etl.iso_datestr_parser import datetime_sans_ms_tz
from experts_etl.extractor_loaders.pure_api_external_organisations import \
    get_db_org, \
    db_org_same_or_newer_than_api_external_org, \
    already_loaded_same_api_external_org, \
    load_api_external_org

# defaults:

db_name = 'hotel'
transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'external-persons'

# functions:

def external_person_external_org_uuids(api_external_person):
    return {org.uuid for org in api_external_person.externalOrganisations}

def load_external_orgs(session, external_org_uuids: set):
    for api_external_org in client.filter_all_by_uuid_transformed('external-organisations', uuids=list(external_org_uuids)):
        db_org = get_db_org(session, api_external_org.uuid)
        if db_org and db_org_same_or_newer_than_api_external_org(session, db_org, api_external_org):
            continue
        if already_loaded_same_api_external_org(session, api_external_org):
            continue
        load_api_external_org(session, api_external_org, json.dumps(api_external_org))

def already_loaded_same_api_external_person(session, api_external_person):
    api_external_person_modified = datetime_sans_ms_tz(
        api_external_person.info.modifiedDate
    )
    db_api_external_person = (
        session.query(PureApiExternalPerson)
        .filter(and_(
            PureApiExternalPerson.uuid == api_external_person.uuid,
            PureApiExternalPerson.modified == api_external_person_modified,
        ))
        .one_or_none()
    )
    if db_api_external_person:
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

    session.query(PersonScopusId).filter(
        PersonScopusId.person_uuid == db_person.uuid
    ).delete(synchronize_session=False)

    session.delete(db_person)

def delete_merged_records(session, api_person):
    for uuid in api_person.info.previousUuids:
        db_person = get_db_person(session, uuid)
        if db_person:
            delete_db_person(session, db_person)

def db_person_same_or_newer_than_api_external_person(session, db_person, api_external_person):
    api_external_person_modified = datetime_sans_ms_tz(
        api_external_person.info.modifiedDate
    )
    if db_person.pure_modified and db_person.pure_modified >= api_external_person_modified:
        return True
    return False

def load_api_external_person(session, api_external_person, raw_json):
    api_external_person_modified = datetime_sans_ms_tz(
        api_external_person.info.modifiedDate
    )
    db_api_external_person = PureApiExternalPerson(
        uuid=api_external_person.uuid,
        json=raw_json,
        modified=api_external_person_modified
    )
    session.add(db_api_external_person)

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
            external_org_uuids = set()
            processed_changes = []
            for changes in changes_for_family_ordered_by_uuid_version(session, 'ExternalPerson'):
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

                api_external_person = response.transform(
                    pure_api_record_type,
                    r.json(),
                    version=pure_api_config.version
                )

                delete_merged_records(session, api_external_person)

                if db_person and db_person_same_or_newer_than_api_external_person(session, db_person, api_external_person):
                    continue
                if already_loaded_same_api_external_person(session, api_external_person):
                    continue

                external_org_uuids.update(
                    external_person_external_org_uuids(api_external_person)
                )
                load_api_external_person(session, api_external_person, r.text)

                processed_changes.extend(changes)
                if len(processed_changes) >= transaction_record_limit:
                    record_changes_as_processed(session, processed_changes)
                    processed_changes = []
                    session.commit()

            load_external_orgs(session, external_org_uuids)
            record_changes_as_processed(session, processed_changes)
            session.commit()

    except Exception as e:
        formatted_exception = loggers.format_exception(e)
        experts_etl_logger.error(
            f'exception encountered during record extraction: {formatted_exception}',
            extra={'pure_uuid': latest_change.uuid, 'pure_api_record_type': pure_api_record_type}
        )

    experts_etl_logger.info('ending: extracting/loading', extra={'pure_api_record_type': pure_api_record_type})
