import json

from sqlalchemy import and_, func

from experts_dw import db
from experts_dw.models import PureApiPub, PureApiPubHst, PureApiChange, PureApiChangeHst, Pub, PubPerson, PubPersonPureOrg, PubAuthorCollaboration
from pureapi import client, response
from pureapi.client import Config, PureAPIRequestException, PureAPIHTTPError
from experts_etl import loggers
from experts_etl.changes_buffer_managers import changes_for_family_ordered_by_uuid_version, record_changes_as_processed
from experts_etl.iso_datestr_parser import datetime_sans_ms_tz
from experts_etl.extractor_loaders.pure_api_external_persons import \
    get_db_person, \
    db_person_same_or_newer_than_api_external_person, \
    already_loaded_same_api_external_person, \
    load_api_external_person, \
    external_person_external_org_uuids, \
    load_external_orgs
from experts_etl.extractor_loaders.pure_api_external_organisations import \
    get_db_org, \
    db_org_same_or_newer_than_api_external_org, \
    already_loaded_same_api_external_org, \
    load_api_external_org

# defaults:

db_name = 'hotel'
transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'research-outputs'

# contributiontobookanthology
# contributiontomemorandum
# contributiontoconference
# contributiontoperiodical
# contributiontojournal

supported_pure_types = {
    'contributiontojournal': [
        'article', # Article: A presentation of new research with other scientists as primary audience.
        'letter', # Letter: A short description of new, important research results.
        'comment', # Comment/debate: Short commentary/contribution to debate in a scientific publication, often about former printed articles.
        'book', # Book/Film/Article review: Review of a book/film/article, published in a journal.
        'scientific', # Literature review: A critical review and evaluation of a (scientific) publication
        'editorial', # Editorial: An article-like text with the official opinion about a subject, from a journal's point of view.
        'special', # Special issue: A specific journal issue with focus on a special theme or subject.
        'abstract', # Meeting Abstract
        'systematicreview', # Review article: An article that review previous research on a topic and provides a summarization of the current understanding of the topic.
        'shortsurvey', # Short survey: A short survey is a mini-review of previous research on a topic, including a summarization of the current understanding of the topic. It is generally shorter than systematic review articles and contains a less extensive bibliography.
        'conferencearticle', # Conference article: An article that has been presented at a conference and published in a journal
  ],
}

# functions:

def pub_external_person_uuids(api_pub):
    return {
        person_assoc.externalPerson.uuid
        for person_assoc in api_pub.personAssociations
        if 'externalPerson' in person_assoc
    }

def load_external_persons(session, external_person_uuids: set):
    external_org_uuids = set()
    for api_external_person in client.filter_all_by_uuid_transformed('external-persons', uuids=list(external_person_uuids)):
        db_person = get_db_person(session, api_external_person.uuid)
        if db_person and db_person_same_or_newer_than_api_external_person(session, db_person, api_external_person):
            continue
        if already_loaded_same_api_external_person(session, api_external_person):
            continue
        load_api_external_person(session, api_external_person, json.dumps(api_external_person))

        external_org_uuids.update(
            external_person_external_org_uuids(api_external_person)
        )

    return external_org_uuids

def already_loaded_same_api_pub(session, api_pub):
    api_pub_modified = datetime_sans_ms_tz(
        api_pub.info.modifiedDate
    )
    db_api_pub = (
        session.query(PureApiPub)
        .filter(and_(
            PureApiPub.uuid == api_pub.uuid,
            PureApiPub.modified == api_pub_modified,
        ))
        .one_or_none()
    )
    if db_api_pub:
        return True
    return False

def get_db_pub(session, uuid):
    return (
        session.query(Pub)
        .filter(Pub.pure_uuid == uuid)
        .one_or_none()
    )

def delete_db_pub(session, db_pub):
    session.query(PubPerson).filter(
        PubPerson.pub_uuid == db_pub.uuid
    ).delete(synchronize_session=False)

    session.query(PubPersonPureOrg).filter(
        PubPersonPureOrg.pub_uuid == db_pub.uuid
    ).delete(synchronize_session=False)

    session.query(PubAuthorCollaboration).filter(
        PubAuthorCollaboration.pub_uuid == db_pub.uuid
    ).delete(synchronize_session=False)

    session.delete(db_pub)

def delete_merged_records(session, api_pub):
    for uuid in api_pub.info.previousUuids:
        db_pub = get_db_pub(session, uuid)
        if db_pub:
            delete_db_pub(session, db_pub)

def db_pub_same_or_newer_than_api_pub(session, db_pub, api_pub):
    api_pub_modified = datetime_sans_ms_tz(
        api_pub.info.modifiedDate
    )
    if db_pub.pure_modified and db_pub.pure_modified >= api_pub_modified:
        return True
    return False

def load_api_pub(session, api_pub, raw_json):
    api_pub_modified = datetime_sans_ms_tz(
        api_pub.info.modifiedDate
    )
    db_api_pub = PureApiPub(
        uuid=api_pub.uuid,
        json=raw_json,
        modified=api_pub_modified
    )
    session.add(db_api_pub)

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
            external_person_uuids = set()
            processed_changes = []
            for changes in changes_for_family_ordered_by_uuid_version(session, 'ResearchOutput'):
                latest_change = changes[0]
                db_pub = get_db_pub(session, latest_change.uuid)

                # We delete here and continue, because there will be no record
                # to download from the Pure API when it has been deleted.
                if latest_change.change_type == 'DELETE':
                    if db_pub:
                        delete_db_pub(session, db_pub)
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
                        if db_pub:
                            # This record has been deleted from Pure but still exists in our local db:
                            delete_db_pub(session, db_pub)
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

                api_pub = response.transform(
                    pure_api_record_type,
                    r.json(),
                    version=pure_api_config.version
                )

                delete_merged_records(session, api_pub)

                type_uri_parts = api_pub.type.uri.split('/')
                type_uri_parts.reverse()
                pure_subtype, pure_type, pure_parent_type = type_uri_parts[0:3]

                if pure_type not in supported_pure_types or pure_subtype not in supported_pure_types[pure_type]:
                    # Check whether we previously loaded this research output: its type(/subtype)
                    # may have changed to a type(/subtype) we do not support. If so, delete:
                    if db_pub:
                        delete_db_pub(session, db_pub)
                    continue
                if db_pub and db_pub_same_or_newer_than_api_pub(session, db_pub, api_pub):
                    continue
                if already_loaded_same_api_pub(session, api_pub):
                    continue

                external_person_uuids.update(
                    pub_external_person_uuids(api_pub)
                )
                load_api_pub(session, api_pub, r.text)

                processed_changes.extend(changes)
                if len(processed_changes) >= transaction_record_limit:
                    record_changes_as_processed(session, processed_changes)
                    processed_changes = []
                    session.commit()

            load_external_orgs(
                session,
                load_external_persons(session, external_person_uuids)
            )
            record_changes_as_processed(session, processed_changes)
            session.commit()

    except Exception as e:
        formatted_exception = loggers.format_exception(e)
        experts_etl_logger.error(
            f'exception encountered during record extraction: {formatted_exception}',
            extra={'pure_uuid': latest_change.uuid, 'pure_api_record_type': pure_api_record_type}
        )

    experts_etl_logger.info('ending: extracting/loading', extra={'pure_api_record_type': pure_api_record_type})
