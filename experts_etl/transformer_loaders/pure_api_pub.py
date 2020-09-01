from datetime import datetime, timezone
import json
import itertools
import uuid
from experts_dw import db
from sqlalchemy import and_, func
from experts_dw.models import PureApiPub, PureApiPubHst, Pub, Person, PubPerson, PureOrg, PubPersonPureOrg, AuthorCollaboration, PubAuthorCollaboration
from experts_etl import loggers
from pureapi import response
from pureapi.client import Config

# defaults:

db_name = 'hotel'
transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'research-outputs'
pure_api_record_logger = loggers.pure_api_record_logger(type=pure_api_record_type)

pure_edw_pub_state_map = {
    'published': 'issued',
    'epub': 'eissued',
    'unpublished': 'unissued',
    'inprep': 'inprep',
    'submitted': 'submitted',
    'in_press': 'inpress',
    'inpress': 'inpress',
}

def nullify_pub_states(db_pub):
    for base_column_name in pure_edw_pub_state_map.values():
        for column_name in [base_column_name, base_column_name + '_current', base_column_name + '_precision']:
            setattr(db_pub, column_name, None)

def update_pub_state(db_pub, api_pub_state):
    if 'uri' not in api_pub_state.publicationStatus:
        return
    state_uri_parts = api_pub_state.publicationStatus.uri.split('/')
    state_uri_parts.reverse()
    pure_pub_state = state_uri_parts[0]
    base_column_name = pure_edw_pub_state_map[pure_pub_state]

    if 'current' in api_pub_state and api_pub_state.current is True:
        setattr(db_pub, base_column_name + '_current', True)
    else:
        setattr(db_pub, base_column_name + '_current', False)

    if 'publicationDate' in api_pub_state:
        state_date = api_pub_state.publicationDate
        year = state_date.year
        state_precision = 366
        month = 1
        day = 1
        if 'month' in state_date:
            month = state_date.month
            state_precision = 31
        if 'day' in state_date:
            day = state_date.day
            state_precision = 1

        setattr(db_pub, base_column_name, datetime(year, month, day, tzinfo=timezone.utc))
        setattr(db_pub, base_column_name + '_precision', state_precision)

def extract_api_pubs(session):
    for uuid in [result[0] for result in session.query(PureApiPub.uuid).distinct()]:
        pubs = session.query(PureApiPub).filter(
                PureApiPub.uuid == uuid
            ).order_by(
                PureApiPub.modified.desc()
            ).all()
        # The first record in the list should be the latest:
        yield pubs[0]

def get_pub_ids(api_pub):
    pub_ids = {
        # None of these may exist, so we give them default values:
        'scopus_id': None,
        'pmid': None,
        'doi': None,
    }

    if api_pub.externalIdSource == 'Scopus':
        pub_ids['scopus_id'] = api_pub.externalId

    for version in api_pub.electronicVersions:
        if 'doi' in version:
          pub_ids['doi'] = version.doi

    source_key_map = {
        'PubMed': 'pmid',
        'PubMedCentral': 'pubmed_central_id',
        'QABO': 'qabo_id',
    }
    for _id in api_pub.info.additionalExternalIds:
        if _id.idSource in source_key_map:
          key = source_key_map[_id.idSource]
          pub_ids[key] = _id.value

    return pub_ids

def mark_api_pubs_as_processed(session, pure_api_record_logger, processed_api_pub_uuids):
    for uuid in processed_api_pub_uuids:
        for pub in session.query(PureApiPub).filter(PureApiPub.uuid==uuid).all():

            pub_hst = (
                session.query(PureApiPubHst)
                .filter(and_(
                    PureApiPubHst.uuid == pub.uuid,
                    PureApiPubHst.modified == pub.modified,
                ))
                .one_or_none()
            )

            if pub_hst is None:
                pub_hst = PureApiPubHst(
                    uuid=pub.uuid,
                    modified=pub.modified,
                    downloaded=pub.downloaded
                )
                session.add(pub_hst)

            pure_api_record_logger.info(pub.json)
            session.delete(pub)

def get_db_pub(session, pure_uuid):
    return (
        session.query(Pub)
        .filter(Pub.pure_uuid == pure_uuid)
        .one_or_none()
    )

def create_db_pub(api_pub):
    return Pub(
        uuid = str(uuid.uuid4()),
        pure_uuid = api_pub.uuid,
    )

def run(
    # Do we need other default functions here?
    extract_api_pubs=extract_api_pubs,
    db_name=db_name,
    transaction_record_limit=transaction_record_limit,
    pure_api_record_logger=pure_api_record_logger,
    experts_etl_logger=None,
    pure_api_config=None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: transforming/loading', extra={'pure_api_record_type': pure_api_record_type})

    if pure_api_config is None:
        pure_api_config = Config()

    # Capture the current record for each iteration, so we can log it in case of an exception:
    api_pub = None

    try:
        with db.session(db_name) as session:
            processed_api_pub_uuids = []
            for db_api_pub in extract_api_pubs(session):
                api_pub = response.transform(
                    pure_api_record_type,
                    json.loads(db_api_pub.json),
                    version=pure_api_config.version
                )
                db_pub = get_db_pub(session, db_api_pub.uuid)
                db_pub_previously_existed = False
                if db_pub:
                    db_pub_previously_existed = True
                    if db_pub.pure_modified and db_pub.pure_modified >= db_api_pub.modified:
                        # Skip this record, since we already have a newer one:
                        processed_api_pub_uuids.append(db_api_pub.uuid)
                        continue
                else:
                    db_pub = create_db_pub(api_pub)

                pub_ids = get_pub_ids(api_pub)
                db_pub.scopus_id = pub_ids['scopus_id']
                db_pub.pmid = pub_ids['pmid']
                db_pub.doi = pub_ids['doi']

                # Commented out for now, because we will rely more on pure types and subtypes (below):
                #db_pub.type = 'article-journal'

                type_uri_parts = api_pub.type.uri.split('/')
                type_uri_parts.reverse()
                pure_subtype, pure_type, pure_parent_type = type_uri_parts[0:3]
                db_pub.pure_type = pure_type
                db_pub.pure_subtype = pure_subtype

                db_pub.title = api_pub.title.value

                db_pub.container_title = api_pub.journalAssociation.title.value
                db_pub.issn = api_pub.journalAssociation.issn.value if 'issn' in api_pub.journalAssociation else None

                nullify_pub_states(db_pub)
                for api_pub_state in api_pub.publicationStatuses:
                    update_pub_state(db_pub, api_pub_state)

                db_pub.volume = api_pub.volume
                db_pub.issue = api_pub.journalNumber
                db_pub.pages = api_pub.pages
                db_pub.citation_total = api_pub.totalScopusCitations

                db_pub.pure_modified = db_api_pub.modified

                if 'managingOrganisationalUnit' in api_pub:
                    owner_pure_org_uuid = api_pub.managingOrganisationalUnit.uuid
                    owner_pure_org = session.query(PureOrg).filter(
                        PureOrg.pure_uuid == owner_pure_org_uuid
                    ).one_or_none()
                    if owner_pure_org == None:
                        experts_etl_logger.info(
                            'skipping updates: owner pure org does not exist in EDW.',
                            extra={'pure_uuid': api_pub.uuid, 'pure_api_record_type': pure_api_record_type}
                        )
                        continue
                    db_pub.owner_pure_org_uuid = owner_pure_org_uuid
                else:
                    # TODO: We do this because currently owner_pure_org_uuid is not null. We may want to change that.
                    experts_etl_logger.info(
                        'skipping updates: no owner pure org.',
                        extra={'pure_uuid': api_pub.uuid, 'pure_api_record_type': pure_api_record_type}
                    )
                    continue

                ## associations

                author_ordinal = 0
                missing_person = False
                missing_person_pure_uuid = False
                missing_org = False
                pub_author_collabs = []
                all_author_collab_uuids = set()
                all_person_uuids = set()
                pub_persons = []
                pub_person_pure_orgs = []

                # personAssociations can contain authorCollaboration's, which are not persons at all,
                # so we call this variable author_assoc, to be more accurate here:
                for author_assoc in api_pub.personAssociations:
                    author_ordinal += 1

                    if 'authorCollaboration' in author_assoc:
                        author_collab_assoc = author_assoc
                        author_collab_pure_uuid = author_collab_assoc.authorCollaboration.uuid

                        # Sometimes Pure records contain duplicate author collaborations. Grrr...
                        if author_collab_pure_uuid in all_author_collab_uuids:
                            continue
                        all_author_collab_uuids.add(author_collab_pure_uuid)

                        db_author_collab = session.query(AuthorCollaboration).filter(
                            AuthorCollaboration.pure_uuid == author_collab_pure_uuid
                        ).one_or_none()
                        if db_author_collab is None:
                            db_author_collab = AuthorCollaboration(
                                uuid = str(uuid.uuid4()),
                                pure_uuid = author_collab_pure_uuid,
                            )
                        # This is less than ideal, but for now we just update the author collaboration
                        # name with whatever value this record includes:
                        db_author_collab.name = next(
                            (author_collab_text.value
                                for author_collab_text
                                in author_collab_assoc.authorCollaboration.name.text
                                if author_collab_text.locale =='en_US'
                            ),
                            None
                        )
                        session.add(db_author_collab)

                        pub_author_collab = PubAuthorCollaboration(
                            pub_uuid = db_pub.uuid,
                            author_collaboration_uuid = db_author_collab.uuid,
                            author_ordinal = author_ordinal,

                            # TODO: This needs work. We may have tried mapping these to CSL values at
                            # one point, but now we're just taking what Pure gives us.
                            author_role = next(
                                (author_role_text.value
                                    for author_role_text
                                    in author_collab_assoc.personRole.term.text
                                    if author_role_text.locale =='en_US'
                                ),
                                None
                            ).lower(),
                        )
                        pub_author_collabs.append(pub_author_collab)

                        continue

                    person_assoc = author_assoc
                    person_pure_uuid = None
                    if 'person' in person_assoc:
                        person_pure_uuid = person_assoc.person.uuid
                        person_pure_internal = 'Y'
                    if 'externalPerson' in person_assoc:
                        person_pure_uuid = person_assoc.externalPerson.uuid
                        person_pure_internal = 'N'
                    if person_assoc is not None and person_pure_uuid is None:
                        missing_person_pure_uuid = True
                        break

                    db_person = session.query(Person).filter(
                        Person.pure_uuid == person_pure_uuid
                    ).one_or_none()
                    if db_person == None:
                        missing_person = True
                        break

                    if db_person.uuid not in all_person_uuids:
                        pub_person = PubPerson(
                            pub_uuid = db_pub.uuid,
                            person_uuid = db_person.uuid,
                            person_ordinal = author_ordinal,

                            # TODO: This needs work. We may have tried mapping these to CSL values at
                            # one point, but now we're just taking what Pure gives us.
                            person_role = next(
                                (person_role_text.value
                                    for person_role_text
                                    in person_assoc.personRole.term.text
                                    if person_role_text.locale =='en_US'
                                ),
                                None
                            ).lower(),

                            person_pure_internal = person_pure_internal,
                            first_name = person_assoc.name.firstName if 'firstName' in person_assoc.name else None,
                            last_name = person_assoc.name.lastName if 'lastName' in person_assoc.name else None,
                            emplid = db_person.emplid,
                        )
                        pub_persons.append(pub_person)
                        all_person_uuids.add(db_person.uuid)
                    else:
                        continue

                    all_person_org_uuids = set()
                    for api_pure_org in itertools.chain(person_assoc.organisationalUnits, person_assoc.externalOrganisations):
                        db_pure_org = session.query(PureOrg).filter(
                            PureOrg.pure_uuid == api_pure_org.uuid
                        ).one_or_none()
                        if db_pure_org == None:
                            missing_org = True
                            break

                        person_org_uuids = frozenset([db_person.uuid, db_pure_org.pure_uuid])
                        if person_org_uuids not in all_person_org_uuids:
                            pub_person_pure_org = PubPersonPureOrg(
                                pub_uuid = db_pub.uuid,
                                person_uuid = db_person.uuid,
                                pure_org_uuid = db_pure_org.pure_uuid,
                            )
                            pub_person_pure_orgs.append(pub_person_pure_org)
                            all_person_org_uuids.add(person_org_uuids)
                        else:
                            continue
                    if missing_org:
                        break

                if missing_person:
                    experts_etl_logger.info(
                        'skipping updates: one or more associated persons do not exist in EDW.',
                        extra={'pure_uuid': api_pub.uuid, 'pure_api_record_type': pure_api_record_type}
                    )
                    continue

                if missing_person_pure_uuid:
                    experts_etl_logger.info(
                        'skipping updates: one or more associated persons has no pure uuid.',
                        extra={'pure_uuid': api_pub.uuid, 'pure_api_record_type': pure_api_record_type}
                    )
                    continue

                if missing_org:
                    experts_etl_logger.info(
                        'skipping updates: one or more associated orgs do not exist in EDW.',
                        extra={'pure_uuid': api_pub.uuid, 'pure_api_record_type': pure_api_record_type}
                    )
                    continue

                # Now we can add the pub to the session, because there are no other
                # reasons for intentionally skipping it:
                session.add(db_pub)

                # Now we can also delete and re-create the associations for this research output:

                session.query(PubAuthorCollaboration).filter(
                    PubAuthorCollaboration.pub_uuid == db_pub.uuid
                ).delete(synchronize_session=False)
                for pub_author_collab in pub_author_collabs:
                    session.add(pub_author_collab)

                session.query(PubPerson).filter(
                    PubPerson.pub_uuid == db_pub.uuid
                ).delete(synchronize_session=False)
                for pub_person in pub_persons:
                    session.add(pub_person)

                session.query(PubPersonPureOrg).filter(
                    PubPersonPureOrg.pub_uuid == db_pub.uuid
                ).delete(synchronize_session=False)
                for pub_person_pure_org in pub_person_pure_orgs:
                    session.add(pub_person_pure_org)

                processed_api_pub_uuids.append(api_pub.uuid)
                if len(processed_api_pub_uuids) >= transaction_record_limit:
                    mark_api_pubs_as_processed(session, pure_api_record_logger, processed_api_pub_uuids)
                    processed_api_pub_uuids = []
                    session.commit()

            mark_api_pubs_as_processed(session, pure_api_record_logger, processed_api_pub_uuids)
            session.commit()

    except Exception as e:
        formatted_exception = loggers.format_exception(e)
        experts_etl_logger.error(
            f'exception encountered during record transformation: {formatted_exception}',
            extra={'pure_uuid': api_pub.uuid, 'pure_api_record_type': pure_api_record_type}
        )

    loggers.rollover(pure_api_record_logger)
    experts_etl_logger.info('ending: transforming/loading', extra={'pure_api_record_type': pure_api_record_type})
