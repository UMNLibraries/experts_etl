import json
import re
import uuid

from dateutil.parser import isoparse
from sqlalchemy import and_, func

from experts_dw import db
from experts_dw.models import PureApiInternalPerson, PureApiInternalPersonHst, Person, PureOrg, PersonPureOrg, UmnPersonPureOrg, PersonScopusId
from experts_etl import loggers
from pureapi import response
from pureapi.client import Config

# defaults:

db_name = 'hotel'
transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'persons'
pure_api_record_logger = loggers.pure_api_record_logger(type=pure_api_record_type)

def extract_api_persons(session):
    for uuid in [result[0] for result in session.query(PureApiInternalPerson.uuid).distinct()]:
        persons = session.query(PureApiInternalPerson).filter(
                PureApiInternalPerson.uuid == uuid
            ).order_by(
                PureApiInternalPerson.modified.desc()
            ).all()
        # The first record in the list should be the latest:
        yield persons[0]

def get_person_ids(api_person):
    type_uri_key_map = {
        '/dk/atira/pure/person/personsources/employee': 'emplid',
        '/dk/atira/pure/person/personsources/umn': 'internet_id',
    }
    person_ids = {
        # None of these may exist, so we give them default values:
        'scopus_ids': set(),
        'emplid': None,
        'internet_id': None,
    }
    for _id in api_person.ids:
        if _id.type.uri == '/dk/atira/pure/person/personsources/scopusauthor':
            person_ids['scopus_ids'].add(_id.value.value)
        elif _id.type.uri in type_uri_key_map:
            key = type_uri_key_map[_id.type.uri]
            person_ids[key] = _id.value.value
    return person_ids

def mark_api_persons_as_processed(session, pure_api_record_logger, processed_api_person_uuids):
    for uuid in processed_api_person_uuids:
        for person in session.query(PureApiInternalPerson).filter(PureApiInternalPerson.uuid==uuid).all():

            person_hst = (
                session.query(PureApiInternalPersonHst)
                .filter(and_(
                    PureApiInternalPersonHst.uuid == person.uuid,
                    PureApiInternalPersonHst.modified == person.modified,
                ))
                .one_or_none()
            )

            if person_hst is None:
                person_hst = PureApiInternalPersonHst(
                    uuid=person.uuid,
                    modified=person.modified,
                    downloaded=person.downloaded
                )
                session.add(person_hst)

            pure_api_record_logger.info(person.json)
            session.delete(person)

def get_db_person(session, emplid):
    return (
        session.query(Person)
        .filter(Person.emplid == emplid)
        .one_or_none()
    )

def create_db_person(emplid):
    return Person(
        uuid=str(uuid.uuid4()),
        emplid=emplid,
        pure_internal='Y',
    )

def run(
    # Do we need other default functions here?
    extract_api_persons=extract_api_persons,
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
    api_person = None

    try:
        with db.session(db_name) as session:
            processed_api_person_uuids = []
            for db_api_person in extract_api_persons(session):
                api_person = response.transform(
                    pure_api_record_type,
                    json.loads(db_api_person.json),
                    version=pure_api_config.version
                )

                person_ids = get_person_ids(api_person)
                # Not sure that we should be requiring either of these in EDW, but we do for now,
                # in umn_person_pure_org, at least:
                if person_ids['emplid'] is None:
                    experts_etl_logger.info(
                        'skipping updates: missing emplid.',
                        extra={'pure_uuid': api_person.uuid, 'pure_api_record_type': pure_api_record_type}
                    )
                    continue
                if api_person.externalId is None:
                    experts_etl_logger.info(
                        'skipping updates: missing pure id.',
                        extra={'pure_uuid': api_person.uuid, 'pure_api_record_type': pure_api_record_type}
                    )
                    continue

                db_person = get_db_person(session, person_ids['emplid'])
                db_person_previously_existed = False
                if db_person:
                    db_person_previously_existed = True
                    if db_person.pure_modified and db_person.pure_modified >= db_api_person.modified:
                        # Skip this record, since we already have a newer one:
                        processed_api_person_uuids.append(db_api_person.uuid)
                        continue
                else:
                    db_person = create_db_person(person_ids['emplid'])

                # All internal persons should have this. Usually it will be the same as the emplid,
                # but sometimes the old SciVal identifier.
                db_person.pure_id = api_person.externalId

                db_person.pure_uuid = api_person.uuid
                db_person.internet_id = person_ids['internet_id']
                db_person.first_name = api_person.name.firstName
                db_person.last_name = api_person.name.lastName
                db_person.orcid = api_person.orcid
                db_person.hindex = api_person.scopusHIndex
                db_person.pure_modified = db_api_person.modified

                # TODO:
                # 2. Can internal persons have externalOrganisationAssociations (sp?)?
                #    Not sure yet, but they can have honoraryStaffOrganisationAssociations!
                # 3. If a person has an association with on org not in EDW yet, skip that person...
                #    or should we just immediately attempt to fetch the org and update EDW?
                #    For now, we'll skip that person.

                # Check for orgs not in EDW yet:

                api_org_uuids = set()
                for org_assoc in api_person.staffOrganisationAssociations:
                    api_org_uuids.add(org_assoc.organisationalUnit.uuid)

                db_org_uuids = set()
                if db_person_previously_existed:
                    # Avoid trying to query a person that doesn't exist in the db yet:
                    db_org_uuids = {db_org.pure_uuid for db_org in db_person.pure_orgs}

                api_only_org_uuids = api_org_uuids - db_org_uuids
                db_only_org_uuids = db_org_uuids - api_org_uuids

                # For now, skip this person if there are any orgs referenced in the api record
                # that we don't have in EDW:
                if len(api_only_org_uuids) > 0:
                    api_only_orgs_in_db = session.query(PureOrg).filter(
                        PureOrg.pure_uuid.in_(api_only_org_uuids)
                    ).all()
                    if len(api_only_org_uuids) > len(api_only_orgs_in_db):
                        experts_etl_logger.info(
                            'skipping updates: some associated orgs do not exist in EDW.',
                            extra={'pure_uuid': api_person.uuid, 'pure_api_record_type': pure_api_record_type}
                        )
                        continue

                ## umn person pure orgs aka staff organisation associations aka jobs

                # TODO: We may encounter duplicate jobs that break our uniqueness constraints.

                found_missing_job_description = False
                all_umn_person_pure_org_primary_keys = set()
                umn_person_pure_orgs = []
                for org_assoc in api_person.staffOrganisationAssociations:
                    job_description = next(
                        (job_description_text.value
                             for job_description_text
                             in org_assoc.jobDescription.text
                             if job_description_text.locale =='en_US'
                        ),
                        None
                    )
                    if job_description is None:
                        found_missing_job_description = True
                        break

                    # Due to transitioning from master list to xml syncs of jobs, we may encounter duplicates.
                    # This may also happen due to manual entering of jobs in the Pure UI.
                    umn_person_pure_org_primary_keys = frozenset([
                        'person_uuid:' + db_person.uuid,
                        'pure_org_uuid:' + org_assoc.organisationalUnit.uuid,
                        'job_description:' + job_description,
                        'start_date:' + org_assoc.period.startDate,
                    ])
                    if umn_person_pure_org_primary_keys in all_umn_person_pure_org_primary_keys:
                        experts_etl_logger.info(
                            'duplicate job found',
                            extra={
                                'umn_person_pure_org_primary_keys': umn_person_pure_org_primary_keys,
                                'pure_uuid': api_person.uuid,
                                'pure_api_record_type': pure_api_record_type,
                            }
                        )
                        continue
                    all_umn_person_pure_org_primary_keys.add(umn_person_pure_org_primary_keys)

                    umn_person_pure_org = UmnPersonPureOrg(
                        pure_org_uuid = org_assoc.organisationalUnit.uuid,
                        person_uuid = db_person.uuid,
                        pure_person_id = db_person.pure_id,
                        emplid = db_person.emplid,
                        # Skipping this for now:
                        pure_org_id = None,
                        job_description = job_description,

                        # Note: Both employmentType and staffType may be missing because they are not required
                        # fields in the Pure UI, which UMN staff sometimes use to enter jobs not in PeopleSoft.

                        # Called employed_as in EDW, which was all 'Academic' as of 2018-06-05.
                        # Probably due to an artifact of the old master list upload process, or a
                        # misunderstanding of it. The newer EDW tables for upload (sync) to Pure
                        # have similar values in default_employed_as, but they're the last segment of the
                        # employmentType uri.
                        employed_as = next(
                            (employment_type_text.value
                                for employment_type_text
                                in org_assoc.employmentType.term.text
                                if employment_type_text.locale =='en_US'
                            ),
                            None
                        ),

                        # Sometimes staffType will be 'non-academic', but we allowed space in EDW
                        # only for 'nonacademic' (without a hyphen):
                        staff_type = re.sub('[^a-zA-Z]+', '', next(
                            (staff_type_text.value
                                for staff_type_text
                                in org_assoc.staffType.term.text
                                if staff_type_text.locale =='en_US'
                            ),
                            None
                        ).lower()),

                        start_date = isoparse(org_assoc.period.startDate),
                        end_date = isoparse(org_assoc.period.endDate) if org_assoc.period.endDate else None,
                        primary = 'Y' if org_assoc.isPrimaryAssociation == True else 'N',
                    )
                    umn_person_pure_orgs.append(umn_person_pure_org)

                if found_missing_job_description:
                    experts_etl_logger.info(
          	            'skipping updates: one or more org associations are missing job descriptions',
                        extra={'pure_uuid': api_person.uuid, 'pure_api_record_type': pure_api_record_type}
          	        )
                    continue

                # Now we can add the person to the session, because there are no other
                # reasons for intentionally skipping it:
                session.add(db_person)

                # Now we can add the umn person pure orgs (i.e., jobs) to the session, too.
                # These are so complex, it's easiest to just delete and re-create them:
                session.query(UmnPersonPureOrg).filter(
                    UmnPersonPureOrg.person_uuid == db_person.uuid
                ).delete()
                for umn_person_pure_org in umn_person_pure_orgs:
                    session.add(umn_person_pure_org)

                ## person pure orgs

                for org_uuid in api_only_org_uuids:
                    person_pure_org = PersonPureOrg(
                        person_uuid = db_person.uuid,
                        pure_org_uuid = org_uuid,
                    )
                    session.add(person_pure_org)

                session.query(PersonPureOrg).filter(
                    PersonPureOrg.person_uuid == db_person.uuid,
                    PersonPureOrg.pure_org_uuid.in_(db_only_org_uuids)
                ).delete(synchronize_session=False)

                ## scopus ids

                db_scopus_ids = set()
                if db_person_previously_existed:
                    # Avoid trying to query a person that doesn't exist in the db yet:
                    db_scopus_ids = set(db_person.scopus_ids)
                api_only_scopus_ids = person_ids['scopus_ids'] - db_scopus_ids
                db_only_scopus_ids = db_scopus_ids - person_ids['scopus_ids']

                for scopus_id in api_only_scopus_ids:
                    person_scopus_id = PersonScopusId(
                        person_uuid = db_person.uuid,
                        scopus_id = scopus_id,
                    )
                    session.add(person_scopus_id)

                session.query(PersonScopusId).filter(
                    PersonScopusId.person_uuid == db_person.uuid,
                    PersonScopusId.scopus_id.in_(db_only_scopus_ids)
                ).delete(synchronize_session=False)

                processed_api_person_uuids.append(api_person.uuid)
                if len(processed_api_person_uuids) >= transaction_record_limit:
                    mark_api_persons_as_processed(session, pure_api_record_logger, processed_api_person_uuids)
                    processed_api_person_uuids = []
                    session.commit()

            mark_api_persons_as_processed(session, pure_api_record_logger, processed_api_person_uuids)
            session.commit()

    except Exception as e:
        formatted_exception = loggers.format_exception(e)
        experts_etl_logger.error(
            f'exception encountered during record transformation: {formatted_exception}',
            extra={'pure_uuid': api_person.uuid, 'pure_api_record_type': pure_api_record_type}
        )

    loggers.rollover(pure_api_record_logger)
    experts_etl_logger.info('ending: transforming/loading', extra={'pure_api_record_type': pure_api_record_type})
