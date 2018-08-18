from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from datetime import datetime, timezone
import json
import itertools
import uuid
from experts_dw import db
from sqlalchemy import and_, func
from experts_dw.models import PureApiPub, PureApiPubHst, Pub, Person, PubPerson, PureOrg, PubPersonPureOrg
from experts_etl import loggers
from pureapi import response

# defaults:

db_name = 'hotel'
transaction_record_limit = 100 
# Named for the Pure API endpoint:
pure_api_record_type = 'research-outputs'
pure_api_record_logger = loggers.pure_api_record_logger(type=pure_api_record_type)

def extract_api_pubs(session):
  sq = session.query(
    PureApiPub.uuid,
    func.max(PureApiPub.modified).label('modified')
  ).select_from(PureApiPub).group_by(PureApiPub.uuid).subquery()

  for pub in (session.query(PureApiPub)
    .join(
      sq,
      and_(PureApiPub.uuid==sq.c.uuid, PureApiPub.modified==sq.c.modified)
    )
    .all()
  ):
    yield pub

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
  experts_etl_logger=None
):
  if experts_etl_logger is None:
    experts_etl_logger = loggers.experts_etl_logger()
  experts_etl_logger.info('starting: {} processing'.format(pure_api_record_type))

  with db.session(db_name) as session:
    processed_api_pub_uuids = []
    for db_api_pub in extract_api_pubs(session):
      api_pub = response.transform(pure_api_record_type, json.loads(db_api_pub.json))      
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
    
      # Hard-coded for now:
      db_pub.type = 'article-journal'
    
      db_pub.title = api_pub.title
    
      db_pub.container_title = api_pub.journalAssociation.title.value
      db_pub.issn = api_pub.journalAssociation.issn.value if 'issn' in api_pub.journalAssociation else None
    
      if 'publicationDate' in api_pub.publicationStatuses[0]:
        pub_date = api_pub.publicationStatuses[0].publicationDate
        year = pub_date.year
        issued_precision = 366
        month = 1
        day = 1
        if 'month' in pub_date:
          month = pub_date.month
          issued_precision = 31
        if 'day' in pub_date:
          day = pub_date.day
          issued_precision = 1
    
        db_pub.issued = datetime(year, month, day, tzinfo=timezone.utc)
        db_pub.issued_precision = issued_precision
    
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
            'Skipping updates for research output: owner pure org does not exist in EDW.',
            extra={'pure_uuid': api_pub.uuid}
          )
          continue
        db_pub.owner_pure_org_uuid = owner_pure_org_uuid
      else:
        # TODO: We do this because currently owner_pure_org_uuid is not null. We may want to change that.
        experts_etl_logger.info(
          'Skipping updates for research output: no owner pure org.',
          extra={'pure_uuid': api_pub.uuid}
        )
        continue

      ## associations
    
      missing_person = False
      missing_person_pure_uuid = False
      missing_org = False
      person_ordinal = 0
      all_person_uuids = set()
      pub_persons = []
      pub_person_pure_orgs = []
    
      for person_assoc in api_pub.personAssociations:
        person_pure_uuid = None
        if 'person' in person_assoc:
          person_pure_uuid = person_assoc.person.uuid
          person_pure_internal = 'Y'
        if 'externalPerson' in person_assoc:
          person_pure_uuid = person_assoc.externalPerson.uuid
          person_pure_internal = 'N'
        if person_pure_uuid == None:
          missing_person_pure_uuid = True
          break
    
        db_person = session.query(Person).filter(
          Person.pure_uuid == person_pure_uuid
        ).one_or_none()
        if db_person == None:
          missing_person = True
          break
    
        person_ordinal += 1
    
        if db_person.uuid not in all_person_uuids:
          pub_person = PubPerson(
            pub_uuid = db_pub.uuid,
            person_uuid = db_person.uuid,
            person_ordinal = person_ordinal,
      
            # TODO: This needs work. We may have tried mapping these to CSL values at
            # one point, but now we're just taking what Pure gives us.
            person_role = person_assoc.personRole[0].value.lower(),
      
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
          'Skipping updates for research output: one or more associated persons do not exist in EDW.',
          extra={'pure_uuid': api_pub.uuid}
        )
        continue
    
      if missing_person_pure_uuid:
        experts_etl_logger.info(
          'Skipping updates for research output: one or more associated persons has no pure uuid.',
          extra={'pure_uuid': api_pub.uuid}
        )
        continue
    
      if missing_org:
        experts_etl_logger.info(
          'Skipping updates for research output: one or more associated orgs do not exist in EDW.',
          extra={'pure_uuid': api_pub.uuid}
        )
        continue

      # Now we can add the pub to the session, because there are no other
      # reasons for intentionally skipping it:
      session.add(db_pub)

      # Now we can also delete and re-create the associations for this research output:

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

  loggers.rollover(pure_api_record_logger)
  experts_etl_logger.info('ending: {} processing'.format(pure_api_record_type))
