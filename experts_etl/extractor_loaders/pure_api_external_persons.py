from sqlalchemy import and_, func
from experts_dw import db
from experts_dw.models import PureApiExternalPerson, PureApiExternalPersonHst, PureApiChange, PureApiChangeHst, Person, PubPerson, PubPersonPureOrg, PersonPureOrg, PersonScopusId
from experts_etl import transformers
from pureapi import client, response
from pureapi.exceptions import PureAPIClientRequestException
from experts_etl import loggers

# defaults:

db_name = 'hotel'
transaction_record_limit = 100 
# Named for the Pure API endpoint:
pure_api_record_type = 'external-persons'
experts_etl_logger = loggers.experts_etl_logger()

def extract_api_changes(session):
  sq = session.query(
    PureApiChange.uuid,
    func.max(PureApiChange.version).label('version')
  ).select_from(PureApiChange).group_by(PureApiChange.uuid).subquery()

  for change in (session.query(PureApiChange)
    .join(
      sq,
      and_(PureApiChange.uuid==sq.c.uuid, PureApiChange.version==sq.c.version)
    )
    .filter(PureApiChange.family_system_name=='ExternalPerson')
    .all()
  ):
    yield change

# functions:

def api_external_person_exists_in_db(session, api_external_person):
  api_external_person_modified = transformers.iso_8601_string_to_datetime(api_external_person.info.modifiedDate)

  db_api_external_person_hst = (
    session.query(PureApiExternalPersonHst)
    .filter(and_(
      PureApiExternalPersonHst.uuid == api_external_person.uuid,
      PureApiExternalPersonHst.modified == api_external_person_modified,
    ))
    .one_or_none()
  )
  if db_api_external_person_hst:
    return True

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

def db_person_newer_than_api_person(session, api_person):
  api_person_modified = transformers.iso_8601_string_to_datetime(api_person.info.modifiedDate)
  db_person = get_db_person(session, api_person.uuid)
  # We need the replace(tzinfo=None) here, or we get errors like:
  # TypeError: can't compare offset-naive and offset-aware datetimes
  if db_person and db_person.pure_modified and db_person.pure_modified >= api_person_modified.replace(tzinfo=None):
    return True
  return False

def load_api_external_person(session, api_external_person, raw_json):
  db_api_external_person = PureApiExternalPerson(
    uuid=api_external_person.uuid,
    json=raw_json,
    modified=transformers.iso_8601_string_to_datetime(api_external_person.info.modifiedDate)
  )
  session.add(db_api_external_person)

def mark_api_changes_as_processed(session, processed_api_change_uuids):
  for uuid in processed_api_change_uuids:
    for change in session.query(PureApiChange).filter(PureApiChange.uuid==uuid).all():

      change_hst = (
        session.query(PureApiChangeHst)
        .filter(and_(
          PureApiChangeHst.uuid == change.uuid,
          PureApiChangeHst.version == change.version,
        ))
        .one_or_none()
      )

      if change_hst is None:
        change_hst = PureApiChangeHst(
          uuid=change.uuid,
          family_system_name=change.family_system_name,
          change_type=change.change_type,
          version=change.version,
          downloaded=change.downloaded
        )
        session.add(change_hst)

      session.delete(change)

# entry point/public api:

def run(
  # Do we need other default functions here?
  extract_api_changes=extract_api_changes,
  db_name=db_name,
  transaction_record_limit=transaction_record_limit,
  experts_etl_logger=experts_etl_logger
):
  experts_etl_logger.info('Starting {} extracting/loading...'.format(pure_api_record_type))

  with db.session(db_name) as session:
    processed_api_change_uuids = []
    for api_change in extract_api_changes(session):

      if api_change.change_type == 'DELETE':
        db_person = get_db_person(session, api_change.uuid)
        if db_person:
          delete_db_person(session, db_person)
        processed_api_change_uuids.append(api_change.uuid)
        continue

      r = None
      try:
        r = client.get(pure_api_record_type + '/' + api_change.uuid)
      except PureAPIClientRequestException:
        # This is probably a 404, due to the record being deleted. For now, just skip it.
        processed_api_change_uuids.append(api_change.uuid)
        continue
      except Exception:
        raise
      api_external_person = response.transform(pure_api_record_type, r.json())

      load = True
      if db_person_newer_than_api_person(session, api_external_person):
        load = False
      if api_external_person_exists_in_db(session, api_external_person):
        load = False
      if load:
        load_api_external_person(session, api_external_person, r.text)
  
      processed_api_change_uuids.append(api_change.uuid)
      if len(processed_api_change_uuids) >= transaction_record_limit:
        mark_api_changes_as_processed(session, processed_api_change_uuids)
        processed_api_change_uuids = []
        session.commit()
  
    mark_api_changes_as_processed(session, processed_api_change_uuids)
    session.commit()

  experts_etl_logger.info('Ending {} extracting/loading...'.format(pure_api_record_type))
