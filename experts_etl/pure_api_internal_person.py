import json
import datetime
import re
from sqlalchemy import and_, func
from experts_dw import db
from experts_dw.models import PureApiInternalPerson, PureApiInternalPersonHst, PureApiChange, PureApiChangeHst, Person, PubPerson, PubPersonPureOrg, PersonPureOrg, PersonScopusId, UmnPersonPureOrg
from . import transformers
from pureapi import client, response

# defaults:

db_name = 'hotel'
transaction_record_limit = 100 

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
    .filter(PureApiChange.family_system_name=='Person')
    .all()
  ):
    yield change

# functions:

def api_internal_person_exists_in_db(session, api_internal_person):
  api_internal_person_modified = transformers.iso_8601_string_to_datetime(api_internal_person.info.modifiedDate)

  db_api_internal_person_hst = (
    session.query(PureApiInternalPersonHst)
    .filter(and_(
      PureApiInternalPersonHst.uuid == api_internal_person.uuid,
      PureApiInternalPersonHst.modified == api_internal_person_modified,
    ))
    .one_or_none()
  )
  if db_api_internal_person_hst:
    return True

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

def db_person_newer_than_api_person(session, api_person):
  api_person_modified = transformers.iso_8601_string_to_datetime(api_person.info.modifiedDate)
  db_person = get_db_person(session, api_person.uuid)
  # We need the replace(tzinfo=None) here, or we get errors like:
  # TypeError: can't compare offset-naive and offset-aware datetimes
  if db_person and db_person.pure_modified and db_person.pure_modified >= api_person_modified.replace(tzinfo=None):
    return True
  return False

def load_api_internal_person(session, api_internal_person, raw_json):
  db_api_internal_person = PureApiInternalPerson(
    uuid=api_internal_person.uuid,
    json=raw_json,
    modified=transformers.iso_8601_string_to_datetime(api_internal_person.info.modifiedDate)
  )
  session.add(db_api_internal_person)

def mark_api_changes_as_processed(session, processed_api_change_uuids):
  marked_uuids = []
  for uuid in processed_api_change_uuids:
    for change in session.query(PureApiChange).filter(PureApiChange.uuid==uuid).all():

      marked_uuids.append(change.uuid)

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

      if (len(marked_uuids) % transaction_record_limit) == 0:
        session.commit()
  session.commit()

# entry point/public api:

def run(
  # Do we need other default functions here?
  extract_api_changes=extract_api_changes,
  db_name=db_name,
  transacton_record_limit=transaction_record_limit
):
  with db.session(db_name) as session:
    processed_api_change_uuids = []
    for api_change in extract_api_changes(session):

      processed_api_change_uuids.append(api_change.uuid)

      if api_change.change_type == 'DELETE':
        db_person = get_db_person(session, api_change.uuid)
        if db_person:
          delete_db_person(session, db_person)
        continue

      r = None
      try:
        r = client.get('persons/' + api_change.uuid)
      except PureAPIClientRequestException:
        # This is probably a 404, due to the record being deleted. For now, just skip it.
        continue
      except Exception:
        raise
      api_internal_person = response.transform('persons', r.json())
      if db_person_newer_than_api_person(session, api_internal_person):
        continue
      if api_internal_person_exists_in_db(session, api_internal_person):
        continue
      load_api_internal_person(session, api_internal_person, r.text)
  
      if (len(processed_api_change_uuids) % transaction_record_limit) == 0:
        session.commit()
    session.commit()
  
    mark_api_changes_as_processed(session, processed_api_change_uuids)
