import re
from sqlalchemy import and_, func
from experts_dw import db
from experts_dw.models import PureApiExternalOrg, PureApiExternalOrgHst, PureApiChange, PureApiChangeHst, PureOrg, PersonPureOrg, PubPersonPureOrg, Pub
from experts_etl import transformers
from pureapi import client, response
from pureapi.exceptions import PureAPIClientRequestException, PureAPIClientHTTPError
from experts_etl import loggers

# defaults:

db_name = 'hotel'
transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'external-organisations'

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
    .filter(PureApiChange.family_system_name=='ExternalOrganisation')
    .all()
  ):
    yield change

# functions:

def api_external_org_exists_in_db(session, api_external_org):
  api_external_org_modified = transformers.iso_8601_string_to_datetime(api_external_org.info.modifiedDate)

  db_api_external_org_hst = (
    session.query(PureApiExternalOrgHst)
    .filter(and_(
      PureApiExternalOrgHst.uuid == api_external_org.uuid,
      PureApiExternalOrgHst.modified == api_external_org_modified,
    ))
    .one_or_none()
  )
  if db_api_external_org_hst:
    return True

  db_api_external_org = (
    session.query(PureApiExternalOrg)
    .filter(and_(
      PureApiExternalOrg.uuid == api_external_org.uuid,
      PureApiExternalOrg.modified == api_external_org_modified,
    ))
    .one_or_none()
  )
  if db_api_external_org:
    return True

  return False

def get_db_org(session, uuid):
  return (
    session.query(PureOrg)
    .filter(PureOrg.pure_uuid == uuid)
    .one_or_none()
  )

def delete_db_org(session, db_org):
  # We may be able to do this with less code by using
  # the sqlalchemy delete cascade somehow:
  session.query(PubPersonPureOrg).filter(
    PubPersonPureOrg.pure_org_uuid == db_org.pure_uuid
  ).delete(synchronize_session=False)

  session.query(PersonPureOrg).filter(
    PersonPureOrg.pure_org_uuid == db_org.pure_uuid
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

def load_api_external_org(session, api_external_org, raw_json):
  db_api_external_org = PureApiExternalOrg(
    uuid=api_external_org.uuid,
    json=raw_json,
    modified=transformers.iso_8601_string_to_datetime(api_external_org.info.modifiedDate)
  )
  session.add(db_api_external_org)

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
  experts_etl_logger=None
):
  if experts_etl_logger is None:
    experts_etl_logger = loggers.experts_etl_logger()
  experts_etl_logger.info('starting: extracting/loading', extra={'pure_api_record_type': pure_api_record_type})

  with db.session(db_name) as session:
    processed_api_change_uuids = []
    for api_change in extract_api_changes(session):

      db_org = get_db_org(session, api_change.uuid)

      if api_change.change_type == 'DELETE':
        if db_org:
          delete_db_org(session, db_org)
        processed_api_change_uuids.append(api_change.uuid)
        # The associated record should no longer exist in Pure, so there's nothing else to do:
        continue

      r = None
      try:
        r = client.get(pure_api_record_type + '/' + api_change.uuid)
      except PureAPIClientHTTPError as e:
        # This record has been deleted from Pure but still exists in our local db:
        if e.response.status_code == 404 and db_org:
          delete_db_org(session, db_org)
          processed_api_change_uuids.append(api_change.uuid)
        # Some other HTTP error occurred. Skip for now and try later.
        continue
      except PureAPIClientRequestException:
        # Some other ambiguous HTTP-communication-related error occurred. Skip for now and try later.
        continue
      except Exception:
        # Some unexpected error occurred from which we probably can't recover.
        raise
      api_external_org = response.transform(pure_api_record_type, r.json())

      delete_merged_records(session, api_external_org)

      load = True
      if db_org_newer_than_api_org(session, api_external_org):
        load = False
      if api_external_org_exists_in_db(session, api_external_org):
        load = False
      if load:
        load_api_external_org(session, api_external_org, r.text)

      processed_api_change_uuids.append(api_change.uuid)
      if len(processed_api_change_uuids) >= transaction_record_limit:
        mark_api_changes_as_processed(session, processed_api_change_uuids)
        processed_api_change_uuids = []
        session.commit()

    mark_api_changes_as_processed(session, processed_api_change_uuids)
    session.commit()

  experts_etl_logger.info('ending: extracting/loading', extra={'pure_api_record_type': pure_api_record_type})
