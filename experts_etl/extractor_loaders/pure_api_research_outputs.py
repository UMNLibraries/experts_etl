import json
from sqlalchemy import and_, func
from experts_dw import db
from experts_dw.models import PureApiPub, PureApiPubHst, PureApiChange, PureApiChangeHst, Pub, PubPerson, PubPersonPureOrg
from experts_etl import transformers
from pureapi import client, response
from pureapi.exceptions import PureAPIClientRequestException
from experts_etl import loggers

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
    'article',
    'systematicreview',
  ],
}

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
    .filter(PureApiChange.family_system_name=='ResearchOutput')
    .all()
  ):
    yield change

# functions:

def api_pub_exists_in_db(session, api_pub):
  api_pub_modified = transformers.iso_8601_string_to_datetime(api_pub.info.modifiedDate)

  db_api_pub_hst = (
    session.query(PureApiPubHst)
    .filter(and_(
      PureApiPubHst.uuid == api_pub.uuid,
      PureApiPubHst.modified == api_pub_modified,
    ))
    .one_or_none()
  )
  if db_api_pub_hst:
    return True

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

  session.delete(db_pub)

def db_pub_newer_than_api_pub(session, api_pub):
  api_pub_modified = transformers.iso_8601_string_to_datetime(api_pub.info.modifiedDate)
  db_pub = get_db_pub(session, api_pub.uuid)
  # We need the replace(tzinfo=None) here, or we get errors like:
  # TypeError: can't compare offset-naive and offset-aware datetimes
  if db_pub and db_pub.pure_modified and db_pub.pure_modified >= api_pub_modified.replace(tzinfo=None):
    return True
  return False

def load_api_pub(session, api_pub, raw_json):
  db_api_pub = PureApiPub(
    uuid=api_pub.uuid,
    json=raw_json,
    modified=transformers.iso_8601_string_to_datetime(api_pub.info.modifiedDate)
  )
  session.add(db_api_pub)

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
    uuids_to_download = []
    processed_api_change_uuids = []
    for api_change in extract_api_changes(session):

      # We delete here and continue, because there will be no record
      # to download from the Pure API when it has been deleted.
      if api_change.change_type == 'DELETE':
        db_pub = get_db_pub(session, api_change.uuid)
        if db_pub:
          delete_db_pub(session, db_pub)
        processed_api_change_uuids.append(api_change.uuid)
        if len(processed_api_change_uuids) >= transaction_record_limit:
          mark_api_changes_as_processed(session, processed_api_change_uuids)
          processed_api_change_uuids = []
          session.commit()
        continue

      uuids_to_download.append(api_change.uuid)

    try:
      for r in client.filter_all_by_uuid(pure_api_record_type, uuids=uuids_to_download):
        d = r.json()
        for api_pub_orig in d['items']:
          api_pub = response.transform(pure_api_record_type, api_pub_orig)

          type_uri_parts = api_pub.type[0].uri.split('/')
          type_uri_parts.reverse()
          pure_subtype, pure_type, pure_parent_type = type_uri_parts[0:3]

          load = True
          if pure_type not in supported_pure_types or pure_subtype not in supported_pure_types[pure_type]:
            load = False
          if db_pub_newer_than_api_pub(session, api_pub):
            load = False
          if api_pub_exists_in_db(session, api_pub):
            load = False
          if load:
            load_api_pub(session, api_pub, json.dumps(api_pub_orig))

          processed_api_change_uuids.append(api_pub.uuid)
          if len(processed_api_change_uuids) >= transaction_record_limit:
            mark_api_changes_as_processed(session, processed_api_change_uuids)
            processed_api_change_uuids = []
            session.commit()
    
    except Exception as e:
      experts_etl_logger.exception(str(e))

    mark_api_changes_as_processed(session, processed_api_change_uuids)
    session.commit()

  experts_etl_logger.info('ending: extracting/loading', extra={'pure_api_record_type': pure_api_record_type})
