from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import json
from experts_dw import db
from sqlalchemy import and_, func
from experts_dw.models import PureApiInternalOrg, PureApiInternalOrgHst, PureOrg
from experts_etl import loggers
from pureapi import response

# defaults:

db_name = 'hotel'
transaction_record_limit = 100 
# Named for the Pure API endpoint:
pure_api_record_type = 'organisational-units'
pure_api_record_logger = loggers.pure_api_record_logger(type=pure_api_record_type)

def extract_api_orgs(session):
  sq = session.query(
    PureApiInternalOrg.uuid,
    func.max(PureApiInternalOrg.modified).label('modified')
  ).select_from(PureApiInternalOrg).group_by(PureApiInternalOrg.uuid).subquery()

  for org in (session.query(PureApiInternalOrg)
    .join(
      sq,
      and_(PureApiInternalOrg.uuid==sq.c.uuid, PureApiInternalOrg.modified==sq.c.modified)
    )
    .all()
  ):
    yield org

def mark_api_orgs_as_processed(session, pure_api_record_logger, processed_api_org_uuids):
  for uuid in processed_api_org_uuids:
    for org in session.query(PureApiInternalOrg).filter(PureApiInternalOrg.uuid==uuid).all():

      org_hst = (
        session.query(PureApiInternalOrgHst)
        .filter(and_(
          PureApiInternalOrgHst.uuid == org.uuid,
          PureApiInternalOrgHst.modified == org.modified,
        ))
        .one_or_none()
      )

      if org_hst is None:
        org_hst = PureApiInternalOrgHst(
          uuid=org.uuid,
          modified=org.modified,
          downloaded=org.downloaded
        )
        session.add(org_hst)

      pure_api_record_logger.info(org.json)
      session.delete(org)

def get_db_org(session, pure_uuid):
  return (
    session.query(PureOrg)
    .filter(PureOrg.pure_uuid == pure_uuid)
    .one_or_none()
  )

def create_db_org(api_org):
  return PureOrg(
    pure_uuid = api_org.uuid,
    name_en = api_org.name[0].value,
    pure_internal = 'Y',
  )

def run(
  # Do we need other default functions here?
  extract_api_orgs=extract_api_orgs,
  db_name=db_name,
  transaction_record_limit=transaction_record_limit,
  pure_api_record_logger=pure_api_record_logger,
  experts_etl_logger=None
):
  if experts_etl_logger is None:
    experts_etl_logger = loggers.experts_etl_logger()
  experts_etl_logger.info('starting: {} processing'.format(pure_api_record_type))

  with db.session(db_name) as session:
    processed_api_org_uuids = []
    for db_api_org in extract_api_orgs(session):
      api_org = response.transform(pure_api_record_type, json.loads(db_api_org.json))      
      db_org = get_db_org(session, db_api_org.uuid)
      if db_org:
        if db_org.pure_modified and db_org.pure_modified >= db_api_org.modified:
          # Skip this record, since we already have a newer one:
          processed_api_org_uuids.append(db_api_org.uuid)
          continue
      else:   
        db_org = create_db_org(api_org)

      # Skipping for now, because we can't get it directly from the API. We'll load it 
      # from the Experts DW later instead.
      #db_org.parent_pure_id = 

      db_org.name_en = api_org.name[0].value
      db_org.parent_pure_uuid = api_org.parents[0].uuid
      db_org.pure_id = api_org.externalId
      db_org.type = api_org.type[0].value.lower()
      db_org.pure_modified = db_api_org.modified
      session.add(db_org)

      processed_api_org_uuids.append(api_org.uuid)
      if len(processed_api_org_uuids) >= transaction_record_limit:
        mark_api_orgs_as_processed(session, pure_api_record_logger, processed_api_org_uuids)
        processed_api_org_uuids = []
        session.commit()

    mark_api_orgs_as_processed(session, pure_api_record_logger, processed_api_org_uuids)
    session.commit()

  loggers.rollover(pure_api_record_logger)
  experts_etl_logger.info('ending: {} processing'.format(pure_api_record_type))
