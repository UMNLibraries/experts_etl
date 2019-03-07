from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import json
from experts_dw import db
from sqlalchemy import and_, func
from experts_dw.models import PureApiInternalOrg, PureApiInternalOrgHst, PureOrg, PureInternalOrg, UmnDeptPureOrg
from experts_etl import loggers
from pureapi import client, response

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

def load_db_dept_orgs(session, api_org):
  # Rather than try to reconcile any existing rows with any new ones,
  # just delete and re-create:
  session.query(UmnDeptPureOrg).filter(
    UmnDeptPureOrg.pure_org_uuid == api_org.uuid
  ).delete(synchronize_session=False)

  for _id in api_org.ids:
      if _id.typeUri.split('/')[-1] != 'peoplesoft_deptid':
          continue
      db_dept_org = UmnDeptPureOrg(
        pure_org_id = get_pure_id(api_org),
        pure_org_uuid = api_org.uuid,
        deptid = _id.value
      )
      session.add(db_dept_org)

def get_pure_org(pure_org_uuid):
  pure_org = None
  try:
    r = client.get(pure_api_record_type + '/' + pure_org_uuid)
    pure_org = response.transform(pure_api_record_type, r.json())      
  except Exception:
    pass
  return pure_org

def get_pure_id(api_org):
  pure_id = None
  if api_org.externalId is not None:
    pure_id = api_org.externalId
  else:
    pure_id = next(
      (id_ for id_ in api_org.ids if id_.typeUri =='/dk/atira/pure/organisation/organisationsources/organisationid'),
      None
    )
  return pure_id

def db_org_depth_first_search(session, parent_org, visited={}):
    if parent_org.pure_uuid not in visited:
        visited[parent_org.pure_uuid] = parent_org
        for child_org in session.query(PureOrg).filter(
            PureOrg.parent_pure_id == parent_org.pure_id, PureOrg.type != 'peoplesoft deptid'
        ).all():
            org_tree(session, child_org, visited)
    return visited

def update_internal_org_tree(session):
    # Unviersity of Minnesota org. There's probably a better way to do this...
    root_org = session.query(PureOrg).filter(PureOrg.pure_id == 'GLSGXMKPL').one()

    for pure_uuid, pure_org in db_org_depth_first_search(session, root_org).items():
        pure_org_immediate_children = {
            org.pure_uuid:org for org in
            session.query(PureOrg).filter(
                PureOrg.parent_pure_uuid == pure_uuid,
                PureOrg.type != 'peoplesoft deptid'
            ).all()
        }
        pure_internal_org = session.query(PureInternalOrg).filter(PureInternalOrg.pure_uuid == pure_uuid).one_or_none()
        parent_pure_internal_org = session.query(PureInternalOrg).filter(PureInternalOrg.pure_uuid == pure_org.parent_pure_uuid).one_or_none()
        if pure_internal_org is None:
            if parent_pure_internal_org:
                pure_internal_org = PureInternalOrg(
                    parent_id=parent_pure_internal_org.id,
                    pure_id=pure_org.pure_id,
                    name_en=pure_org.name_en,
                    pure_uuid=pure_uuid
                )
                session.add(pure_internal_org)
                session.commit()
            else:
                experts_etl_logger.warning(
                    'cannot create pure_internal_org: no parent with pure_org.pure_uuid',
                    extra={
                        'pure_api_record_type': pure_api_record_type,
                        'pure_uuid': pure_uuid,
                        'pure_id': pure_org.pure_id,
                        'name_en': pure_org.name_en,
                    }
                )
        if pure_internal_org:
            pure_internal_org_immediate_children = {
                org.pure_uuid:org for org in
                session.query(PureInternalOrg).filter(PureInternalOrg.parent_id == pure_internal_org.id).all()
            }
            for child_pure_uuid, child_pure_org in pure_org_immediate_children.items():
                if child_pure_org.pure_uuid in pure_internal_org_immediate_children:
                    child_pure_internal_org = pure_internal_org_immediate_children[child_pure_uuid]
                    child_pure_internal_org.pure_id = child_pure_org.pure_id
                    child_pure_internal_org.name_en = child_pure_org.name_en
                else:
                    child_pure_internal_org = session.query(PureInternalOrg).filter(PureInternalOrg.pure_uuid == child_pure_uuid).one_or_none()
                    if child_pure_internal_org:
                        child_pure_internal_org.parent_id = pure_internal_org.id
                        child_pure_internal_org.pure_id = child_pure_org.pure_id
                        child_pure_internal_org.name_en = child_pure_org.name_en
                    else:
                        max_id = session.query(func.max(PureInternalOrg.id)).scalar()
                        child_pure_internal_org = PureInternalOrg(
                            id=max_id+1,
                            parent_id=pure_internal_org.id,
                            pure_id=child_pure_org.pure_id,
                            name_en=child_pure_org.name_en,
                            pure_uuid=child_pure_uuid
                        )
                session.add(child_pure_internal_org)
                session.commit()

    # Delete any internal orgs that no longer exist in PureOrg:
    session.query(PureInternalOrg).filter(
        ~session.query(PureOrg).filter(PureOrg.pure_uuid == PureInternalOrg.pure_uuid).exists()
    ).delete(synchronize_session=False)
    session.commit()

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
  experts_etl_logger.info('starting: transforming/loading', extra={'pure_api_record_type': pure_api_record_type})

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

      # TODO: This needs work! Fix pureapi.response.
      parent_pure_id = None
      if api_org.parents[0].uuid is not None:
        parent_pure_uuid = api_org.parents[0].uuid
        parent_pure_org = get_pure_org(parent_pure_uuid)
        if parent_pure_org is not None:
          parent_pure_id = get_pure_id(parent_pure_org)
      db_org.parent_pure_id = parent_pure_id

      db_org.name_en = api_org.name[0].value
      db_org.parent_pure_uuid = api_org.parents[0].uuid
      db_org.pure_id = get_pure_id(api_org)
      db_org.type = api_org.type[0].value.lower()
      db_org.pure_modified = db_api_org.modified
      session.add(db_org)

      load_db_dept_orgs(session, api_org)

      processed_api_org_uuids.append(api_org.uuid)
      if len(processed_api_org_uuids) >= transaction_record_limit:
        mark_api_orgs_as_processed(session, pure_api_record_logger, processed_api_org_uuids)
        processed_api_org_uuids = []
        session.commit()

    mark_api_orgs_as_processed(session, pure_api_record_logger, processed_api_org_uuids)
    session.commit()

    update_internal_org_tree(session)

  loggers.rollover(pure_api_record_logger)
  experts_etl_logger.info('ending: transforming/loading', extra={'pure_api_record_type': pure_api_record_type})
