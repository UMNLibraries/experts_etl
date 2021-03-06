import json
from experts_dw import db
from sqlalchemy import and_, func
from experts_dw.models import PureApiInternalOrg, PureApiInternalOrgHst, PureOrg, PureInternalOrg, UmnDeptPureOrg
from experts_etl import loggers
from pureapi import client, response
from pureapi.client import Config

# defaults:

db_name = 'hotel'
transaction_record_limit = 100
# Named for the Pure API endpoint:
pure_api_record_type = 'organisational-units'
pure_api_record_logger = loggers.pure_api_record_logger(type=pure_api_record_type)

def extract_api_orgs(session):
    for uuid in [result[0] for result in session.query(PureApiInternalOrg.uuid).distinct()]:
        orgs = session.query(PureApiInternalOrg).filter(
                PureApiInternalOrg.uuid == uuid
            ).order_by(
                PureApiInternalOrg.modified.desc()
            ).all()
        # The first record in the list should be the latest:
        yield orgs[0]

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
        pure_internal = 'Y',
        name_en = next(
            (name_text.value
                for name_text
                in api_org.name.text
                if name_text.locale =='en_US'
            ),
            None
        ),
    )

def load_db_dept_orgs(session, api_org):
    for _id in api_org.ids:
        if _id.type.uri.split('/')[-1] != 'peoplesoft_deptid':
            continue

        deptid = _id.value.value
        pure_org_id = get_pure_id(api_org)

        db_dept_org = session.query(UmnDeptPureOrg).filter(
            UmnDeptPureOrg.deptid == deptid
        ).one_or_none()
        if db_dept_org is None:
            db_dept_org = UmnDeptPureOrg(
                pure_org_id = pure_org_id,
                pure_org_uuid = api_org.uuid,
                deptid = deptid
            )
        else:
            db_dept_org.pure_org_uuid = api_org.uuid
            db_dept_org.pure_org_id = pure_org_id
        session.add(db_dept_org)

def get_pure_org(pure_org_uuid, pure_api_config):
    pure_org = None
    try:
        r = client.get(pure_api_record_type + '/' + pure_org_uuid, config=pure_api_config)
        pure_org = response.transform(
            pure_api_record_type,
            r.json(),
            version=pure_api_config.version
        )
    except Exception:
        pass
    return pure_org

def get_pure_id(api_org):
    pure_id = None
    if api_org.externalId is not None:
        pure_id = api_org.externalId
    else:
        pure_id = next(
            (_id.value.value for _id in api_org.ids if _id.type.uri =='/dk/atira/pure/organisation/organisationsources/organisationid'),
            None
        )
    return pure_id

def db_org_depth_first_search(session, parent_org):
    yield parent_org
    visited = set()
    visited.add(parent_org.pure_uuid)
    children = db_org_children(session, parent_org)
    while children:
        child_org = children.pop()
        if child_org.pure_uuid not in visited:
            yield child_org
            visited.add(child_org.pure_uuid)
            children.extend(db_org_children(session, child_org))

def db_org_children(session, parent_org):
    return session.query(PureOrg).filter(
            PureOrg.pure_internal == 'Y',
            PureOrg.parent_pure_uuid == parent_org.pure_uuid,
            PureOrg.type != 'peoplesoft deptid'
        ).all()

def update_db_mptt_orgs(session):
    root_db_mptt_org = session.query(PureInternalOrg).filter(PureInternalOrg.left == 1).one()
    root_db_org = session.query(PureOrg).filter(PureOrg.pure_id == root_db_mptt_org.pure_id).one()

    for db_org in db_org_depth_first_search(session, root_db_org):
        db_mptt_org = session.query(PureInternalOrg).filter(
            PureInternalOrg.pure_uuid == db_org.pure_uuid
        ).one_or_none()
        parent_db_mptt_org = session.query(PureInternalOrg).filter(
            PureInternalOrg.pure_uuid == db_org.parent_pure_uuid
        ).one_or_none()
        if db_mptt_org is None:
            if parent_db_mptt_org:
                db_mptt_org = PureInternalOrg(
                    parent_id=parent_db_mptt_org.id,
                    pure_id=db_org.pure_id,
                    name_en=db_org.name_en,
                    pure_uuid=db_org.pure_uuid
                )
                session.add(db_mptt_org)
            else:
                experts_etl_logger.warning(
                    'cannot create pure_internal_org: no parent with pure_org.pure_uuid',
                    extra={
                        'pure_api_record_type': pure_api_record_type,
                        'pure_uuid': db_org.pure_uuid,
                        'pure_id': db_org.pure_id,
                        'name_en': db_org.name_en,
                    }
                )
        else:
            db_mptt_org_immediate_children = {
                org.pure_uuid:org for org in
                session.query(PureInternalOrg).filter(PureInternalOrg.parent_id == db_mptt_org.id).all()
            }
            for child_db_org in db_org_children(session, db_org):
                child_pure_uuid = child_db_org.pure_uuid
                if child_pure_uuid in db_mptt_org_immediate_children:
                    child_db_mptt_org = db_mptt_org_immediate_children[child_pure_uuid]
                    child_db_mptt_org.pure_id = child_db_org.pure_id
                    child_db_mptt_org.name_en = child_db_org.name_en
                else:
                    child_db_mptt_org = session.query(PureInternalOrg).filter(PureInternalOrg.pure_uuid == child_pure_uuid).one_or_none()
                    if child_db_mptt_org:
                        child_db_mptt_org.parent_id = db_mptt_org.id
                        child_db_mptt_org.pure_id = child_db_org.pure_id
                        child_db_mptt_org.name_en = child_db_org.name_en
                    else:
                        max_id = session.query(func.max(PureInternalOrg.id)).scalar()
                        child_db_mptt_org = PureInternalOrg(
                            id=max_id+1,
                            parent_id=db_mptt_org.id,
                            pure_id=child_db_org.pure_id,
                            name_en=child_db_org.name_en,
                            pure_uuid=child_pure_uuid
                        )
                session.add(child_db_mptt_org)

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
    experts_etl_logger=None,
    pure_api_config = None
):
    if experts_etl_logger is None:
        experts_etl_logger = loggers.experts_etl_logger()
    experts_etl_logger.info('starting: transforming/loading', extra={'pure_api_record_type': pure_api_record_type})

    if pure_api_config is None:
        pure_api_config = Config()

    # Capture the current record for each iteration, so we can log it in case of an exception:
    api_org = None

    try:
        with db.session(db_name) as session:
            processed_api_org_uuids = []
            for db_api_org in extract_api_orgs(session):
                api_org = response.transform(
                    pure_api_record_type,
                    json.loads(db_api_org.json),
                    version=pure_api_config.version
                )
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
                    parent_pure_org = get_pure_org(
                        parent_pure_uuid,
                        pure_api_config
                    )
                    if parent_pure_org is not None:
                        parent_pure_id = get_pure_id(parent_pure_org)
                db_org.parent_pure_id = parent_pure_id

                db_org.name_en = next(
                    (name_text.value
                        for name_text
                        in api_org.name.text
                        if name_text.locale =='en_US'
                    ),
                    None
                )

                db_org.parent_pure_uuid = api_org.parents[0].uuid
                db_org.pure_id = get_pure_id(api_org)

                db_org.type = next(
                    (type_text.value
                        for type_text
                        in api_org.type.term.text
                        if type_text.locale =='en_US'
                    ),
                    None
                ).lower()

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

            update_db_mptt_orgs(session)

    except Exception as e:
        formatted_exception = loggers.format_exception(e)
        experts_etl_logger.error(
            f'exception encountered during record transformation: {formatted_exception}',
            extra={'pure_uuid': api_org.uuid, 'pure_api_record_type': pure_api_record_type}
        )

    loggers.rollover(pure_api_record_logger)
    experts_etl_logger.info('ending: transforming/loading', extra={'pure_api_record_type': pure_api_record_type})
