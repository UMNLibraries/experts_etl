from sqlalchemy import and_
from experts_dw.models import PureApiChange, PureApiChangeHst

def changes_for_family_ordered_by_uuid_version(session, family_system_name):
    changes = []
    for change in (session.query(PureApiChange).filter(
            PureApiChange.family_system_name==family_system_name
        ).order_by(
            PureApiChange.uuid
        ).order_by(
            PureApiChange.version.desc()
        ).all()
    ):
        if len(changes) == 0 or change.uuid == changes[0].uuid:
            changes.append(change)
            continue
        yield changes
        changes = [change]
    if len(changes) > 0:
        yield changes
    return

def record_changes_as_processed(session, changes):
    for change in changes:
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
