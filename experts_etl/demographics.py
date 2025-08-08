from sqlalchemy import func
from experts_dw.models import PureEligibleDemogChngHst

def latest_demographics_for_emplid(session, emplid):
    subqry = session.query(func.max(PureEligibleDemogChngHst.timestamp)).filter(PureEligibleDemogChngHst.emplid == emplid)
    demog = (
        session.query(PureEligibleDemogChngHst)
        .filter(
            PureEligibleDemogChngHst.emplid == emplid,
            PureEligibleDemogChngHst.timestamp == subqry.as_scalar()
        )
        .one_or_none()
    )
    return demog

def latest_not_null_internet_id_for_emplid(session, emplid):
    demog = (
        session.query(PureEligibleDemogChngHst)
        .filter(
            PureEligibleDemogChngHst.emplid == emplid,
            PureEligibleDemogChngHst.internet_id != None,
        )
        .order_by(PureEligibleDemogChngHst.timestamp.desc())
        .limit(1)
        .one_or_none()
    )
    return demog.internet_id if demog else None
