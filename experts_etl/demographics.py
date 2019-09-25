from sqlalchemy import func
from experts_dw.models import PureEligibleDemogChngHst

def latest_demographics(session, emplid):
    subqry = session.query(func.max(PureEligibleDemogChngHst.timestamp)).filter(PureEligibleDemogChngHst.emplid == emplid)
    demog = (
        session.query(PureEligibleDemogChngHst)
        .filter(
            PureEligibleDemogChngHst.emplid == emplid,
            PureEligibleDemogChngHst.timestamp == subqry
        )
        .one_or_none()
    )
    return demog
