import re
from experts_dw import db
from experts_dw.models import PureEligibleEmpJobChngHst

session = db.session('hotel')

def extract(emplid):
  jobs = []
  for job in session.query(PureEligibleEmpJobChngHst).filter(PureEligibleEmpJobChngHst.emplid == emplid).order_by(PureEligibleEmpJobChngHst.effdt, PureEligibleEmpJobChngHst.effseq):
    jobs.append(
      {c.name: getattr(job, c.name) for c in job.__table__.columns}
    )

  return jobs

def transform(jobs, primary_empl_rcdno):
  t_jobs = []
  return t_jobs
