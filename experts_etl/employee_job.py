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

"""
status_flag values:
C Current 
F Future 
H Historical

empl_status values:
A Active 
D Deceased 
L Leave of Absence 
P Leave With Pay 
Q Retired With Pay 
R Retired 
S Suspended 
T Terminated 
U Terminated With Pay 
V Terminated Pension Pay Out 
W Short Work Break 
X Retired-Pension Administration
"""
def transform(jobs):
  jobs_by_position_nbr = group_by_position_nbr(jobs)

  t_jobs = []
  return t_jobs

def group_by_position_nbr(jobs):
  jobs_by_position_nbr = {}
  for job in jobs:
    position_nbr = job['position_nbr']
    if position_nbr not in jobs_by_position_nbr:
      jobs_by_position_nbr[position_nbr] = [] 
    jobs_by_position_nbr[position_nbr].append(job)
  return jobs_by_position_nbr
