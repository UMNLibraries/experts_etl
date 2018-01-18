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
F Future (We exclude these entries in the SQL views.)
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
  transformed_jobs = []

  for position_nbr, entries in jobs_by_position_nbr.items():
    job_stints = transform_job_entries(entries)

    for job_stint in job_stints:
      transformed_job = transform_job_stint(job_stint)
      transformed_jobs.append(transormed_job)
      
  return transformed_jobs

def transform_job_entries(entries):
  job_stints = []
  current_stint = []
  active_states = ['A', 'L', 'S', 'W']
  current_stint_ending = False

  for entry in entries:
    if current_stint_ending:
      if entry['empl_status'] in active_states:
        # We've passed the end of the current stint, and this is a new stint in the same position.
        job_stints.append(current_stint)
        current_stint = []
        current_stint_ending = False
      current_stint.append(entry)
      continue

    if entry['empl_status'] not in active_states:
      # This is the first entry with an inactive state for this stint, so it's ending.
      # Other entries with inactive states may follow.
      current_stint_ending = True
    current_stint.append(entry)

  if len(current_stint) > 0:
    job_stints.append(current_stint)

  return job_stints

def group_by_position_nbr(jobs):
  jobs_by_position_nbr = {}
  for job in jobs:
    position_nbr = job['position_nbr']
    if position_nbr not in jobs_by_position_nbr:
      jobs_by_position_nbr[position_nbr] = [] 
    jobs_by_position_nbr[position_nbr].append(job)
  return jobs_by_position_nbr
